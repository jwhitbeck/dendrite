(ns dendrite.assembly
  (:require [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema])
  (:import [dendrite.java LeveledValue]))

(set! *warn-on-reflection* true)

(defn- comp-some [f g]
  (fn [x]
    (when-let [v (g x)]
      (f v))))

(defmulti ^:private assemble-fn*
  (fn [field]
    (if (schema/record? field)
      (case (:repetition field)
        :map :map
        (:list :vector :set) :repeated-record
        (:optional :required) :non-repeated-record)
      (case (:repetition field)
        (:list :vector :set) :repeated-value
        (:optional :required) :non-repeated-value))))

(defmethod assemble-fn* :non-repeated-value
  [field]
  (let [col-idx (-> field :column-spec :query-column-index)]
    (fn [leveled-values-vec]
      (let [lvls (get leveled-values-vec col-idx)
            ^LeveledValue lv (first lvls)]
        (assoc! leveled-values-vec col-idx (rest lvls))
        (when lv
          (.value lv))))))

(defmethod assemble-fn* :repeated-value
  [field]
  (let [cs (:column-spec field)
        col-idx (:query-column-index cs)
        max-rl (:max-repetition-level cs)
        max-dl (:max-definition-level cs)
        rep-type (:repetition field)
        empty-coll (if (= :set rep-type) #{} [])
        ass-fn (fn [leveled-values-vec]
                 (let [lvs (get leveled-values-vec col-idx)
                       ^LeveledValue flv (first lvs)
                       next-rl (if-let [^LeveledValue n (second lvs)] (.repetitionLevel n) 0)]
                   (when-not (and (nil? (.value flv)) (> max-dl (.definitionLevel flv)) (> max-rl next-rl))
                     (let [ret (loop [rlvs (rest lvs)
                                      tr (conj! (transient empty-coll) (.value flv))]
                                 (let [^LeveledValue lv (first rlvs)]
                                   (if (and lv (= max-rl (.repetitionLevel lv)))
                                     (recur (rest rlvs) (conj! tr (.value lv)))
                                     (do (assoc! leveled-values-vec col-idx rlvs)
                                         (persistent! tr)))))]
                       (when-not (empty? ret)
                         ret)))))
        reader-fn (:reader-fn field)]
    (cond->> ass-fn
             (= rep-type :list) (comp seq)
             reader-fn (comp-some reader-fn))))

(defmethod assemble-fn* :non-repeated-record
  [field]
  (let [name-fn-map (reduce (fn [m fld] (assoc m (:name fld) (assemble-fn* fld))) {} (:sub-fields field))
        ass-fn (fn [leveled-values-vec]
                 (let [rec (->> name-fn-map
                                (reduce-kv (fn [m n f] (let [v (f leveled-values-vec)]
                                                         (if (nil? v) m (assoc! m n v))))
                                           (transient {}))
                                persistent!)]
                   (when-not (empty? rec)
                     rec)))]
    (if-let [reader-fn (:reader-fn field)]
      (comp-some reader-fn ass-fn)
      ass-fn)))

(defmethod assemble-fn* :repeated-record
  [field]
  (let [next-rl-col-idx (-> field schema/column-specs last :query-column-index)
        next-rl-fn (fn [lvv] (if-let [^LeveledValue lv (first (get lvv next-rl-col-idx))]
                               (.repetitionLevel lv)
                               0))
        next-dl-fn (fn [lvv] (if-let [^LeveledValue lv (first (get lvv next-rl-col-idx))]
                               (.definitionLevel lv)
                               0))
        rep-lvl (:repetition-level field)
        def-lvl (:definition-level field)
        rep-type (:repetition field)
        non-repeated-ass-fn (assemble-fn* (assoc field :repetition :optional :reader-fn nil))
        empty-coll (if (= :set rep-type) #{} [])
        ass-fn (fn [leveled-values-vec]
                 (let [dl (next-dl-fn leveled-values-vec)
                       fr (non-repeated-ass-fn leveled-values-vec)]
                   (when-not (and (nil? fr) (> def-lvl dl))
                     (let [ret (loop [trvs (conj! (transient empty-coll) fr)
                                      nrl (next-rl-fn leveled-values-vec)]
                                 (if (> rep-lvl nrl)
                                   (persistent! trvs)
                                   (let [nr (non-repeated-ass-fn leveled-values-vec)]
                                     (recur (conj! trvs nr) (next-rl-fn leveled-values-vec)))))]
                       (when-not (empty? ret)
                         ret)))))
        reader-fn (:reader-fn field)]
    (cond->> ass-fn
             (= :list rep-type) (comp-some seq)
             reader-fn (comp-some reader-fn))))

(defmethod assemble-fn* :map
  [field]
  (let [as-list-ass-fn (assemble-fn* (assoc field :repetition :list :reader-fn nil))
        ass-fn (fn [leveled-values-vec]
                 (some->> (as-list-ass-fn leveled-values-vec)
                          (map (juxt :key :value))
                          (into {})))]
    (if-let [reader-fn (:reader-fn field)]
      (comp-some reader-fn ass-fn)
      ass-fn)))

(defn assemble-fn [parsed-query]
  (let [ass-fn (assemble-fn* parsed-query)]
    (fn [leveled-values-vec]
      (let [tr (transient leveled-values-vec)]
        (ass-fn tr)))))

(defn assemble [leveled-values-vec query]
  ((assemble-fn query) leveled-values-vec))
