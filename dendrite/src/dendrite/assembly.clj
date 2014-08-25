;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.assembly
  (:require [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema])
  (:import [clojure.lang ArraySeq]
           [dendrite.java LeveledValue PersistentFixedKeysHashMap]))

(set! *warn-on-reflection* true)

(defn- comp-some [f g]
  (fn [x]
    (when-let [v (g x)]
      (f v))))

(defmulti ^:private assemble-fn*
  (fn [field]
    (if (nil? field)
      :nil
      (if (schema/record? field)
        (case (:repetition field)
          :map :map
          (:list :vector :set) :repeated-record
          (:optional :required) :non-repeated-record)
        (case (:repetition field)
          (:list :vector :set) :repeated-value
          (:optional :required) :non-repeated-value)))))

(defmethod assemble-fn* :nil [field] (constantly nil))

(defmethod assemble-fn* :non-repeated-value
  [field]
  (let [col-idx (-> field :column-spec :query-column-index)]
    (fn [^objects leveled-values-array]
      (let [lvls (aget leveled-values-array col-idx)
            ^LeveledValue lv (first lvls)]
        (aset leveled-values-array col-idx (rest lvls))
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
        ass-fn (fn [^objects leveled-values-array]
                 (let [lvs (aget leveled-values-array col-idx)
                       ^LeveledValue flv (first lvs)
                       next-rl (if-let [^LeveledValue n (second lvs)] (.repetitionLevel n) 0)]
                   (when-not (and (nil? (.value flv)) (> max-dl (.definitionLevel flv)) (> max-rl next-rl))
                     (let [ret (loop [rlvs (rest lvs)
                                      tr (conj! (transient empty-coll) (.value flv))]
                                 (let [^LeveledValue lv (first rlvs)]
                                   (if (and lv (= max-rl (.repetitionLevel lv)))
                                     (recur (rest rlvs) (conj! tr (.value lv)))
                                     (do (aset leveled-values-array col-idx rlvs)
                                         (persistent! tr)))))]
                       (when-not (empty? ret)
                         ret)))))
        reader-fn (:reader-fn field)]
    (cond->> ass-fn
             (= rep-type :list) (comp seq)
             reader-fn (comp-some reader-fn))))

(def ^:private undefined PersistentFixedKeysHashMap/UNDEFINED)

(defn- record-ctor-fn [field-names field-ass-fns]
  (let [fact (PersistentFixedKeysHashMap/factory field-names)
        ^objects field-ass-fn-array (into-array clojure.lang.IFn field-ass-fns)
        n (count field-names)]
    (fn [^objects leveled-values-array]
      (let [^objects vals (make-array Object n)]
        (loop [i (int 0)]
          (if (= i n)
            (.create fact vals)
            (let [v ((aget field-ass-fn-array i) leveled-values-array)]
              (if (nil? v)
                (aset vals i undefined)
                (aset vals i v))
              (recur (inc i)))))))))

(defmethod assemble-fn* :non-repeated-record
  [field]
  (let [name-fn-map (reduce (fn [m fld] (assoc m (:name fld) (assemble-fn* fld))) {} (:sub-fields field))
        record-ctor (record-ctor-fn (map :name (:sub-fields field)) (map assemble-fn* (:sub-fields field)))
        ass-fn (fn [^objects leveled-values-array]
                 (let [rec (record-ctor leveled-values-array)]
                   (when-not (empty? rec)
                     rec)))]
    (if-let [reader-fn (:reader-fn field)]
      (comp-some reader-fn ass-fn)
      ass-fn)))

(defmethod assemble-fn* :repeated-record
  [field]
  (let [next-rl-col-idx (-> field schema/column-specs last :query-column-index)
        next-rl-fn (fn [^objects lva] (if-let [^LeveledValue lv (first (aget lva next-rl-col-idx))]
                                        (.repetitionLevel lv)
                                        0))
        next-dl-fn (fn [^objects lva] (if-let [^LeveledValue lv (first (aget lva next-rl-col-idx))]
                                        (.definitionLevel lv)
                                        0))
        rep-lvl (:repetition-level field)
        def-lvl (:definition-level field)
        rep-type (:repetition field)
        non-repeated-ass-fn (assemble-fn* (assoc field :repetition :optional :reader-fn nil))
        empty-coll (if (= :set rep-type) #{} [])
        ass-fn (fn [^objects leveled-values-array]
                 (let [dl (next-dl-fn leveled-values-array)
                       fr (non-repeated-ass-fn leveled-values-array)]
                   (when-not (and (nil? fr) (> def-lvl dl))
                     (let [ret (loop [trvs (conj! (transient empty-coll) fr)
                                      nrl (next-rl-fn leveled-values-array)]
                                 (if (> rep-lvl nrl)
                                   (persistent! trvs)
                                   (let [nr (non-repeated-ass-fn leveled-values-array)]
                                     (recur (conj! trvs nr) (next-rl-fn leveled-values-array)))))]
                       (when-not (empty? ret)
                         ret)))))
        reader-fn (:reader-fn field)]
    (cond->> ass-fn
             (= :list rep-type) (comp-some seq)
             reader-fn (comp-some reader-fn))))

(defmethod assemble-fn* :map
  [field]
  (let [as-list-ass-fn (assemble-fn* (assoc field :repetition :list :reader-fn nil))
        ass-fn (fn [^objects leveled-values-array]
                 (some->> (as-list-ass-fn leveled-values-array)
                          (map (juxt :key :value))
                          (into {})))]
    (if-let [reader-fn (:reader-fn field)]
      (comp-some reader-fn ass-fn)
      ass-fn)))

(defn assemble-fn [parsed-query]
  (let [ass-fn (assemble-fn* parsed-query)]
    (fn [^ArraySeq leveled-values]
      (when leveled-values
        (let [lva (.array leveled-values)]
          (ass-fn lva))))))
