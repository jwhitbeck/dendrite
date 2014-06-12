(ns dendrite.assembly
  (:require [dendrite.common :refer :all]
            [dendrite.schema :as schema]))

(defmulti ^:private assemble*
  (fn [leveled-values-vec schema]
    (case (:repetition schema)
      (:optional :required) :field
      (:list :vector :set) :repeated
      :map :map)))

(defmethod assemble* :field
  [leveled-values-vec schema]
  (if (schema/record? schema)
    (let [[record next-repetition-level next-leveled-values-vec]
            (reduce (fn [[record next-repetition-level leveled-values-vec] sub-schema]
                      (let [[value next-repetition-level next-leveled-values-vec]
                              (assemble* leveled-values-vec sub-schema)]
                        [(if value (assoc record (:name sub-schema) value) record)
                         next-repetition-level
                         next-leveled-values-vec]))
                    [{} 0 leveled-values-vec]
                    (:sub-fields schema))]
      [(when-not (empty? record)
         (if-let [reader-fn (:reader-fn schema)]
           (reader-fn record)
           record))
       next-repetition-level
       next-leveled-values-vec])
    (let [column-index (-> schema :column-spec :column-index)
          leveled-values (get leveled-values-vec column-index)
          value (-> leveled-values first :value)
          next-repetition-level (or (some-> leveled-values second :repetition-level) 0)]
      ; TODO explain why we don't call reader-fn here
      [value next-repetition-level (assoc leveled-values-vec column-index (rest leveled-values))])))

(defmethod assemble* :repeated
  [leveled-values-vec schema]
  (let [non-repeated-schema (assoc schema :repetition :optional)
        [value next-repetition-level next-leveled-values-vec]
          (assemble* leveled-values-vec non-repeated-schema)]
    (if-not value
      [nil next-repetition-level next-leveled-values-vec]
      (let [init-coll (case (:repetition schema)
                        :list (list value)
                        :vector [value]
                        :set #{value})
            [record next-repetition-level next-leveled-values-vec]
              (loop [coll init-coll next-rl next-repetition-level next-lvv next-leveled-values-vec]
                (if (> (:repetition-level schema) next-rl)
                  [coll next-rl next-lvv]
                  (let [[value rl lvv] (assemble* next-lvv non-repeated-schema)]
                    (recur (conj coll value) rl lvv))))
            record (if (= :list (:repetition schema)) (doall (reverse record)) record)]
        [(if-let [reader-fn (:reader-fn schema)]
           (reader-fn record)
           record)
         next-repetition-level
         next-leveled-values-vec]))))

(defmethod assemble* :map
  [leveled-values-vec schema]
  (let [[key-value-pairs next-repetition-level next-leveled-values-vec]
          (assemble* leveled-values-vec (assoc schema :repetition :list :reader-fn nil))
        record (some->> key-value-pairs (map (juxt :key :value)) (into {}))]
    [(when-not (empty? record)
       (if-let [reader-fn (:reader-fn schema)]
         (reader-fn record)
         record))
     next-repetition-level
     next-leveled-values-vec]))

(defn assemble [leveled-values-vec schema]
  (first (assemble* leveled-values-vec schema)))
