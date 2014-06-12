(ns dendrite.assembly
  (:require [dendrite.common :refer :all]
            [dendrite.schema :as schema]))

(defmulti ^:private assemble*
  (fn [schema leveled-values-vec]
    (case (:repetition schema)
      (:optional :required) :field
      (:list :vector :set) :repeated
      :map :map)))

(defmethod assemble* :field
  [schema leveled-values-vec]
  (if (schema/record? schema)
    (let [[record next-repetition-level next-leveled-values-vec]
            (reduce (fn [[record next-repetition-level leveled-values-vec] sub-schema]
                      (let [[value next-repetition-level next-leveled-values-vec]
                            (assemble* sub-schema leveled-values-vec)]
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
      [value next-repetition-level (assoc leveled-values-vec column-index (rest leveled-values))])))

(defmethod assemble* :repeated
  [schema leveled-values-vec]
  (let [non-repeated-schema (assoc schema :repetition :optional)
        [value next-repetition-level next-leveled-values-vec]
          (assemble* non-repeated-schema leveled-values-vec)]
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
                  (let [[value rl lvv] (assemble* non-repeated-schema next-lvv)]
                    (recur (conj coll value) rl lvv))))]
        [(if-let [reader-fn (:reader-fn schema)]
           (reader-fn record)
           record)
         next-repetition-level
         next-leveled-values-vec]))))

(defmethod assemble* :map
  [schema leveled-values-vec]
  (let [[key-value-pairs next-repetition-level next-leveled-values-vec]
          (assemble* (assoc schema :repetition :list) leveled-values-vec)
        record (some->> key-value-pairs (map (juxt :key :value)) (into {}))]
    [(when-not (empty? record)
       (if-let [reader-fn (:reader-fn schema)]
         (reader-fn record)
         record))
     next-repetition-level
     next-leveled-values-vec]))

(defn assemble [schema leveled-values-vec]
  (first (assemble* schema leveled-values-vec)))
