(ns dendrite.striping
  (:require [clojure.string :as string]
            [dendrite.core :refer [leveled-value]]
            [dendrite.encoding :refer [coercion-fn]]
            [dendrite.schema :as schema]))

(defn- new-striped-record [n] (vec (repeat n nil)))

(defn- coercion-fns-vec [value-types] (mapv (comp coercion-fn :type) value-types))

(defn- format-parents [parents] (format "[%s]" (string/join " " parents)))

(defmulti recursively-stripe-record
  (fn [striped-record record schema parents coercion-fns repetition-level definition-level]
    (case (:repetition schema)
      (:vector :list :set) :seq
      :map :map
      (if (schema/record? schema)
        (:repetition schema)
        :atomic))))

(defmethod recursively-stripe-record :atomic
  [striped-record record schema parents coercion-fns repetition-level definition-level]
  #_(if (and (= :required (:repetition schema)) (nil? record))
    (throw (IllegalArgumentException. (format "Required field %s is missing" (format-parents parents)))))
  (let [value-type (:value schema)
        value (when record
                (let [coercion-fn (get coercion-fns (:column-index value-type))]
                  (try
                    (coercion-fn record)
                    (catch Exception e
                      (throw (IllegalArgumentException. (format "Could not coerce value in %s" (format-parents parents)) e))))))
        value-with-level (leveled-value repetition-level
                                        (if value (:definition-level value-type) definition-level)
                                        value)]
    (update-in striped-record [(:column-index value-type)] #(conj (or % [] leveled-value) value-with-level))))

(defmethod recursively-stripe-record :optional
  [striped-record record schema parents coercion-fns repetition-level definition-level]
  (let [definition-level (if (empty? record) definition-level (inc definition-level))]
    (reduce (fn [striped-record field]
              (let [v (get record (:name field))]
                (recursively-stripe-record striped-record v field (conj parents (:name field)) coercion-fns repetition-level definition-level)))
            striped-record
            (schema/sub-fields schema))))

(defmethod recursively-stripe-record :required
  [striped-record record schema parents coercion-fns repetition-level definition-level]
  #_(when (empty? record)
    (throw (IllegalArgumentException. (format "Required field %s is missing" (format-parents parents)))))
  (reduce (fn [striped-record field]
            (let [v (get record (:name field))]
              (recursively-stripe-record striped-record v field (conj parents (:name field)) coercion-fns repetition-level definition-level)))
          striped-record
          (schema/sub-fields schema)))

(defmethod recursively-stripe-record :seq
  [striped-record record schema parents coercion-fns repetition-level definition-level]
  (let [schema-without-repetition (assoc schema :repetition :optional)]
    (if (empty? record)
      (recursively-stripe-record striped-record nil schema-without-repetition parents coercion-fns repetition-level definition-level)
      (reduce (fn [striped-record [sub-record repetition-level]]
                (recursively-stripe-record striped-record sub-record schema-without-repetition parents coercion-fns repetition-level definition-level))
              striped-record
              (->> (interleave record (cons repetition-level (repeat (:repetition-level schema)))) (partition 2))))))

(defmethod recursively-stripe-record :map
  [striped-record record schema parents coercion-fns repetition-level definition-level]
  (let [schema-as-list (assoc schema :repetition :list)]
    (if (empty? record)
      (recursively-stripe-record striped-record nil schema-as-list parents coercion-fns repetition-level definition-level)
      (recursively-stripe-record striped-record (map (fn [[k v]] {:key k :value v}) record) schema-as-list parents
                                 coercion-fns repetition-level definition-level))))

(defn- stripe-record [record schema new-striped-record-fn coercion-fns]
  (recursively-stripe-record (new-striped-record-fn) record schema [] coercion-fns 0 0))

(defn stripe-fn [schema]
  (let [value-types (schema/value-types schema)
        coercion-fns (coercion-fns-vec value-types)
        num-cols (count value-types)]
    (fn [record]
      (stripe-record record schema #(new-striped-record num-cols) coercion-fns))))
