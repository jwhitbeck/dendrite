(ns dendrite.striping
  (:require [dendrite.common :refer :all]
            [dendrite.encoding :as encoding]
            [dendrite.schema :as schema]))

(defn- new-striped-record [n] (vec (repeat n nil)))

(defn- coercion-fns-vec [value-types] (mapv (comp encoding/coercion-fn :type) value-types))

(defmulti ^:private recursively-stripe-record
  (fn [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
    (case (:repetition schema)
      (:vector :list :set) :seq
      :map :map
      (if (schema/record? schema)
        (:repetition schema)
        :atomic))))

(defmethod recursively-stripe-record :atomic
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (when (and (= :required (:repetition schema)) (nil? record) (not nil-parent?))
    (throw (IllegalArgumentException. (format "Required field %s is missing" (format-ks parents)))))
  (let [value-type (:value schema)
        value (when record
                (let [coercion-fn (get coercion-fns (:column-index value-type))]
                  (try
                    (coercion-fn record)
                    (catch Exception e
                      (throw (IllegalArgumentException.
                              (format "Could not coerce value in %s" (format-ks parents)) e))))))
        value-with-level (leveled-value repetition-level
                                        (if value (:definition-level value-type) definition-level)
                                        value)]
    (update-in striped-record
               [(:column-index value-type)] #(conj (or % [] leveled-value) value-with-level))))

(defmethod recursively-stripe-record :optional
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (let [definition-level (if (empty? record) definition-level (inc definition-level))]
    (reduce (fn [striped-record field]
              (let [v (get record (:name field))]
                (recursively-stripe-record striped-record v field (conj parents (:name field))
                                           (empty? record) coercion-fns repetition-level definition-level)))
            striped-record
            (schema/sub-fields schema))))

(defmethod recursively-stripe-record :required
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (when (and (empty? record) (not nil-parent?))
    (throw (IllegalArgumentException. (format "Required field %s is missing" (format-ks parents)))))
  (reduce (fn [striped-record field]
            (let [v (get record (:name field))]
              (recursively-stripe-record striped-record v field (conj parents (:name field)) (empty? record)
                                         coercion-fns repetition-level definition-level)))
          striped-record
          (schema/sub-fields schema)))

(defmethod recursively-stripe-record :seq
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (let [schema-without-repetition (assoc schema :repetition :optional)]
    (if (empty? record)
      (recursively-stripe-record striped-record nil schema-without-repetition parents true coercion-fns
                                 repetition-level definition-level)
      (reduce (fn [striped-record [sub-record repetition-level]]
                (recursively-stripe-record striped-record sub-record schema-without-repetition parents false
                                           coercion-fns repetition-level definition-level))
              striped-record
              (->> (interleave record (cons repetition-level (repeat (:repetition-level schema))))
                   (partition 2))))))

(defmethod recursively-stripe-record :map
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (let [schema-as-list (assoc schema :repetition :list)]
    (if (empty? record)
      (recursively-stripe-record striped-record nil schema-as-list parents true coercion-fns
                                 repetition-level definition-level)
      (recursively-stripe-record striped-record (map (fn [[k v]] {:key k :value v}) record) schema-as-list
                                 parents false coercion-fns repetition-level definition-level))))

(defn- recursively-stripe-record* [record schema new-striped-record-fn coercion-fns]
  (recursively-stripe-record (new-striped-record-fn) record schema [] false coercion-fns 0 0))

(defn- striper [schema]
  (let [column-specs (schema/column-specs schema)
        coercion-fns (coercion-fns-vec column-specs)
        num-cols (count column-specs)]
    (fn [record]
      (try
        (recursively-stripe-record* record schema #(new-striped-record num-cols) coercion-fns)
        (catch Exception e
          (throw (IllegalArgumentException. (format "Failed to stripe record '%s'" record) e)))))))

(defn stripe-record [record schema]
  ((striper schema) record))
