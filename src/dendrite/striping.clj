(ns dendrite.striping
  (:require [dendrite.encoding :as encoding]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema]
            [dendrite.utils :refer [format-ks]]))

(defn- new-striped-record [n] (vec (repeat n nil)))

(defn- coercion-fns-vec [value-types] (mapv (comp encoding/coercion-fn :type) value-types))

(defmulti ^:private stripe
  (fn [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
    (case (:repetition schema)
      (:vector :list :set) :seq
      :map :map
      (if (schema/record? schema)
        (:repetition schema)
        :atomic))))

(defmethod stripe :atomic
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (when (and (= :required (:repetition schema)) (nil? record) (not nil-parent?))
    (throw (IllegalArgumentException. (format "Required field %s is missing" (format-ks parents)))))
  (let [column-spec (:column-spec schema)
        value (when record
                (let [coercion-fn (get coercion-fns (:column-index column-spec))]
                  (try
                    (coercion-fn record)
                    (catch Exception e
                      (throw (IllegalArgumentException.
                              (format "Could not coerce value in %s" (format-ks parents)) e))))))
        value-with-level (->LeveledValue repetition-level
                                         (if value (:max-definition-level column-spec) definition-level)
                                         value)]
    (update-in striped-record
               [(:column-index column-spec)] #(conj (or % []) value-with-level))))

(defmethod stripe :optional
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (let [definition-level (if (empty? record) definition-level (inc definition-level))]
    (reduce (fn [striped-record field]
              (let [v (get record (:name field))]
                (stripe striped-record v field (conj parents (:name field))
                        (empty? record) coercion-fns repetition-level definition-level)))
            striped-record
            (:sub-fields schema))))

(defmethod stripe :required
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (when (and (empty? record) (not nil-parent?))
    (throw (IllegalArgumentException. (if (empty? parents)
                                        "Empty record!"
                                        (format "Required field %s is missing" (format-ks parents))))))
  (reduce (fn [striped-record field]
            (let [v (get record (:name field))]
              (stripe striped-record v field (conj parents (:name field)) (empty? record)
                      coercion-fns repetition-level definition-level)))
          striped-record
          (:sub-fields schema)))

(defmethod stripe :seq
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (let [schema-without-repetition (assoc schema :repetition :optional)]
    (if (empty? record)
      (stripe striped-record nil schema-without-repetition parents true coercion-fns
              repetition-level definition-level)
      (reduce (fn [striped-record [sub-record repetition-level]]
                (stripe striped-record sub-record schema-without-repetition parents false
                        coercion-fns repetition-level definition-level))
              striped-record
              (->> (interleave record (cons repetition-level (repeat (:repetition-level schema))))
                   (partition 2))))))

(defmethod stripe :map
  [striped-record record schema parents nil-parent? coercion-fns repetition-level definition-level]
  (let [schema-as-list (assoc schema :repetition :list)]
    (if (empty? record)
      (stripe striped-record nil schema-as-list parents true coercion-fns
              repetition-level definition-level)
      (stripe striped-record (map (fn [[k v]] {:key k :value v}) record) schema-as-list
              parents false coercion-fns repetition-level definition-level))))

(defn stripe-fn [schema]
  (let [column-specs (schema/column-specs schema)
        coercion-fns (coercion-fns-vec column-specs)
        num-cols (count column-specs)]
    (fn [record]
      (try
        (stripe (new-striped-record num-cols) record schema [] false coercion-fns 0 0)
        (catch Exception e
          (throw (IllegalArgumentException. (format "Failed to stripe record '%s'" record) e)))))))

(defn stripe-record [record schema]
  ((stripe-fn schema) record))
