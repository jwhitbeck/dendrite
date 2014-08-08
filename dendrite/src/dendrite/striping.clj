(ns dendrite.striping
  (:require [dendrite.encoding :as encoding]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema]
            [dendrite.utils :refer [format-ks single]]))

(set! *warn-on-reflection* true)

(defn- coercion-fns-map [column-specs]
  (->> column-specs
       (map :type)
       set
       (map #(vector % (encoding/coercion-fn %)))
       (into {})))

(defmulti ^:private stripe-fn*
  (fn [field parents coercion-fns-map]
    (if (schema/record? field)
      (case (:repetition field)
        (:vector :list :set) :repeated-record
        :map :map
        :optional :optional-record
        :required :required-record)
      (case (:repetition field)
        (:vector :list :set) :repeated-value
        :optional :optional-value
        :required :required-value))))

(defn- append-non-repeated! [striped-record column-index leveled-values]
  (assoc! striped-record column-index leveled-values))

(defn- append-repeated! [striped-record column-index leveled-values]
  (assoc! striped-record column-index (concat (or (get striped-record column-index) []) leveled-values)))

(defmethod stripe-fn* :required-value
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        {:keys [max-repetition-level max-definition-level column-index] :as cs} (:column-spec field)
        coercion-fn (get coercion-fns-map (:type cs))
        append-fn (if (zero? max-repetition-level) append-non-repeated! append-repeated!)]
    (fn [striped-record record nil-parent? repetition-level definition-level]
      (let [lv (if nil-parent?
                 (->LeveledValue repetition-level definition-level nil)
                 (if (nil? record)
                   (throw (IllegalArgumentException.
                           (format "Required field %s is missing" (format-ks path))))
                   (try
                     (->LeveledValue repetition-level max-definition-level (coercion-fn record))
                     (catch Exception e
                       (throw (IllegalArgumentException.
                               (format "Could not coerce value in %s" (format-ks path)) e))))))]
        (append-fn striped-record column-index (single lv))))))

(defmethod stripe-fn* :optional-value
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        {:keys [max-repetition-level max-definition-level column-index] :as cs} (:column-spec field)
        coercion-fn (get coercion-fns-map (:type cs))
        append-fn (if (zero? max-repetition-level) append-non-repeated! append-repeated!)]
    (fn [striped-record record nil-parent? repetition-level definition-level]
      (let [v (try
                (some-> record coercion-fn)
                (catch Exception e
                  (throw (IllegalArgumentException.
                          (format "Could not coerce value in %s" (format-ks path)) e))))
            lvs (single (->LeveledValue repetition-level
                                        (if (nil? v) definition-level max-definition-level)
                                        v))]
        (append-fn striped-record column-index lvs)))))

(defmethod stripe-fn* :repeated-value
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        {:keys [max-repetition-level max-definition-level column-index] :as cs} (:column-spec field)
        coercion-fn (get coercion-fns-map (:type cs))
        append-fn (if (= 1 max-repetition-level) append-non-repeated! append-repeated!)]
    (fn [striped-record record nil-parent? repetition-level definition-level]
      (let [lvs (if (empty? record)
                  (single (->LeveledValue repetition-level definition-level nil))
                  (try
                    (mapv (fn [v rl] (->LeveledValue rl max-definition-level (coercion-fn v)))
                          record
                          (cons repetition-level (repeat max-repetition-level)))
                    (catch Exception e
                      (throw (IllegalArgumentException.
                              (format "Could not coerce value in %s" (format-ks path)) e)))))]
        (append-fn striped-record column-index lvs)))))

(defmethod stripe-fn* :optional-record
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        subfield-stripe-fn-pairs (->> field
                                      :sub-fields
                                      (map #(vector (:name %) (stripe-fn* % path coercion-fns-map))))]
    (fn [striped-record record nil-parent? repetition-level definition-level]
      (let [definition-level (if (empty? record) definition-level (inc definition-level))]
        (doseq [[subfield-name subfield-stripe-fn] subfield-stripe-fn-pairs]
          (subfield-stripe-fn striped-record (get record subfield-name)
                              (empty? record) repetition-level definition-level))))))

(defmethod stripe-fn* :required-record
  [field parents coercion-fns-map]
  (let [path (if (:name field) (conj parents (:name field)) parents)
        subfield-stripe-fn-pairs (->> field
                                      :sub-fields
                                      (map #(vector (:name %) (stripe-fn* % path coercion-fns-map))))]
    (fn [striped-record record nil-parent? repetition-level definition-level]
      (when (and (empty? record) (not nil-parent?))
        (throw (IllegalArgumentException. (if (empty? parents)
                                            "Empty record!"
                                            (format "Required field %s is missing" (format-ks path))))))
      (doseq [[subfield-name subfield-stripe-fn] subfield-stripe-fn-pairs]
        (subfield-stripe-fn striped-record (get record subfield-name)
                            (empty? record) repetition-level definition-level)))))

(defmethod stripe-fn* :repeated-record
  [field parents coercion-fns-map]
  (let [empty-stripe-fn (stripe-fn* (assoc field :repetition :optional) parents coercion-fns-map)
        rep-lvl (:repetition-level field)
        def-lvl (:definition-level field)
        path (conj parents (:name field))
        subfield-stripe-fn-pairs (->> field
                                      :sub-fields
                                      (map #(vector (:name %) (stripe-fn* % path coercion-fns-map))))]
    (fn [striped-record repeated-records nil-parent? repetition-level definition-level]
      (if (empty? repeated-records)
        (empty-stripe-fn striped-record nil true repetition-level definition-level)
        (doseq [[rl record] (map vector (cons repetition-level (repeat rep-lvl)) repeated-records)
                [subfield-name subfield-stripe-fn] subfield-stripe-fn-pairs]
          (subfield-stripe-fn striped-record (get record subfield-name)
                              (empty? record) rl def-lvl))))))

(defmethod stripe-fn* :map
  [field parents coercion-fns-map]
  (let [list-stripe-fn (stripe-fn* (assoc field :repetition :list) parents coercion-fns-map)]
    (fn [striped-record map-record nil-parent? repetition-level definition-level]
      (if (empty? map-record)
        (list-stripe-fn striped-record nil true repetition-level definition-level)
        (list-stripe-fn striped-record (map (fn [[k v]] {:key k :value v}) map-record)
                        false repetition-level definition-level)))))

(defn stripe-fn [schema error-handler]
  (let [column-specs (schema/column-specs schema)
        empty-striped-record (vec (repeat (count column-specs) nil))
        sf (let [sf* (stripe-fn* schema [] (coercion-fns-map column-specs))]
             (fn [record]
               (let [tsr (transient empty-striped-record)]
                 (try
                   (do
                     (sf* tsr record false 0 0)
                     (persistent! tsr))
                   (catch Exception e
                     (throw (IllegalArgumentException. (format "Failed to stripe record '%s'" record) e)))))))]
    (if error-handler
      (fn [record]
        (try (sf record)
             (catch Exception e
               (error-handler record e)
               nil)))
      sf)))

(defn stripe-record [record schema]
  ((stripe-fn schema nil) record))
