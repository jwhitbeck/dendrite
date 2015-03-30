;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.striping
  (:require [dendrite.encoding :as encoding]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema]
            [dendrite.utils :refer [format-ks single transient-linked-seq transient?]]))

(set! *warn-on-reflection* true)

(defn- coercion-fns-map [type-store column-specs]
  (->> column-specs
       (map :type)
       (reduce #(update-in %1 [%2] (fn [cf] (or cf (encoding/coercion-fn type-store %2)))) {})))

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

(defn- append-non-repeated! [striped-record-array column-index value]
  (aset ^objects striped-record-array (int column-index) value))

(defn- append-repeated! [striped-record-array column-index leveled-values]
  (let [tr (or (aget ^objects striped-record-array (int column-index)) (transient-linked-seq))]
    (aset ^objects striped-record-array (int column-index) (reduce conj! tr leveled-values))))

(defmethod stripe-fn* :required-value
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        {:keys [max-repetition-level max-definition-level column-index] :as cs} (:column-spec field)
        coercion-fn (get coercion-fns-map (:type cs))]
    (if (zero? max-repetition-level)
      (fn [striped-record-array record nil-parent? repetition-level definition-level]
        (let [v (when-not nil-parent?
                  (if (nil? record)
                    (throw (IllegalArgumentException.
                            (format "Required field %s is missing" (format-ks path))))
                    (try
                      (coercion-fn record)
                      (catch Exception e
                        (throw (IllegalArgumentException.
                                (format "Could not coerce value in %s" (format-ks path)) e))))))]
          (append-non-repeated! striped-record-array column-index v)))
      (fn [striped-record-array record nil-parent? repetition-level definition-level]
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
          (append-repeated! striped-record-array column-index (single lv)))))))

(defmethod stripe-fn* :optional-value
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        {:keys [max-repetition-level max-definition-level column-index] :as cs} (:column-spec field)
        coercion-fn (get coercion-fns-map (:type cs))]
    (if (zero? max-repetition-level)
      (fn [striped-record-array record nil-parent? repetition-level definition-level]
        (let [v (when record
                  (try
                    (coercion-fn record)
                    (catch Exception e
                      (throw (IllegalArgumentException.
                              (format "Could not coerce value in %s" (format-ks path)) e)))))]
          (append-non-repeated! striped-record-array column-index v)))
      (fn [striped-record-array record nil-parent? repetition-level definition-level]
        (let [v (when record
                  (try
                    (coercion-fn record)
                    (catch Exception e
                      (throw (IllegalArgumentException.
                              (format "Could not coerce value in %s" (format-ks path)) e)))))
              lvs (single (->LeveledValue repetition-level
                                          (if (nil? v) definition-level max-definition-level)
                                          v))]
          (append-repeated! striped-record-array column-index lvs))))))

(defmethod stripe-fn* :repeated-value
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        {:keys [max-repetition-level max-definition-level column-index] :as cs} (:column-spec field)
        coercion-fn (get coercion-fns-map (:type cs))
        append-fn (if (= 1 max-repetition-level) append-non-repeated! append-repeated!)]
    (fn [striped-record-array record nil-parent? repetition-level definition-level]
      (let [lvs (if (empty? record)
                  (single (->LeveledValue repetition-level definition-level nil))
                  (try
                    (mapv (fn [v rl] (->LeveledValue rl max-definition-level (coercion-fn v)))
                          record
                          (cons repetition-level (repeat max-repetition-level)))
                    (catch Exception e
                      (throw (IllegalArgumentException.
                              (format "Could not coerce value in %s" (format-ks path)) e)))))]
        (append-fn striped-record-array column-index lvs)))))

(defmethod stripe-fn* :optional-record
  [field parents coercion-fns-map]
  (let [path (conj parents (:name field))
        subfield-stripe-fn-pairs (->> field
                                      :sub-fields
                                      (map (juxt :name #(stripe-fn* % path coercion-fns-map))))]
    (fn [striped-record-array record nil-parent? repetition-level definition-level]
      (let [definition-level (if (empty? record) definition-level (inc definition-level))]
        (doseq [[subfield-name subfield-stripe-fn] subfield-stripe-fn-pairs]
          (subfield-stripe-fn striped-record-array (get record subfield-name)
                              (empty? record) repetition-level definition-level))))))

(defmethod stripe-fn* :required-record
  [field parents coercion-fns-map]
  (let [path (if (:name field) (conj parents (:name field)) parents)
        subfield-stripe-fn-pairs (->> field
                                      :sub-fields
                                      (map (juxt :name #(stripe-fn* % path coercion-fns-map))))]
    (fn [striped-record-array record nil-parent? repetition-level definition-level]
      (when (and (empty? record) (not nil-parent?))
        (throw (IllegalArgumentException. (if (empty? parents)
                                            "Empty record!"
                                            (format "Required field %s is missing" (format-ks path))))))
      (doseq [[subfield-name subfield-stripe-fn] subfield-stripe-fn-pairs]
        (subfield-stripe-fn striped-record-array (get record subfield-name)
                            (empty? record) repetition-level definition-level)))))

(defmethod stripe-fn* :repeated-record
  [field parents coercion-fns-map]
  (let [empty-stripe-fn (stripe-fn* (assoc field :repetition :optional) parents coercion-fns-map)
        rep-lvl (:repetition-level field)
        def-lvl (:definition-level field)
        path (conj parents (:name field))
        subfield-stripe-fn-pairs (->> field
                                      :sub-fields
                                      (map (juxt :name #(stripe-fn* % path coercion-fns-map))))]
    (fn [striped-record-array repeated-records nil-parent? repetition-level definition-level]
      (if (empty? repeated-records)
        (empty-stripe-fn striped-record-array nil true repetition-level definition-level)
        (doseq [[rl record] (map vector (cons repetition-level (repeat rep-lvl)) repeated-records)
                [subfield-name subfield-stripe-fn] subfield-stripe-fn-pairs]
          (subfield-stripe-fn striped-record-array (get record subfield-name)
                              (empty? record) rl def-lvl))))))

(defmethod stripe-fn* :map
  [field parents coercion-fns-map]
  (let [list-stripe-fn (stripe-fn* (assoc field :repetition :list) parents coercion-fns-map)]
    (fn [striped-record-array map-record nil-parent? repetition-level definition-level]
      (if (empty? map-record)
        (list-stripe-fn striped-record-array nil true repetition-level definition-level)
        (list-stripe-fn striped-record-array (map (fn [[k v]] {:key k :value v}) map-record)
                        false repetition-level definition-level)))))

(defn- persist-striped-array! [^objects striped-array]
  (loop [i (int 0)]
    (if (< i (alength striped-array))
      (let [lvs (aget striped-array i)]
        (when (transient? lvs)
          (aset striped-array i (persistent! lvs)))
        (recur (inc i)))
      striped-array)))

(defn stripe-fn [schema type-store err-handler]
  (let [column-specs (schema/column-specs schema)
        n (count column-specs)
        sf (let [sf* (stripe-fn* schema [] (coercion-fns-map type-store column-specs))]
             (fn [record]
               (let [sa (object-array n)]
                 (try
                   (do
                     (sf* sa record false 0 0)
                     (seq (persist-striped-array! sa)))
                   (catch Exception e
                     (throw (IllegalArgumentException. (format "Failed to stripe record '%s'" record) e)))))))]
    (if err-handler
      (fn [record]
        (try (sf record)
             (catch Exception e
               (err-handler record e)
               nil)))
      sf)))
