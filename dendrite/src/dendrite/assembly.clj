;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
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
            [dendrite.schema :as schema]
            [dendrite.utils :as utils])
  (:import [clojure.lang ArraySeq]
           [dendrite.java Assembly LeveledValue]))

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
    (if (-> field :column-spec :max-repetition-level pos?)
      (Assembly/getNonRepeatedValueFn col-idx)
      (Assembly/getRequiredNonRepeatedValueFn col-idx))))

(defmethod assemble-fn* :repeated-value
  [field]
  (let [cs (:column-spec field)
        col-idx (:query-column-index cs)
        max-rl (:max-repetition-level cs)
        max-dl (:max-definition-level cs)
        rep-type (:repetition field)
        empty-coll (if (= :set rep-type) #{} [])
        ass-fn (Assembly/getRepeatedValueFn col-idx max-rl max-dl empty-coll)
        reader-fn (:reader-fn field)]
    (cond->> ass-fn
             (= rep-type :list) (comp seq)
             reader-fn (comp-some reader-fn))))

(defmethod assemble-fn* :non-repeated-record
  [field]
  (let [name-fn-map (reduce (fn [m fld] (assoc m (:name fld) (assemble-fn* fld))) {} (:sub-fields field))
        record-ctor (Assembly/getRecordConstructorFn (map :name (:sub-fields field))
                                                     (map assemble-fn* (:sub-fields field)))
        ass-fn (Assembly/getNonRepeatedRecordFn record-ctor)]
    (if-let [reader-fn (:reader-fn field)]
      (comp-some reader-fn ass-fn)
      ass-fn)))

(defmethod assemble-fn* :repeated-record
  [field]
  (let [next-rl-col-idx (-> field schema/column-specs last :query-column-index)
        rep-lvl (:repetition-level field)
        def-lvl (:definition-level field)
        rep-type (:repetition field)
        non-repeated-ass-fn (assemble-fn* (assoc field :repetition :optional :reader-fn nil))
        empty-coll (if (= :set rep-type) #{} [])
        ass-fn (Assembly/getRepeatedRecordFn non-repeated-ass-fn next-rl-col-idx rep-lvl def-lvl empty-coll)
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
    (fn [^ArraySeq striped-record]
      (when striped-record
        (let [lva (.array striped-record)]
          (ass-fn lva))))))
