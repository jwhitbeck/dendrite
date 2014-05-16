(ns dendrite.schema
  (:require [clojure.data.fressian :as fressian]
            [clojure.edn :as edn]
            [clojure.string :as string])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [java.io Writer])
  (:refer-clojure :exclude [read-string]))

(set! *warn-on-reflection* true)

(defrecord ColumnType [value-type encoding compression-type required?])

(def column-type ->ColumnType)

(defrecord ValueType [type encoding compression column-index repetition-level definition-level nested?])

(defn- map->value-type-with-defaults [m]
  (map->ValueType (merge {:encoding :plain :compression :none :nested? true} m)))

(defmethod print-method ValueType
  [v ^Writer w]
  (.write w (str "#val " (cond-> (dissoc v :column-index :repetition-level :definition-level :nested?)
                                 (= (:compression v) :none) (dissoc :compression)
                                 (= (:encoding v) :plain) (dissoc :encoding)))))

(defrecord Field [name repetition value])

(defn record? [field] (-> field :value type (not= ValueType)))

(defn sub-field [field k] (->> field :value (filter #(= (:name %) k)) first))

(defn sub-fields [field] (when (record? field) (:value field)))

(defn sub-field-in [field [k & ks]]
  (if (empty? ks)
    (sub-field field k)
    (sub-field-in (sub-field field k) ks)))

(def ^:private value-type-tag "dendrite/value-type")

(def ^:private value-type-writer
  (reify WriteHandler
    (write [_ writer value-type]
      (doto writer
        (.writeTag value-type-tag 3)
        (.writeString (-> value-type :type name))
        (.writeString (-> value-type :encoding name))
        (.writeString (-> value-type :compression name))
        (.writeInt (:column-index value-type))
        (.writeInt (:repetition-level value-type))
        (.writeInt (:definition-level value-type))
        (.writeBoolean (:nested? value-type))))))

(def ^:private field-tag "dendrite/field")

(def ^:private field-writer
  (reify WriteHandler
    (write [_ writer field]
      (doto writer
        (.writeTag field-tag 3)
        (.writeString (-> field :name name))
        (.writeString (-> field :repetition name))
        (.writeObject (:value field))))))

(def ^:private write-handlers
  (-> (merge {ValueType {value-type-tag value-type-writer}
              Field {field-tag field-writer}}
             fressian/clojure-write-handlers)
      fressian/associative-lookup
      fressian/inheritance-lookup))

(def ^:private value-type-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (ValueType. (-> reader .readObject keyword)
                  (-> reader .readObject keyword)
                  (-> reader .readObject keyword)
                  (.readInt reader)
                  (.readInt reader)
                  (.readInt reader)
                  (.readBoolean reader)))))

(def ^:private field-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (Field. (-> reader .readObject keyword)
              (-> reader .readObject keyword)
              (-> reader .readObject)))))

(def ^:private read-handlers
  (-> (merge {value-type-tag value-type-reader
              field-tag field-reader}
             fressian/clojure-read-handlers)
      fressian/associative-lookup))

(defrecord Required [value])

(defmethod print-method Required
  [v ^Writer w]
  (.write w "#req ")
  (print-method (:value v) w))

(defn read-string [s]
  (edn/read-string {:readers {'req ->Required
                              'val map->value-type-with-defaults}}
                   s))

(defn- wrapped-required? [elem] (= (type elem) Required))

(defn- value-type? [elem] (= (type elem) ValueType))

(defmulti parse
  (fn [elem]
    (cond
     (or (symbol? elem) (value-type? elem)) :value-type
     (and (map? elem) ((some-fn value-type? symbol?) (-> elem first key))) :map
     (and (map? elem) (keyword? (-> elem first key))) :record
     (set? elem) :set
     (vector? elem) :vector
     (list? elem) :list
     :else (throw (IllegalArgumentException. (format "Unable to parse schema element %s" elem))))))

(defmethod parse :value-type
  [value-type]
  (if (symbol? value-type)
    (map->value-type-with-defaults {:type (keyword value-type)})
    value-type))

(defmethod parse :list
  [coll]
  (let [sub-schema (parse (first coll))]
    (if (= (type sub-schema) ValueType)
      (Field. nil :list sub-schema)
      (assoc sub-schema :repetition :list))))

(defmethod parse :vector
  [coll]
  (let [sub-schema (parse (first coll))]
    (if (= (type sub-schema) ValueType)
      (Field. nil :vector sub-schema)
      (assoc sub-schema :repetition :vector))))

(defmethod parse :set
  [coll]
  (let [sub-schema (parse (first coll))]
    (if (= (type sub-schema) ValueType)
      (Field. nil :set sub-schema)
      (assoc sub-schema :repetition :set))))

(defmethod parse :record
  [coll]
  (Field. nil :optional
          (for [[k v] coll :let [mark-required? (wrapped-required? v)
                                 v (if mark-required? (:value v) v)]]
            (let [parsed-v (parse v)
                  field (if (value-type? parsed-v)
                          (Field. k :optional parsed-v)
                          (assoc parsed-v :name k))]
              (cond-> field
                      mark-required? (assoc :repetition :required))))))

(defmethod parse :map
  [coll]
  (let [[key-elem val-elem] (first coll)]
    (Field. nil :map [(Field. :key :required (parse key-elem))
                      (Field. :value :required (parse key-elem))])))

(defn- column-indexed-schema [field current-column-index]
  (if (record? field)
    (let [[indexed-sub-fields next-index]
            (reduce (fn [[indexed-sub-fields i] sub-field]
                      (let [[indexed-sub-field next-index] (column-indexed-schema sub-field i)]
                        [(conj indexed-sub-fields indexed-sub-field) next-index]))
                    [[] current-column-index]
                    (sub-fields field))]
      [(assoc field :value indexed-sub-fields) next-index])
    [(update-in field [:value] assoc :column-index current-column-index)
     (inc current-column-index)]))

(defn- index-columns [schema]
  (first (column-indexed-schema schema 0)))

(defn- repeated? [{repetition :repetition}] (not (#{:optional :required} repetition)))

(defn- required? [{repetition :repetition}] (= repetition :required))

(defn- set-nested-flags [schema]
  (if (record? schema)
    (update-in schema [:value] #(mapv (fn [field]
                                        (if (or (record? field) (repeated? field))
                                          field
                                          (assoc-in field [:value :nested?] false)))
                                      %))
    (assoc-in schema [:value :nested?] false)))

(defn- schema-with-level [field current-level pred k]
  (if (record? field)
    (let [sub-fields
          (reduce (fn [sub-fields sub-field]
                    (let [next-level (if (pred sub-field) (inc current-level) current-level)]
                      (conj sub-fields (schema-with-level sub-field next-level pred k))))
                  []
                  (sub-fields field))]
      (assoc field :value sub-fields))
    (assoc-in field [:value k] current-level)))

(defn- set-definition-levels [schema]
  (schema-with-level schema 0 (complement required?) :definition-level))

(defn- set-repetition-levels [schema]
  (schema-with-level schema 0 repeated? :repetition-level))

(defn annotate [schema]
  (-> schema
      index-columns
      set-nested-flags
      set-definition-levels
      set-repetition-levels))

(defn- recursive-value-types [field previous-value-types]
  (if (record? field)
    (reduce (fn [value-types sub-field]
              (recursive-value-types sub-field value-types))
            previous-value-types
            (sub-fields field))
    (conj previous-value-types (:value field))))

(defn value-types [schema]
  (->> (recursive-value-types schema [])
       (sort-by :column-index)))

(defmulti human-readable type)

(defmethod human-readable ValueType
  [vt]
  (if (and (= (:compression vt) :none) (= (:encoding vt) :plain))
    (-> vt :type name symbol)
    (map->ValueType vt)))

(defmethod human-readable Field
  [field]
  (let [sub-edn (if (record? field)
                  (->> (:value field)
                       (map (fn [sub-field]
                              [(:name sub-field) (human-readable sub-field)]))
                       (into {}))
                  (cond-> (human-readable (:value field))
                          (= :required (:repetition field)) ->Required))]
    (case (:repetition field)
      :list (list sub-edn)
      :vector [sub-edn]
      :set #{sub-edn}
      :map {(-> sub-edn :key :value) (-> sub-edn :value :value)}
      sub-edn)))
