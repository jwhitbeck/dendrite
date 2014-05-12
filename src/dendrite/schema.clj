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

(defrecord ValueType [type encoding compression])

(defrecord Field [name repetition value])

(defn record? [field] (-> field :value type (not= ValueType)))

(def ^:private value-type-tag "dendrite/value-type")

(def ^:private value-type-writer
  (reify WriteHandler
    (write [_ writer value-type]
      (doto writer
        (.writeTag value-type-tag 3)
        (.writeString (-> value-type :type name))
        (.writeString (-> value-type :encoding name))
        (.writeString (-> value-type :compression name))))))

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
                  (-> reader .readObject keyword)))))

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
  (.write w (str "#req " (:value v))))

(defn read-string [s]
  (edn/read-string {:readers {'req ->Required}} s))

(defn- required? [elem] (= (type elem) Required))

(defmulti parse
  (fn [elem]
    (cond
     (and (map? elem) (list? (-> elem first key))) :map
     (and (map? elem) (keyword? (-> elem first key))) :record
     (set? elem) :set
     (vector? elem) :list
     (list? elem) :value-type
     :else (throw (IllegalArgumentException. (format "Unable to parse schema element %s" elem))))))

(defmethod parse :value-type
  [value-type]
  (let [[type-sym encoding compression] value-type]
    (ValueType. (keyword type-sym) (or encoding :plain) (or compression :none))))

(defmethod parse :list
  [coll]
  (let [sub-schema (parse (first coll))]
    (if (= (type sub-schema) ValueType)
      (Field. nil :list sub-schema)
      (assoc sub-schema :repetition :list))))

(defmethod parse :set
  [coll]
  (let [sub-schema (parse (first coll))]
    (if (= (type sub-schema) ValueType)
      (Field. nil :set sub-schema)
      (assoc sub-schema :repetition :set))))

(defmethod parse :record
  [coll]
  (Field. nil :optional
          (for [[k v] coll :let [mark-required? (required? v)
                                 v (if mark-required? (:value v) v)]]
            (let [parsed-v (parse v)
                  field (if (= (type parsed-v) ValueType)
                          (Field. k :optional parsed-v)
                          (assoc parsed-v :name k))]
              (cond-> field
                      mark-required? (assoc :repetition :required))))))

(defmethod parse :map
  [coll]
  (let [[key-elem val-elem] (first coll)]
    (Field. nil :map [(Field. :key :required (parse key-elem))
                      (Field. :value :required (parse key-elem))])))

(defmulti human-readable type)

(defmethod human-readable ValueType
  [vt]
  (let [type-symb (-> vt :type name symbol)]
    (if (= :none (:compression vt))
      (if (= :plain (:encoding vt))
        (list type-symb)
        (list type-symb (:encoding vt)))
      (list type-symb (:encoding vt) (:compression vt)))))

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
      :list [sub-edn]
      :set #{sub-edn}
      :map {(-> sub-edn :key :value) (-> sub-edn :value :value)}
      sub-edn)))
