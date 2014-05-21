(ns dendrite.schema
  (:require [clojure.data.fressian :as fressian]
            [clojure.edn :as edn]
            [dendrite.common :refer :all]
            [dendrite.encoding :as encoding]
            [dendrite.compression :as compression])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [java.io Writer])
  (:refer-clojure :exclude [read-string val]))

(set! *warn-on-reflection* true)

(defrecord ColumnSpec [type encoding compression column-index max-repetition-level max-definition-level])

(defn- map->column-spec-with-defaults [m]
  (map->ColumnSpec (merge {:encoding :plain :compression :none} m)))

(def val map->column-spec-with-defaults)

(defmethod print-method ColumnSpec
  [v ^Writer w]
  (.write w (str "#val " (cond-> (dissoc v :column-index :max-definition-level :max-repetition-level)
                                 (= (:compression v) :none) (dissoc :compression)
                                 (= (:encoding v) :plain) (dissoc :encoding)))))

(defrecord Field [name repetition value repetition-level])

(defn record? [field] (-> field :value type (not= ColumnSpec)))

(defn sub-field [field k] (->> field :value (filter #(= (:name %) k)) first))

(defn sub-fields [field] (when (record? field) (:value field)))

(defn sub-field-in [field [k & ks]]
  (if (empty? ks)
    (sub-field field k)
    (sub-field-in (sub-field field k) ks)))

(defn- repeated? [{repetition :repetition}] (not (#{:optional :required} repetition)))

(defn- required? [{repetition :repetition}] (= repetition :required))

(def ^:private column-spec-tag "dendrite/column-spec")

(def ^:private column-spec-writer
  (reify WriteHandler
    (write [_ writer column-spec]
      (doto writer
        (.writeTag column-spec-tag 3)
        (.writeString (-> column-spec :type name))
        (.writeString (-> column-spec :encoding name))
        (.writeString (-> column-spec :compression name))
        (.writeInt (:column-index column-spec))
        (.writeInt (:max-repetition-level column-spec))
        (.writeInt (:max-definition-level column-spec))))))

(def ^:private field-tag "dendrite/field")

(def ^:private field-writer
  (reify WriteHandler
    (write [_ writer field]
      (doto writer
        (.writeTag field-tag 3)
        (.writeString (-> field :name name))
        (.writeString (-> field :repetition name))
        (.writeInt (:repetition-level field))
        (.writeObject (:value field))))))

(def ^:private write-handlers
  (-> (merge {ColumnSpec {column-spec-tag column-spec-writer}
              Field {field-tag field-writer}}
             fressian/clojure-write-handlers)
      fressian/associative-lookup
      fressian/inheritance-lookup))

(def ^:private column-spec-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (ColumnSpec. (-> reader .readObject keyword)
                   (-> reader .readObject keyword)
                   (-> reader .readObject keyword)
                   (.readInt reader)
                   (.readInt reader)
                   (.readInt reader)))))

(def ^:private field-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (Field. (-> reader .readObject keyword)
              (-> reader .readObject keyword)
              (.readInt reader)
              (.readObject reader)))))

(def ^:private read-handlers
  (-> (merge {column-spec-tag column-spec-reader
              field-tag field-reader}
             fressian/clojure-read-handlers)
      fressian/associative-lookup))

(defrecord Required [value])

(defmethod print-method Required
  [v ^Writer w]
  (.write w "#req ")
  (print-method (:value v) w))

(def req ->Required)

(defn read-string [s]
  (edn/read-string {:readers {'req ->Required
                              'val map->column-spec-with-defaults}}
                   s))

(defn- wrapped-required? [elem] (instance? Required elem))

(defn- column-spec? [elem] (instance? ColumnSpec elem))

(defmulti ^:private parse-tree
  (fn [elem parents]
    (cond
     (or (symbol? elem) (column-spec? elem)) :column-spec
     (and (map? elem) ((some-fn column-spec? symbol?) (-> elem first key))) :map
     (and (map? elem) (keyword? (-> elem first key))) :record
     (set? elem) :set
     (vector? elem) :vector
     (list? elem) :list
     :else (throw (IllegalArgumentException. (format "Unable to parse schema element %s" elem))))))

(defmethod parse-tree :column-spec
  [column-spec parents]
  (let [cs (if (symbol? column-spec)
             (map->column-spec-with-defaults {:type (keyword column-spec)})
             column-spec)]
    (when-not (encoding/valid-value-type? (:type cs))
      (throw (IllegalArgumentException.
              (format "Unsupported type '%s' for column %s" (:type cs) (format-ks parents)))))
    (when-not (encoding/valid-encoding-for-type? (:type cs) (:encoding cs))
      (throw (IllegalArgumentException.
              (format "Mismatched type '%s' and encoding '%s' for column %s"
                      (:type cs) (:encoding cs) (format-ks parents)))))
    (when-not (compression/valid-compression-type? (:compression cs))
      (throw (IllegalArgumentException.
              (format "Unsupported compression type '%s' for column" (:compression cs) (format-ks parents)))))
    cs))

(defmethod parse-tree :list
  [coll parents]
  (let [sub-schema (parse-tree (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :list :value sub-schema})
      (assoc sub-schema :repetition :list))))

(defmethod parse-tree :vector
  [coll parents]
  (let [sub-schema (parse-tree (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :vector :value sub-schema})
      (assoc sub-schema :repetition :vector))))

(defmethod parse-tree :set
  [coll parents]
  (let [sub-schema (parse-tree (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :set :value sub-schema})
      (assoc sub-schema :repetition :set))))

(defmethod parse-tree :record
  [coll parents]
  (map->Field
   {:repetition :optional
    :value (for [[k v] coll :let [mark-required? (wrapped-required? v)
                                  v (if mark-required? (:value v) v)]]
             (let [parsed-v (parse-tree v (conj parents k))
                   field (if (column-spec? parsed-v)
                           (map->Field {:name k :repetition :optional :value parsed-v})
                           (assoc parsed-v :name k))]
               (when (and mark-required? (repeated? field))
                 (throw (IllegalArgumentException.
                         (format "Field '%s' is marked both required and repeated"
                                 (format-ks (conj parents k))))))
               (cond-> field
                       mark-required? (assoc :repetition :required))))}))

(defmethod parse-tree :map
  [coll parents]
  (let [[key-elem val-elem] (first coll)]
    (map->Field
     {:repetition :map
      :value [(map->Field {:name :key :repetition :required :value (parse-tree key-elem parents)})
              (map->Field {:name :value :repetition :required :value (parse-tree val-elem parents)})]})))

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

(defn- schema-with-level [field current-level pred ks all-fields?]
  (if (record? field)
    (let [sub-fields
          (reduce (fn [sub-fields sub-field]
                    (let [next-level (if (pred sub-field) (inc current-level) current-level)]
                      (conj sub-fields (schema-with-level sub-field next-level pred ks all-fields?))))
                  []
                  (sub-fields field))]
      (cond-> (assoc field :value sub-fields)
              all-fields? (assoc-in ks current-level)))
    (assoc-in field ks current-level)))

(defn- set-definition-levels [schema]
  (schema-with-level schema 0 (complement required?) [:value :max-definition-level] false))

(defn- set-repetition-levels [schema]
  (-> schema
      (schema-with-level 0 repeated? [:value :max-repetition-level] false)
      (schema-with-level 0 repeated? [:repetition-level] true)))

(defn- set-top-record-required [schema]
  (assoc schema :repetition :required))

(defn- annotate [schema]
  (-> schema
      index-columns
      set-top-record-required
      set-repetition-levels
      set-definition-levels))

(defn parse [human-readable-schema]
  (try
    (-> human-readable-schema (parse-tree []) annotate)
    (catch Exception e
      (throw (IllegalArgumentException. (format "Failed to parse schema '%s'" human-readable-schema) e)))))

(defn- recursive-column-specs [field previous-column-specs]
  (if (record? field)
    (reduce (fn [column-specs sub-field]
              (recursive-column-specs sub-field column-specs))
            previous-column-specs
            (sub-fields field))
    (conj previous-column-specs (:value field))))

(defn column-specs [schema]
  (->> (recursive-column-specs schema [])
       (sort-by :column-index)))

(defmulti human-readable type)

(defmethod human-readable ColumnSpec
  [cs]
  (if (and (= (:compression cs) :none) (= (:encoding cs) :plain))
    (-> cs :type name symbol)
    (map->column-spec-with-defaults (select-keys cs [:type :encoding :compression]))))

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
