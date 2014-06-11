(ns dendrite.schema
  (:require [clojure.data.fressian :as fressian]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [dendrite.common :refer :all]
            [dendrite.encoding :as encoding]
            [dendrite.compression :as compression])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [java.io Writer])
  (:refer-clojure :exclude [read-string col]))

(set! *warn-on-reflection* true)

(defrecord ColumnSpec [type encoding compression column-index max-repetition-level max-definition-level])

(defn- map->column-spec-with-defaults [m]
  (map->ColumnSpec (merge {:encoding :plain :compression :none} m)))

(def col map->column-spec-with-defaults)

(defmethod print-method ColumnSpec
  [v ^Writer w]
  (.write w (str "#col " (cond-> (dissoc v :column-index :max-definition-level :max-repetition-level)
                                 (= (:compression v) :none) (dissoc :compression)
                                 (= (:encoding v) :plain) (dissoc :encoding)))))

(defrecord Field [name repetition repetition-level reader-fn column-spec sub-fields])

(defn record? [field] (-> field :sub-fields empty? not))

(defn- repeated? [{repetition :repetition}] (not (#{:optional :required} repetition)))

(defn- required? [{repetition :repetition}] (= repetition :required))

(defn sub-field [field k] (->> field :sub-fields (filter #(= (:name %) k)) first))

(defn sub-field-in [field [k & ks]]
  (if (empty? ks)
    (sub-field field k)
    (sub-field-in (sub-field field k) ks)))

(def ^:private column-spec-tag "dendrite/column-spec")

(def ^:private column-spec-writer
  (reify WriteHandler
    (write [_ writer column-spec]
      (doto writer
        (.writeTag column-spec-tag 6)
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
        (.writeTag field-tag 5)
        (.writeString (-> field :name name))
        (.writeString (-> field :repetition name))
        (.writeInt (:repetition-level field))
        (.writeObject (:column-spec field))
        (.writeObject (:sub-fields field))))))

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
              nil ; the reader-fn is not serialized
              (.readObject reader)
              (.readObject reader)))))

(def ^:private read-handlers
  (-> (merge {column-spec-tag column-spec-reader
              field-tag field-reader}
             fressian/clojure-read-handlers)
      fressian/associative-lookup))

(defrecord RequiredField [field])

(defmethod print-method RequiredField
  [v ^Writer w]
  (.write w "#req ")
  (print-method (:field v) w))

(def req ->RequiredField)

(defn read-string [s]
  (edn/read-string {:readers {'req ->RequiredField
                              'col map->column-spec-with-defaults}}
                   s))

(defn- required-field? [elem] (instance? RequiredField elem))

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
      (map->Field {:repetition :list :column-spec sub-schema})
      (assoc sub-schema :repetition :list))))

(defmethod parse-tree :vector
  [coll parents]
  (let [sub-schema (parse-tree (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :vector :column-spec sub-schema})
      (assoc sub-schema :repetition :vector))))

(defmethod parse-tree :set
  [coll parents]
  (let [sub-schema (parse-tree (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :set :column-spec sub-schema})
      (assoc sub-schema :repetition :set))))

(defmethod parse-tree :record
  [coll parents]
  (map->Field
   {:repetition :optional
    :sub-fields (for [[k v] coll :let [mark-required? (required-field? v)
                                       v (if mark-required? (:field v) v)]]
                  (let [parsed-v (parse-tree v (conj parents k))
                        field (if (column-spec? parsed-v)
                                (map->Field {:name k :repetition :optional :column-spec parsed-v})
                                (assoc parsed-v :name k))]
                    (when (and mark-required? (repeated? field))
                      (throw (IllegalArgumentException.
                              (format "Field '%s' is marked both required and repeated"
                                      (format-ks (conj parents k))))))
                    (cond-> field
                            mark-required? (assoc :repetition :required))))}))

(defmethod parse-tree :map
  [coll parents]
  (let [[key-tree val-tree] (map #(parse-tree % parents) (first coll))]
    (map->Field
     {:repetition :map
      :sub-fields [(-> {:name :key :repetition :required}
                       (assoc (if (column-spec? key-tree) :column-spec :sub-fields) key-tree)
                       map->Field)
                   (-> {:name :value :repetition :required}
                       (assoc (if (column-spec? key-tree) :column-spec :sub-fields) val-tree)
                       map->Field)]})))

(defn- column-indexed-schema [field current-column-index]
  (if (record? field)
    (let [[indexed-sub-fields next-index]
            (reduce (fn [[indexed-sub-fields i] sub-field]
                      (let [[indexed-sub-field next-index] (column-indexed-schema sub-field i)]
                        [(conj indexed-sub-fields indexed-sub-field) next-index]))
                    [[] current-column-index]
                    (:sub-fields field))]
      [(assoc field :sub-fields indexed-sub-fields) next-index])
    [(update-in field [:column-spec] assoc :column-index current-column-index)
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
                  (:sub-fields field))]
      (cond-> (assoc field :sub-fields sub-fields)
              all-fields? (assoc-in ks current-level)))
    (assoc-in field ks current-level)))

(defn- set-definition-levels [schema]
  (schema-with-level schema 0 (complement required?) [:column-spec :max-definition-level] false))

(defn- set-repetition-levels [schema]
  (-> schema
      (schema-with-level 0 repeated? [:column-spec :max-repetition-level] false)
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
            (:sub-fields field))
    (conj previous-column-specs (:column-spec field))))

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
                  (->> (:sub-fields field)
                       (map (fn [sub-field]
                              [(:name sub-field) (human-readable sub-field)]))
                       (into {}))
                  (cond-> (human-readable (:column-spec field))
                          (= :required (:repetition field)) ->RequiredField))]
    (case (:repetition field)
      :list (list sub-edn)
      :vector [sub-edn]
      :set #{sub-edn}
      :map {(-> sub-edn :key :field) (-> sub-edn :value :field)}
      sub-edn)))

(defrecord TaggedField [tag field])

(defmethod print-method TaggedField
  [v ^Writer w]
  (.write w (format "#%s " (-> v :tag name)))
  (print-method (:field v) w))

(def tag ->TaggedField)

(defn read-query-string [query-string]
  (edn/read-string {:default tag} query-string))

(def ^:private sub-schema-selector-symbol '_)

(defmulti ^:private sub-schema-for-query*
  (fn [sub-schema query readers missing-fields-as-nil? parents]
    (cond
     (instance? TaggedField query) :tagged
     (and (map? query) (keyword? (-> query first key))) :record
     (map? query) :map
     (set? query) :set
     (vector? query) :vector
     (list? query) :list
     (symbol? query) :symbol
     :else (throw (IllegalArgumentException. (format "Unable to parse query element %s" query))))))

(defmethod sub-schema-for-query* :tagged
  [sub-schema tagged-query readers missing-fields-as-nil? parents]
  (-> (sub-schema-for-query* sub-schema (:field tagged-query) readers missing-fields-as-nil? parents)
      (assoc :reader-fn (get readers (:tag tagged-query)))))

(defmethod sub-schema-for-query* :symbol
  [sub-schema query-symbol readers missing-fields-as-nil? parents]
  (if (= query-symbol sub-schema-selector-symbol)
    sub-schema
    (if (record? sub-schema)
      (throw (IllegalArgumentException.
              (format "Field '%s' is a record field in schema, not a value" (format-ks parents))))
      (let [queried-type (keyword query-symbol)
            schema-type (-> sub-schema :column-spec :type)]
        (if-not (= queried-type schema-type)
          (throw (IllegalArgumentException.
                  (format "Mismatched column types for field '%s'. Asked for '%s' but schema defines '%s'."
                          (format-ks parents) (name queried-type) (name schema-type))))
          sub-schema)))))

(defmethod sub-schema-for-query* :record
  [sub-schema query-record readers missing-fields-as-nil? parents]
  (if-not (record? sub-schema)
    (throw (IllegalArgumentException.
            (format "Field '%s' is a value field in schema, not a record." (format-ks parents))))
    (do
      (when-not missing-fields-as-nil?
        (let [missing-fields (remove (->> sub-schema :sub-fields (map :name) set) (keys query-record))]
          (when-not (empty? missing-fields)
            (throw (IllegalArgumentException.
                    (format "In field '%s', the following sub-fields don't exist: '%s'"
                            (format-ks parents) (string/join ", " missing-fields)))))))
      (let [sub-fields (->> (:sub-fields sub-schema)
                            (filter (comp query-record :name))
                            (mapv #(sub-schema-for-query* % (get query-record (:name %))
                                                          readers
                                                          missing-fields-as-nil?
                                                          (conj parents (:name %)))))]
        (assoc sub-schema :sub-fields sub-fields)))))

(defmethod sub-schema-for-query* :list
  [sub-schema query-list readers missing-fields-as-nil? parents]
  (let [compatible-repetition-types #{:list :vector :set :map}]
    (if-not (compatible-repetition-types (:repetition sub-schema))
      (throw (IllegalArgumentException.
              (format "Field '%s' contains a %s in the schema, cannot be read as a list."
                      (format-ks parents) (-> sub-schema :repetition name))))
      (-> (sub-schema-for-query* sub-schema (first query-list) readers missing-fields-as-nil? parents)
          (assoc :repetition :list)))))

(defmethod sub-schema-for-query* :vector
  [sub-schema query-vec readers missing-fields-as-nil? parents]
  (let [compatible-repetition-types #{:list :vector :set :map}]
    (if-not (compatible-repetition-types (:repetition sub-schema))
      (throw (IllegalArgumentException.
              (format "Field '%s' contains a %s in the schema, cannot be read as a vector."
                      (format-ks parents) (-> sub-schema :repetition name))))
      (-> (sub-schema-for-query* sub-schema (first query-vec) readers missing-fields-as-nil? parents)
          (assoc :repetition :vector)))))

(defmethod sub-schema-for-query* :set
  [sub-schema query-set readers missing-fields-as-nil? parents]
  (if-not (= :set (:repetition sub-schema))
    (throw (IllegalArgumentException.
            (format "Field '%s' contains a %s in the schema, cannot be read as a set."
                    (format-ks parents) (-> sub-schema :repetition name))))
    (sub-schema-for-query* sub-schema (first query-set) readers missing-fields-as-nil? parents)))

(defmethod sub-schema-for-query* :map
  [sub-schema query-map readers missing-fields-as-nil? parents]
  (if-not (= :map (:repetition sub-schema))
    (throw (IllegalArgumentException.
            (format "Field '%s' contains a %s in the schema, cannot be read as a map."
                    (format-ks parents) (-> sub-schema :repetition name))))
    (let [[key-query value-query] (-> query-map first ((juxt key val)))
          key-sub-schema (sub-schema-for-query* (sub-field sub-schema :key)
                                                key-query
                                                readers
                                                missing-fields-as-nil?
                                                parents)
          value-sub-schema (sub-schema-for-query* (sub-field sub-schema :value)
                                                  value-query
                                                  readers
                                                  missing-fields-as-nil?
                                                  parents)]
      (assoc sub-schema :sub-fields [key-sub-schema value-sub-schema]))))

(defn sub-schema-for-query
  [schema query & {:keys [missing-fields-as-nil? readers] :or {missing-fields-as-nil? true}}]
  (try
    (-> schema
        (sub-schema-for-query* query readers missing-fields-as-nil? [])
        index-columns)
    (catch Exception e
      (throw (IllegalArgumentException.
              (format "Invalid query '%s' for schema '%s'" query (human-readable schema))
              e)))))
