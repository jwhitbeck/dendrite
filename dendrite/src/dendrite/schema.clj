(ns dendrite.schema
  (:require [clojure.edn :as edn]
            [clojure.string :as string]
            [dendrite.encoding :as encoding]
            [dendrite.compression :as compression]
            [dendrite.metadata :refer [map->column-spec-with-defaults map->Field] :as metadata]
            [dendrite.utils :refer [format-ks]])
  (:import [dendrite.metadata ColumnSpec Field]
           [java.io Writer])
  (:refer-clojure :exclude [read-string col record?]))

(set! *warn-on-reflection* true)

(defn col
  ([type]
     (map->column-spec-with-defaults {:type (keyword type)}))
  ([type encoding]
     (map->column-spec-with-defaults {:type (keyword type) :encoding (keyword encoding)}))
  ([type encoding compression]
     (map->column-spec-with-defaults {:type (keyword type)
                                      :encoding (keyword encoding)
                                      :compression (keyword compression)})))

(defn record? [field] (-> field :sub-fields empty? not))

(defn- repeated? [{repetition :repetition}] (not (#{:optional :required} repetition)))

(defn- required? [{repetition :repetition}] (= repetition :required))

(defn sub-field [field k] (->> field :sub-fields (filter #(= (:name %) k)) first))

(defn sub-field-in [field [k & ks]]
  (if (empty? ks)
    (sub-field field k)
    (sub-field-in (sub-field field k) ks)))

(defrecord RequiredField [field])

(defmethod print-method RequiredField
  [v ^Writer w]
  (.write w "#req ")
  (print-method (:field v) w))

(def req ->RequiredField)

(defn read-string [s]
  (edn/read-string {:readers {'req ->RequiredField
                              'col metadata/read-column-spec}}
                   s))

(defn- required-field? [elem] (instance? RequiredField elem))

(defn- column-spec? [elem] (instance? ColumnSpec elem))

(defmulti ^:private parse*
  (fn [elem parents]
    (cond
     (or (symbol? elem) (column-spec? elem)) :column-spec
     (and (map? elem) ((some-fn column-spec? symbol?) (-> elem first key))) :map
     (and (map? elem) (keyword? (-> elem first key))) :record
     (set? elem) :set
     (vector? elem) :vector
     (list? elem) :list
     :else (throw (IllegalArgumentException. (format "Unable to parse schema element %s" elem))))))

(defmethod parse* :column-spec
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
    (when-not (metadata/is-compression-type? (:compression cs))
      (throw (IllegalArgumentException.
              (format "Unsupported compression type '%s' for column %s"
                      (:compression cs)
                      (format-ks parents)))))
    cs))

(defmethod parse* :list
  [coll parents]
  (let [sub-schema (parse* (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :list :column-spec sub-schema})
      (assoc sub-schema :repetition :list))))

(defmethod parse* :vector
  [coll parents]
  (let [sub-schema (parse* (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :vector :column-spec sub-schema})
      (assoc sub-schema :repetition :vector))))

(defmethod parse* :set
  [coll parents]
  (let [sub-schema (parse* (first coll) parents)]
    (if (column-spec? sub-schema)
      (map->Field {:repetition :set :column-spec sub-schema})
      (assoc sub-schema :repetition :set))))

(defmethod parse* :record
  [coll parents]
  (map->Field
   {:repetition :optional
    :sub-fields (for [[k v] coll :let [mark-required? (required-field? v)
                                       v (if mark-required? (:field v) v)]]
                  (let [parsed-v (parse* v (conj parents k))
                        field (if (column-spec? parsed-v)
                                (map->Field {:name k :repetition :optional :column-spec parsed-v})
                                (assoc parsed-v :name k))]
                    (when (and mark-required? (repeated? field))
                      (throw (IllegalArgumentException.
                              (format "Field %s is marked both required and repeated"
                                      (format-ks (conj parents k))))))
                    (cond-> field
                            mark-required? (assoc :repetition :required))))}))

(defmethod parse* :map
  [coll parents]
  (let [[key-tree val-tree] (map #(parse* % parents) (first coll))]
    (map->Field
     {:repetition :map
      :sub-fields [(-> {:name :key :repetition :required}
                       (assoc (if (column-spec? key-tree) :column-spec :sub-fields) key-tree)
                       map->Field)
                   (-> {:name :value :repetition :required}
                       (assoc (if (column-spec? key-tree) :column-spec :sub-fields) val-tree)
                       map->Field)]})))

(defn- column-indexed-schema [field current-column-index index-keyword]
  (if (record? field)
    (let [[indexed-sub-fields next-index]
            (reduce (fn [[indexed-sub-fields i] sub-field]
                      (let [[indexed-sub-field next-index] (column-indexed-schema sub-field i index-keyword)]
                        [(conj indexed-sub-fields indexed-sub-field) next-index]))
                    [[] current-column-index]
                    (:sub-fields field))]
      [(assoc field :sub-fields indexed-sub-fields) next-index])
    [(update-in field [:column-spec] assoc index-keyword current-column-index)
     (inc current-column-index)]))

(defn- with-column-indices [schema index-keyword]
  (first (column-indexed-schema schema 0 index-keyword)))

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

(defn- with-definition-levels [schema]
  (schema-with-level schema 0 (complement required?) [:column-spec :max-definition-level] false))

(defn- with-repetition-levels [schema]
  (-> schema
      (schema-with-level 0 repeated? [:column-spec :max-repetition-level] false)
      (schema-with-level 0 repeated? [:repetition-level] true)))

(defn- with-top-record-required [schema]
  (assoc schema :repetition :required))

(defn- with-column-spec-paths* [field path]
  (let [next-path (if (:name field) (conj path (:name field)) path)]
    (if (record? field)
      (update-in field [:sub-fields] (partial map #(with-column-spec-paths* % next-path)))
      (assoc-in field [:column-spec :path] next-path))))

(defn- with-column-spec-paths [schema]
  (with-column-spec-paths* schema []))

(defn- with-map-fns [field]
  (if (record? field)
    (update-in field [:sub-fields] (partial map with-map-fns))
    (assoc-in field [:column-spec :map-fn] (:reader-fn field))))

(defn- with-optimal-column-specs* [field optimal-column-specs-map]
  (if (record? field)
    (update-in field [:sub-fields] (partial map #(with-optimal-column-specs* % optimal-column-specs-map)))
    (assoc field :column-spec (->> (get-in field [:column-spec :column-index])
                                   (get optimal-column-specs-map)))))

(defn with-optimal-column-specs [schema optimal-column-specs]
  (with-optimal-column-specs* schema (->> optimal-column-specs
                                          (map (juxt :column-index identity))
                                          (into {}))))

(defn- annotate [schema]
  (-> schema
      (with-column-indices :column-index)
      with-top-record-required
      with-repetition-levels
      with-definition-levels))

(defn parse [unparsed-schema]
  (try
    (-> unparsed-schema (parse* []) annotate)
    (catch Exception e
      (throw (IllegalArgumentException. (format "Failed to parse schema '%s'" unparsed-schema) e)))))

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

(defn- root? [field] (nil? (:name field)))

(defmulti unparse type)

(defmethod unparse ColumnSpec
  [cs]
  (if (and (= (:compression cs) :none) (= (:encoding cs) :plain))
    (-> cs :type name symbol)
    (map->column-spec-with-defaults (select-keys cs [:type :encoding :compression]))))

(defmethod unparse Field
  [field]
  (let [sub-edn (if (record? field)
                  (->> (:sub-fields field)
                       (map (fn [sub-field]
                              [(:name sub-field) (unparse sub-field)]))
                       (into {}))
                  (unparse (:column-spec field)))]
    (case (:repetition field)
      :list (list sub-edn)
      :vector [sub-edn]
      :set #{sub-edn}
      :map {(-> sub-edn :key :field) (-> sub-edn :value :field)}
      :required (if (root? field) sub-edn (->RequiredField sub-edn))
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

(defmulti ^:private apply-query*
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

(defmethod apply-query* :tagged
  [sub-schema tagged-query readers missing-fields-as-nil? parents]
  (if-let [reader-fn (get readers (:tag tagged-query))]
    (-> (apply-query* sub-schema (:field tagged-query) readers missing-fields-as-nil? parents)
        (assoc :reader-fn reader-fn))
    (throw (IllegalArgumentException.
            (format "No reader function was provided for tag '%s'" (:tag tagged-query))))))

(defmethod apply-query* :symbol
  [sub-schema query-symbol readers missing-fields-as-nil? parents]
  (if (= query-symbol sub-schema-selector-symbol)
    sub-schema
    (if (record? sub-schema)
      (throw (IllegalArgumentException.
              (format "Field %s is a record field in schema, not a value" (format-ks parents))))
      (let [queried-type (keyword query-symbol)
            schema-type (-> sub-schema :column-spec :type)]
        (if-not (= queried-type schema-type)
          (throw (IllegalArgumentException.
                  (format "Mismatched column types for field %s. Asked for '%s' but schema defines '%s'."
                          (format-ks parents) (name queried-type) (name schema-type))))
          sub-schema)))))

(defmethod apply-query* :record
  [sub-schema query-record readers missing-fields-as-nil? parents]
  (if-not (record? sub-schema)
    (throw (IllegalArgumentException.
            (format "Field %s is a value field in schema, not a record." (format-ks parents))))
    (do
      (when-not missing-fields-as-nil?
        (let [missing-fields (remove (->> sub-schema :sub-fields (map :name) set) (keys query-record))]
          (when-not (empty? missing-fields)
            (throw (IllegalArgumentException.
                    (format "The following fields don't exist: %s"
                            (string/join ", " (map #(format-ks (conj parents %)) missing-fields))))))))
      (let [sub-fields (->> (:sub-fields sub-schema)
                            (filter (comp query-record :name))
                            (mapv #(apply-query* % (get query-record (:name %))
                                                 readers
                                                 missing-fields-as-nil?
                                                 (conj parents (:name %)))))]
        (assoc sub-schema :sub-fields sub-fields)))))

(defmethod apply-query* :list
  [sub-schema query-list readers missing-fields-as-nil? parents]
  (let [compatible-repetition-types #{:list :vector :set :map}]
    (if-not (compatible-repetition-types (:repetition sub-schema))
      (throw (IllegalArgumentException.
              (format "Field %s contains a %s in the schema, cannot be read as a list."
                      (format-ks parents) (-> sub-schema :repetition name))))
      (-> (apply-query* sub-schema (first query-list) readers missing-fields-as-nil? parents)
          (assoc :repetition :list)))))

(defmethod apply-query* :vector
  [sub-schema query-vec readers missing-fields-as-nil? parents]
  (let [compatible-repetition-types #{:list :vector :set :map}]
    (if-not (compatible-repetition-types (:repetition sub-schema))
      (throw (IllegalArgumentException.
              (format "Field %s contains a %s in the schema, cannot be read as a vector."
                      (format-ks parents) (-> sub-schema :repetition name))))
      (-> (apply-query* sub-schema (first query-vec) readers missing-fields-as-nil? parents)
          (assoc :repetition :vector)))))

(defmethod apply-query* :set
  [sub-schema query-set readers missing-fields-as-nil? parents]
  (if-not (= :set (:repetition sub-schema))
    (throw (IllegalArgumentException.
            (format "Field %s contains a %s in the schema, cannot be read as a set."
                    (format-ks parents) (-> sub-schema :repetition name))))
    (apply-query* sub-schema (first query-set) readers missing-fields-as-nil? parents)))

(defmethod apply-query* :map
  [sub-schema query-map readers missing-fields-as-nil? parents]
  (if-not (= :map (:repetition sub-schema))
    (throw (IllegalArgumentException.
            (format "Field %s contains a %s in the schema, cannot be read as a map."
                    (format-ks parents) (-> sub-schema :repetition name))))
    (let [[key-query value-query] (-> query-map first ((juxt key val)))
          key-sub-schema (apply-query* (sub-field sub-schema :key)
                                       key-query
                                       readers
                                       missing-fields-as-nil?
                                       parents)
          value-sub-schema (apply-query* (sub-field sub-schema :value)
                                         value-query
                                         readers
                                         missing-fields-as-nil?
                                         parents)]
      (assoc sub-schema :sub-fields [key-sub-schema value-sub-schema]))))

(defn apply-query
  ([schema query] (apply-query schema query true {}))
  ([schema query missing-fields-as-nil? readers]
     (try
       (-> schema
           (apply-query* query readers missing-fields-as-nil? [])
           (with-column-indices :query-column-index)
           with-column-spec-paths
           with-map-fns)
       (catch Exception e
         (throw (IllegalArgumentException.
                 (format "Invalid query '%s' for schema '%s'" query (unparse schema))
                 e))))))

(defn queried-column-indices-set [queried-schema]
  (->> queried-schema column-specs (map :column-index) set))