;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.core
  (:require [clojure.core.reducers]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint])
  (:import [dendrite.java Col LeveledValue Options IReader FileReader FilesReader PersistentRecord Schema
            Types View FileWriter]
           [java.nio ByteBuffer])
  (:refer-clojure :exclude [read map map-indexed keep keep-indexed filter remove]))

(set! *warn-on-reflection* true)

(defn col
  "Returns a column specification. Takes one to three arguments:
  - type:        the column type symbol (e.g. int)
  - encoding:    the column encoding symbol (default: plain)
  - compression: the column compression symbol (default: none)

  See README for all supported encoding/compression types."
  ([type] (Col. type))
  ([type encoding] (Col. type encoding))
  ([type encoding compression] (Col. type encoding compression)))

(defmethod print-method Col
  [v ^java.io.Writer w]
  (.write w (str v)))

(defmethod pprint/simple-dispatch Col
  [v]
  (.write *out* (str v)))

(defn req
  "Marks the enclosed schema element as required."
  [x]
  (Schema/req x))

(defmethod print-method Schema/REQUIRED_TYPE
  [v ^java.io.Writer w]
  (.write w "#req ")
  (print-method (Schema/unreq v) w))

(defmulti ^:private schema-dispatch type)

(defmethod schema-dispatch Schema/REQUIRED_TYPE
  [v]
  (.write *out* "#req ")
  (pprint/simple-dispatch (Schema/unreq v)))

(defmethod schema-dispatch :default
  [v]
  (pprint/simple-dispatch v))

(extend-protocol clojure.core.protocols/IKVReduce
  PersistentRecord
  (kv-reduce [amap f init]
    (.kvreduce amap f init)))

(defn pprint
  "Pretty-prints the schema."
  [schema]
  (pprint/with-pprint-dispatch schema-dispatch
    (pprint/pprint schema)))

(defn tag
  "Tags the enclosed query element with the provided tag. Meant to be used in combination with the :readers
  option."
  [tag elem]
  (Schema/tag tag elem))

(defmethod print-method Schema/TAGGED_TYPE
  [v ^java.io.Writer w]
  (.write w (format "#%s " (-> v Schema/getTag name)))
  (print-method (Schema/untag v) w))

(defn read-schema-string
  "Parse an edn-formatted dendrite schema string."
  [s]
  (Schema/readString s))

(defn file-writer
  "Returns a dendrite writer that outputs to a file according to the provided schema.

  If provided, the options map supports the following keys:
  :data-page-length        - the length in bytes of the data pages (default 262144)
  :record-group-length     - the length in bytes of each record group (default 134217728)
  :optimize-columns?       - either :all, :none or :default. If :all, will attempt to optimize the
                             encoding and compression for each column; if :default, will only optimize
                             columns with the default encoding & compression (i.e., plain/none); if :none,
                             disables all optimization.
  :compression-thresholds  - a map of compression method (e.g., deflate) to the minimum compression ratio
                             (e.g., 2) below which the overhead of compression is not not deemed worthwhile.
                             Default: {'deflate 1.5}
  :invalid-input-handler   - a function with two arguments: record and exception. If an input record does
                             not conform to the schema, it will be passed to this function along with the
                             exception it triggered. By default, this option is nil and exceptions
                             triggered by invalid records are not caught.
  :custom-types            - a map of of custom-type symbol to custom-type specification. See docs for
                             full explanation.
  :map-fn                  - apply this function to all written records before striping them to columns. This
                             function is applied as part of the parallelized striping process."
  (^dendrite.java.FileWriter [schema file] (file-writer nil schema file))
  (^dendrite.java.FileWriter [opts schema file]
                             (FileWriter/create (Options/getWriterOptions opts) schema (io/as-file file))))

(defn set-metadata!
  "Sets the user-defined metadata for this writer."
  [^FileWriter writer metadata]
  (.setMetadata writer (-> metadata pr-str Types/toByteArray ByteBuffer/wrap)))

(defn file-reader
  "Returns a dendrite reader for the provided file.

  If provided, the options map supports the following keys:
  :custom-types  - a map of of custom-type symbol to custom-type specification. Default: nil. See docs for
                  full explanation."
  (^dendrite.java.FileReader [file] (file-reader nil file))
  (^dendrite.java.FileReader [opts file]
                             (FileReader/create (Options/getReaderOptions opts) (io/as-file file))))

(defn- byte-buffer->edn [^ByteBuffer byte-buffer]
  (-> byte-buffer Types/toByteArray Types/toString edn/read-string))

(defn stats
  "Returns a map containing all the stats associated with this reader. The tree top-level keys
  are :global, :record-groups, and :columns, that, respectively, contain stats summed over the entire file,
  summed across all column-chunks in the same record-groups, and summed across all column-chunks belonging to
  the same column."
  [^FileReader reader]
  (.getStats reader))

(defn num-records
  "Returns the number of records in the file."
  [^FileReader reader]
  (.getNumRecords reader))

(defn metadata
  "Returns the user-defined metadata for this reader."
  [^FileReader reader]
  (byte-buffer->edn (.getMetadata reader)))

(defn custom-types
  "Returns a map of custom-type to base-type."
  [^FileReader reader]
  (.getCustomTypeMappings reader))

(defn schema
  "Returns this reader's schema."
  [^FileReader reader]
  (.getPlainSchema reader))

(defn full-schema
  "Returns this reader's schema with all encoding and compression annotations."
  [^FileReader reader]
  (.getSchema reader))

(defn files-reader
  "Returns a dendrite reader on the provided files (a seq of files or string paths). Reads will query each
  file in the provided order, opening and closing them as needed. Accepts all the same options as
  file-reader."
  (^dendrite.java.FilesReader [files] (files-reader nil files))
  (^dendrite.java.FilesReader [opts files]
    (FilesReader. (Options/getReaderOptions opts) (clojure.core/map io/as-file files))))

(extend-type View
  clojure.core.protocols/CollReduce
  (coll-reduce
    ([this f] (.reduce this f))
    ([this f init] (.reduce this f init)))
  clojure.core.reducers/CollFold
  (coll-fold [this n combinef reducef]
    (.fold this n combinef reducef)))

(defn read
  "Returns a view of all the records in the reader. This view is seqable (lazy), reducible, and foldable (per
  clojure.core.reducers, in which case the folding is done as part of record assembly).

  If provided, the options map supports the following keys:
  :missing-fields-as-nil? - should be true (default) or false. If true, then fields that are specified in the
                            query but are not present in this reader's schema will be read as nil values. If
                            false, querying for fields not present in the schema will throw an exception.
  :query                  - the query. Default: '_. See docs for full explanation.
  :sub-schema-in          - path to the desired sub-schema. The value should be a sequence of keys that cannot
                            contain any keys to repeated elements. If both :sub-schema-in and :query are
                            defined, the query applies to the specified sub-schema. See docs for full
                            explanation.
  :readers                - a map of query tag symbol to tag function. Default: nil. See docs for full
                            explanation."
  (^dendrite.java.View [^IReader reader] (read nil reader))
  (^dendrite.java.View [opts ^IReader reader] (.read reader (Options/getReadOptions opts))))

(defn sample
  "Returns a view of the records containing only those such that (f record-index) evaluates truthfully, where
  record-index goes from 0 (first record) to num-records - 1 (last record). The sampling occurs before record
  assembly thereby skipping assembly entirely for unselected records. As with read, this view is seqable,
  reducible, and foldable. A view can only have a single sample function applied to it and the sample
  function must be applied before any mapping or filtering function."
  ^dendrite.java.View [f ^View view] (.withSampleFn view f))

(defn map
  "Returns a view of the records with the provided function mapped on to each record as part of record
  assembly. As with read, this view is seqable, reducible, and foldable. Calling (d/map f (d/read r)) is
  equivalent to (map f (d/read r)) but more efficient."
  ^dendrite.java.View [f ^View view] (.withMapFn view f))

(defn map-indexed
  "Returns a view of the records with the provided function mapped on to each record and its index as part of
  record assembly. As with read, this view is seqable, reducible, and foldable. Calling
  (d/map-indexed f (d/read r)) is equivalent to (map-indexed f (d/read r)) but more efficient. However, this
  equivalence does not extend to chaining calls to d/map-indexed or any other d/*-indexed function. Indeed the
  index argument passed to f is always the record's index in the file, wheread in clojure.core/map-indexed, a
  new index is generated for each nested lazy-seq. Therefore (->> (d/read r) (d/map-indexed f) (d/map-indexed
  g)) will not in general be equivalent to (->> (d/read r) (map-indexed f) (map-indexed f))."
  ^dendrite.java.View [f ^View view] (.withMapIndexedFn view f))

(defn keep
  "Returns a view of the records with the provided function mapped on to each record as part of record
  assembly and all nil values removed. As with read, this view is seqable, reducible, and
  foldable. Calling (d/keep f (d/read r)) is equivalent to (keep f (d/read r)) but more efficient."
  ^dendrite.java.View [f ^View view] (.withKeepFn view f))

(defn keep-indexed
  "Returns a view of the records with the provided function mapped on to each record and its index as part of
  record assembly and all nil values removed. As with read, this view is seqable, reducible, and
  foldable. Calling (d/keep-indexed f (d/read r)) is equivalent to (keep-indexed f (d/read r)) but more
  efficient. However, this equivalence do not extend to nested calls to keep-indexed. See map-indexed for
  details."
  ^dendrite.java.View [f ^View view] (.withKeepIndexedFn view f))

(defn filter
  "Returns a view of the records containing only those such that (f record) evaluates truthfully. The
  filtering is done as part of record assembly. As with read, this view is seqable, reducible, and
  foldable. Calling (d/filter f (d/read r)) is equivalent to (filter f (d/read r)) but more efficient."
  ^dendrite.java.View [f ^View view] (.withFilterFn view f))

(defn filter-indexed
  "Returns a view of the records containing only those such that (f index record) evaluates truthfully. The
  filtering is done as part of record assembly. As with read, this view is seqable, reducible, and
  foldable. However, this equivalence do not extend to nested calls to keep-indexed. See map-indexed for
  details."
  ^dendrite.java.View [f ^View view] (.withFilterIndexedFn view f))

(defn remove
  "Returns a view of the records not containing any of those such that (f record) evaluates truthfully. The
  removal is done as part of record assembly. As with read, this view is seqable, reducible, and
  foldable. Calling (d/remove f (d/read r)) is equivalent to (remove f (d/read r)) but more efficient."
  ^dendrite.java.View [f ^View view] (.withFilterFn view (complement f)))

(defn remove-indexed
  "Returns a view of the records not containing any of those such that (f index record) evaluates truthfully.
  The removal is done as part of record assembly. As with read, this view is seqable, reducible, and
  foldable. However, this equivalence do not extend to nested calls to keep-indexed. See map-indexed for
  details."
  ^dendrite.java.View [f ^View view] (.withFilterIndexedFn view (complement f)))
