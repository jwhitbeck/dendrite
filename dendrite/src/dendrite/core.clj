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
  (:import [dendrite.java Col LeveledValue Options IReader FileReader FilesReader Schema Types View FileWriter]
           [java.nio ByteBuffer]
           [java.util LinkedHashMap Map])
  (:refer-clojure :exclude [read map filter remove]))

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

(defn metadata
  "Returns the user-defined metadata for this reader."
  [^FileReader reader]
  (byte-buffer->edn (.getMetadata reader)))

(defn schema
  "Returns this reader's schema."
  [^FileReader reader]
  (.getSchema reader))

(defn plain-schema
  "Returns this reader's schema with all encodings set to plain and all compressions set to none."
  [^FileReader reader]
  (.getPlainSchema reader))

(defn files-reader
  "Returns a dendrite reader on the provided files (a seq of files or string paths). Reads will query each
  file in the provided order, opening and closing them as needed. Accepts all the same options as
  file-reader."
  (^dendrite.java.FilesReader [files] (files-reader nil files))
  (^dendrite.java.FilesReader [opts files]
    (FilesReader. (Options/getReaderOptions opts) (clojure.core/map io/as-file files))))

(defn file->stats
  "Returns a map of file to that file's stats. See the stats function for full details."
  [^FilesReader reader]
  (.getStatsByFile reader))

(defn file->metadata
  "Returns a map of file to that file's user-defined metadata."
  [^FilesReader reader]
  (reduce (fn [^Map hm [k v]]
            (doto hm
              (.put k (byte-buffer->edn v))))
          ; We use a LinkedHashMap to preserve the order of the files in the map.
          (LinkedHashMap.)
          (.getMetadataByFile reader)))

(defn file->schema
  "Returns a map of file to that file's schema."
  [^FilesReader reader]
  (.getSchemaByFile reader))

(defn file->plain-schema
  "Returns a map of file to that file's schema with with all encodings set to plain and all compressions set
  to none."
  [^FilesReader reader]
  (.getPlainSchemaByFile reader))

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
  :entrypoint             - a sequence of keys to begin the query within a subset of the schema. Cannot
                            contain any keys to repeated fields. See docs for full explanation.
  :readers                - a map of query tag symbol to tag function. Default: nil. See docs for full
                            explanation.
  :map-fn                 - apply this function to all records. This function is applied as part of the
                            parallel assembly process.
  :sample-fn              - only read the records such that (sample-fn i) evaluates truthfully, where i the
                            index of the record. Applied before record assembly. Default: nil.
  :filter-fn              - filter the records where (filter-fn record) evaluates thruthfully. Applied during
                            bundle assembly. Default nil."
  (^dendrite.java.View [^IReader reader] (read nil reader))
  (^dendrite.java.View [opts ^IReader reader] (.read reader (Options/getReadOptions opts))))

(defn map
  "Returns a view of the records with the provided function applied to them as part of record assembly. As
  with read, this view is seqable, reducible, and foldable. This is a convenience function such that (d/map
  f (d/read r)) is equivalent to (d/read {:map-fn f} r)."
  ^dendrite.java.View [f ^View view] (.withMapFn view f))

(defn sample
  "Returns a view of the records containing only those such that (f record-index) evaluates truthfully, where
  record-index goes from 0 (first record) to num-records - 1 (last record). The sampling occurs before record
  assembly thereby skipping assembly entirely for unselected records.  As with read, this view is seqable,
  reducible, and foldable. This is a convenience function such that (d/sample f (d/read r)) is equivalent
  to (d/read {:sample-fn f} r). A view can only have a single sample function applied to it."
  ^dendrite.java.View [f ^View view] (.withSampleFn view f))

(defn filter
  "Returns a view of the records containing only those such that (f record) evaluates truthfully. As
  with read, this view is seqable, reducible, and foldable. This is a convenience function such that
  (d/filter f (d/read r)) is equivalent to (d/read {:filter-fn f} r)."
  ^dendrite.java.View [f ^View view] (.withFilterFn view f))

(defn remove
  "Returns a view of the records not containing any of those such that (f record) evaluates thruthfully.. As
  with read, this view is seqable, reducible, and foldable. This is a convenience function such that
  (d/remove f (d/read r)) is equivalent to (d/read {:filter-fn (complement f)} r)."
  ^dendrite.java.View [f ^View view] (.withFilterFn view (complement f)))
