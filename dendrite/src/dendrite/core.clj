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
  (:import [dendrite.java Col LeveledValue  Options Reader Schema Types View Writer]
           [java.nio ByteBuffer])
  (:refer-clojure :exclude [read pmap]))

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

(defn plain
  "Returns the provided schema with all ecodings set to plain and all compressions set to none."
  [schema]
  (Schema/plain (Types/create) schema))

(defn reader
  "Returns a dendrite reader for the provided file.

  If provided, the options map supports the following keys:
  :custom-types  - a map of of custom-type symbol to custom-type specification. Default: nil. See README for
                   full explanation."
  (^dendrite.java.Reader [file] (reader nil file))
  (^dendrite.java.Reader [opts file]
    (Reader/create (Options/getReaderOptions opts) (io/as-file file))))

(defn writer
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
  :custom-types            - a map of of custom-type symbol to custom-type specification. See README for
                             full explanation."
  (^dendrite.java.Writer [schema file] (writer nil schema file))
  (^dendrite.java.Writer [opts schema file]
    (Writer/create (Options/getWriterOptions opts) schema (io/as-file file))))

(extend-type View
  clojure.core.protocols/CollReduce
  (coll-reduce [this f]
    (reduce f (seq this)))
  (coll-reduce [this f init]
    (reduce f init (seq this)))
  clojure.core.reducers/CollFold
  (coll-fold [this n combinef reducef]
    (.fold this n combinef reducef)))

(defn read
  "Returns a view of all the records in the reader. This view is Seqable (lazy), reducible, and foldable (per
  clojure.core.reducers, in which case the folding is done as part of record assembly).

  If provided, the options map supports the following keys:
  :missing-fields-as-nil? - should be true (default) or false. If true, then fields that are specified in the
                            query but are not present in this reader's schema will be read as nil values. If
                            false, querying for fields not present in the schema will throw an exception.
  :query                  - the query. Default: '_. See README for full explanation.
  :entrypoint             - a sequence of keys to begin the query within a subset of the schema. Cannot
                            contain any keys to repeated fields. See README for full explanation.
  :readers                - a map of query tag symbol to tag function. Default: nil. See README for full
                            explanation."
  ([^Reader reader] (read nil reader))
  ([opts ^Reader reader] (.read reader (Options/getReadOptions opts))))

(defn pmap
  "Returns a view of all the records in the reader with the provided function applied to them. This is a
  convenience function that is roughly equivalent to (read {:query (tag 'foo '_) :readers {'foo f}} reader).
  See read for full details on available options."
  ([f ^Reader reader] (pmap nil f reader))
  ([opts f ^Reader reader] (.pmap reader (Options/getReadOptions opts) f)))

(defn stats
  "Returns a map containing all the stats associated with this reader. The tree top-level keys
  are :global, :record-groups, and :columns, that, respectively, contain stats summed over the entire file,
  summed across all column-chunks in the same record-groups, and summed across all column-chunks belonging
  to the same column."
  [^Reader reader] (.stats reader))

(defn schema
  "Returns this reader's schema."
  [^Reader reader] (.schema reader))

(defn set-metadata!
  "Sets the user-defined metadata for this writer."
  [^Writer writer metadata]
  (.setMetadata writer (-> metadata pr-str Types/toByteArray ByteBuffer/wrap)))

(defn metadata
  "Returns the user-defined metadata for this reader."
  [^Reader reader]
  (-> (.getMetadata reader) Types/toByteArray Types/toString edn/read-string))
