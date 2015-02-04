;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.impl
  (:require [clojure.core.reducers :refer [CollFold]]
            [dendrite.assembly :as assembly]
            [dendrite.striping :as striping]
            [dendrite.encoding :as encoding]
            [dendrite.estimation :as estimation]
            [dendrite.metadata :as metadata]
            [dendrite.record-group :as record-group]
            [dendrite.schema :as schema]
            [dendrite.stats :as stats]
            [dendrite.utils :as utils])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.util.concurrent Future LinkedBlockingQueue])
  (:refer-clojure :exclude [read pmap]))

(set! *warn-on-reflection* true)

(def ^:private magic-str "den1")
(def ^:private magic-bytes (into-array Byte/TYPE magic-str))
(def ^:private magic-bytes-length (count magic-bytes))
(def ^:private int-length 4)

(defn- valid-magic-bytes? [^ByteBuffer bb]
  (= magic-str (utils/byte-buffer->str bb)))

(def default-writer-options
  {:record-group-length (* 128 1024 1024) ; 128 MB
   :data-page-length (* 256 1024)         ; 256 KB
   :optimize-columns? nil
   :compression-thresholds {:lz4 1.2 :deflate 1.5}
   :invalid-input-handler nil
   :custom-types nil})

(def default-reader-options
  {:custom-types nil})

(def default-read-options
  {:query '_
   :missing-fields-as-nil? true
   :readers nil
   :pmap-fn nil})

(defn- keywordize-custom-types [custom-types]
  (reduce-kv (fn [m k v] (assoc m (keyword k) (update-in v [:base-type] keyword))) {} custom-types))

(defn- parse-writer-option [k v]
  (case k
    :record-group-length
    (if-not (and (number? v) (pos? (long v)))
      (throw (IllegalArgumentException.
              (format ":record-group-length expects a positive integer but got '%s'." v)))
      ;; java.nio.FileChannel's implementation cannot map file areas larger than 2GB. Since dendrite maps
      ;; entire record-groups, this limitation also applies here.
      (if (> (long v) (long Integer/MAX_VALUE))
        (throw (IllegalArgumentException. ":record-group-length can be no greater than Integer/MAX_VALUE"))
        (long v)))
    :data-page-length
    (if-not (and (number? v) (pos? v))
      (throw (IllegalArgumentException.
              (format ":data-page-length expects a positive integer but got '%s'." v)))
      (long v))
    :optimize-columns?
    (if-not ((some-fn utils/boolean? nil?) v)
      (throw (IllegalArgumentException.
              (format ":optimize-columns? expects either true, false, or nil but got '%s'." v)))
      v)
    :compression-thresholds
    (if-not (map? v)
      (throw (IllegalArgumentException. ":compression-thresholds expects a map."))
      (reduce-kv (fn [m c t]
                   (if-not (and (#{:lz4 :deflate} c) (number? t) (pos? (double t)))
                     (throw (IllegalArgumentException.
                             (str ":compression-thresholds expects compression-type/compression-threshold "
                                  "map entries, where the compression-type is either :lz4 or :deflate, and "
                                  "the compression threshold is a double value strictly greater than 0.")))
                     (assoc m c (double t))))
                 {}
                 v))
    :invalid-input-handler
    (when v
      (if-not (utils/callable? v)
        (throw (IllegalArgumentException. ":invalid-input-handler expects a function."))
        v))
    :custom-types
    (when v (-> v keywordize-custom-types encoding/parse-custom-derived-types))
    (throw (IllegalArgumentException. (format "%s is not a supported writer option." k)))))

(defn- parse-reader-option [k v]
  (case k
    :custom-types (when v (keywordize-custom-types v))
    (throw (IllegalArgumentException. (format "%s is not a supported reader option." k)))))

(defn- parse-tag-reader [k v]
  (if-not (symbol? k)
    (throw (IllegalArgumentException. (format ":reader key should be a symbol but got '%s'." k)))
    (if-not (utils/callable? v)
      (throw (IllegalArgumentException. (format ":reader value for tag '%s' should be a function." k)))
      v)))

(defn- parse-read-option [k v]
  (case k
    :query v
    :missing-fields-as-nil?
    (if-not (utils/boolean? v)
      (throw (IllegalArgumentException. (format ":missing-fields-as-nil? expects a boolean but got '%s'" v)))
      v)
    :readers
    (reduce-kv (fn [m t f] (assoc m t (parse-tag-reader t f))) {} v)
    :pmap-fn
    (when v
      (if-not (utils/callable? v)
        (throw (IllegalArgumentException. ":pmap-fn expects a function."))
        v))
    (throw (IllegalArgumentException. (format "%s is not a supported read option." k)))))

(defn- parse-options [default-opts parse-opt-fn provided-opts]
  (->> provided-opts
       (reduce-kv (fn [m k v] (assoc m k (parse-opt-fn k v))) {})
       (merge default-opts)))

(defn- parse-writer-options [opts]
  (parse-options default-writer-options parse-writer-option opts))

(defn- parse-reader-options [opts]
  (parse-options default-reader-options parse-reader-option opts))

(defn- parse-read-options [opts]
  (parse-options default-read-options parse-read-option opts))

(defprotocol IWriter
  (write! [_ record]
    "Write a single record to this writer. Returns the writer."))

(defprotocol IReader
  (read* [_ opts])
  (foldable* [_ opts])
  (stats [_]
    "Returns a map containing all the stats associated with this reader. The tree top-level keys
    are :global, :record-groups, and :columns, that, respectively, contain stats summed over the entire file,
    summed across all column-chunks in the same record-groups, and summed across all column-chunks belonging
    to the same column.")
  (metadata [_]
    "Returns the user-defined metadata for this reader.")
  (schema [_]
    "Returns this reader's schema."))

(defprotocol IBackendWriter
  (flush-record-group! [_ record-group-writer])
  (finish! [_ metadata])
  (close-writer! [_]))

(def ^:private queue-poison ::poison)

(defn- blocking-queue-seq [^LinkedBlockingQueue queue]
  (lazy-seq (let [v (.take queue)]
              (when-not (= queue-poison v)
                (cons v (blocking-queue-seq queue))))))

(defn- close-queue! [^LinkedBlockingQueue queue]
  (.put queue queue-poison))

(defn- done? [fut]
  (.isDone ^Future fut))

(defrecord ByteArrayBackendWriter [^ByteArrayWriter byte-array-writer]
  IBackendWriter
  (flush-record-group! [_ record-group-writer]
    (.flush ^BufferedByteArrayWriter record-group-writer byte-array-writer))
  (finish! [_ metadata]
    (let [metadata-byte-buffer (metadata/write metadata)]
      (.write byte-array-writer metadata-byte-buffer)
      (.writeFixedInt byte-array-writer (.limit metadata-byte-buffer))
      (.writeByteArray byte-array-writer magic-bytes)))
  (close-writer! [_]))

(defn- byte-array-backend-writer []
  (->ByteArrayBackendWriter (doto (ByteArrayWriter.) (.writeByteArray magic-bytes))))

(defrecord FileChannelBackendWriter [^FileChannel file-channel]
  IBackendWriter
  (flush-record-group! [_ record-group-writer]
    (record-group/flush-to-file-channel! record-group-writer file-channel))
  (finish! [_ metadata]
    (let [metadata-byte-buffer (metadata/write metadata)]
      (.write file-channel metadata-byte-buffer)
      (.write file-channel (utils/int->byte-buffer (.limit metadata-byte-buffer)))
      (.write file-channel (ByteBuffer/wrap magic-bytes))))
  (close-writer! [_]
    (.close file-channel)))

(defn- file-channel-backend-writer [filename]
  (->FileChannelBackendWriter (doto (utils/file-channel filename :write)
                                (.write (ByteBuffer/wrap magic-bytes)))))

(defn- is-default-column-spec? [column-spec]
  (and (= (:encoding column-spec) :plain)
       (= (:compression column-spec) :none)))

(defn- all-default-column-specs? [schema]
  (every? is-default-column-spec? (schema/column-specs schema)))

(defn- complete-record-group! [backend-writer ^BufferedByteArrayWriter record-group-writer]
  (.finish ^BufferedByteArrayWriter record-group-writer)
  (let [metadata (record-group/metadata record-group-writer)]
    (flush-record-group! backend-writer record-group-writer)
    (.reset record-group-writer)
    metadata))

(defn- write-striped-records
  [^BufferedByteArrayWriter record-group-writer backend-writer target-record-group-length
   optimize? compression-threshold-map striped-records]
  (loop [next-num-records-for-length-check 10
         record-groups-metadata []
         optimized? (not optimize?)
         rg-writer record-group-writer
         striped-recs striped-records]
    (if (>= (record-group/num-records rg-writer) next-num-records-for-length-check)
      (let [estimated-length (.estimatedLength rg-writer)]
        (if (>= estimated-length target-record-group-length)
          (if (and optimize? (not optimized?))
            (let [^BufferedByteArrayWriter optimized-rg-writer
                  (record-group/optimize! rg-writer compression-threshold-map)]
              (recur (estimation/next-threshold-check (record-group/num-records optimized-rg-writer)
                                                      (.estimatedLength optimized-rg-writer)
                                                      target-record-group-length)
                     record-groups-metadata
                     true
                     optimized-rg-writer
                     striped-recs))
            (let [metadata (complete-record-group! backend-writer rg-writer)
                  next-threshold-check (long (/ (:num-records metadata) 2))]
              (recur next-threshold-check
                     (conj record-groups-metadata metadata)
                     optimized?
                     rg-writer
                     striped-recs)))
          (recur (estimation/next-threshold-check (record-group/num-records rg-writer)
                                                  estimated-length
                                                  target-record-group-length)
                 record-groups-metadata
                 optimized?
                 rg-writer
                 striped-recs)))
      (if-let [striped-record (first striped-recs)]
        (do (record-group/write! rg-writer striped-record)
            (recur next-num-records-for-length-check
                   record-groups-metadata
                   optimized?
                   rg-writer
                   (rest striped-recs)))
        (let [final-rg-writer (if (and optimize? (not optimized?))
                                (record-group/optimize! rg-writer compression-threshold-map)
                                rg-writer)
              final-rg-metadata (if (pos? (record-group/num-records final-rg-writer))
                                  (->> (complete-record-group! backend-writer final-rg-writer)
                                       (conj record-groups-metadata))
                                  record-groups-metadata)]
          (record-group/await-io-completion final-rg-writer)
          {:record-groups-metadata final-rg-metadata
           :column-specs (record-group/column-specs final-rg-writer)})))))

(defrecord Writer
    [metadata-atom ^LinkedBlockingQueue writing-queue write-thread backend-writer custom-types closed]
  IWriter
  (write! [this record]
    (if (done? write-thread)
      (if (instance? Exception @write-thread)
        (throw @write-thread)
        (throw (IllegalStateException. "Cannot write to closed writer.")))
      (.put writing-queue (if (nil? record) {} record)))
    this)
  Closeable
  (close [_]
    (when-not @closed
      (close-queue! writing-queue)
      (let [{:keys [record-groups-metadata column-specs error]} @write-thread]
        (if error
          (try (close-writer! backend-writer)
               (finally (throw error)))
          (try (let [metadata (-> @metadata-atom
                                  (assoc :record-groups-metadata record-groups-metadata
                                         :custom-types custom-types)
                                  (update-in [:schema] schema/with-optimal-column-specs column-specs))]
                 (finish! backend-writer metadata))
               (catch Exception e
                 (throw e))
               (finally
                 (close-writer! backend-writer)))))
      (reset! closed true))))

(defn set-metadata!
  "Sets the user-defined metadata of the provided writer."
  [writer metadata]
  (swap! (:metadata-atom writer) assoc :custom metadata))

(defn swap-metadata!
  "Updates the user-defined metadata of the provided writer to (apply f current-metadata args)."
  [writer f & args]
  (swap! (:metadata-atom writer) update-in [:custom] #(apply f % args)))

(defn- writer [backend-writer schema options]
  (let [{:keys [record-group-length data-page-length optimize-columns? custom-types
                compression-thresholds invalid-input-handler]} (parse-writer-options options)]
    (let [parsed-custom-types (keywordize-custom-types custom-types)
          type-store (encoding/type-store parsed-custom-types)
          parsed-schema (schema/parse schema type-store)
          writing-queue (LinkedBlockingQueue. 100)
          record-group-writer (record-group/writer data-page-length
                                                   type-store
                                                   (schema/column-specs parsed-schema))
          optimize? (or optimize-columns? (all-default-column-specs? parsed-schema))
          stripe (striping/stripe-fn parsed-schema type-store invalid-input-handler)]
      (map->Writer
       {:metadata-atom (atom (metadata/map->Metadata {:schema parsed-schema}))
        :writing-queue writing-queue
        :write-thread (future (->> writing-queue
                                   blocking-queue-seq
                                   (utils/chunked-pmap stripe)
                                   (remove nil?)
                                   (write-striped-records record-group-writer
                                                          backend-writer
                                                          record-group-length
                                                          optimize?
                                                          compression-thresholds)))
        :backend-writer backend-writer
        :custom-types parsed-custom-types
        :closed (atom false)}))))

(defn byte-buffer-writer
  "Returns a dendrite writer that outputs to a byte-buffer. schema should be a valid dendrite schema. The
  optional options map support the same options as for file-writer."
  (^java.io.Closeable [schema] (byte-buffer-writer nil schema))
  (^java.io.Closeable [options schema]
     (writer (byte-array-backend-writer) schema options)))

(defn byte-buffer!
  "Closes a byte-buffer writer and returns a java.nio.ByteBuffer containing the full dendrite serialization."
  ^java.nio.ByteBuffer [^Closeable writer]
  (if-let [^ByteArrayWriter byte-array-writer (get-in writer [:backend-writer :byte-array-writer])]
    (do (.close writer)
        (ByteBuffer/wrap (.buffer byte-array-writer) 0 (.position byte-array-writer)))
    (throw (UnsupportedOperationException. "byte-buffer! is only supported on byte-buffer writers."))))

(defn file-writer
  "Returns a dendrite writer that outputs to a file. schema should be a valid dendrite schema and filename
  the path to the file to output to.

  If provided, the options map supports the following keys:
  :data-page-length        - the length in bytes of the data pages (default 262144)
  :record-group-length     - the length in bytes of each record group (default 134217728)
  :optimize-columns?       - either true, false or nil. If true, will try each encoding/compression pair and
                             select the most efficient one (subject to the :compression-thresholds).
                             If nil (default), will only optimize if all the columns are in the default
                             encoding/compression. If false, will never optimize the columns.
  :compression-thresholds  - a map of compression method (e.g., :lz4) to the minimum compression ratio
                             (e.g., 2) below which the overhead of compression is not not deemed worthwhile.
                             Default: {:lz4 1.2 :deflate 1.5}
  :invalid-input-handler   - a function with two arguments: record and exception. If an input record does
                             not conform to the schema, it will be passed to this function along with the
                             exception it triggered. By default, this option is nil and exceptions
                             triggered by invalid records are not caught.
  :custom-types            - a map of of custom-type symbol to custom-type specification. See README for
                             full explanation."
  (^java.io.Closeable [schema filename] (file-writer nil schema filename))
  (^java.io.Closeable [options schema filename]
     (writer (file-channel-backend-writer filename) schema options)))

(defn- record-group-lengths [record-groups-metadata]
  (map :length record-groups-metadata))

(defn- record-group-offsets [record-groups-metadata offset]
  (->> record-groups-metadata record-group-lengths butlast (reductions + offset)))

(defprotocol IBackendReader
  (record-group-byte-buffers [_ record-groups-metadata])
  (read-metadata [_])
  (length [_])
  (close-reader! [_]))

(defrecord ByteBufferBackendReader [^ByteBuffer byte-buffer]
  IBackendReader
  (length [_]
    (.limit byte-buffer))
  (read-metadata [_]
    (let [length (.limit byte-buffer)
          last-magic-bytes-pos (- length magic-bytes-length)
          metadata-length-pos (- last-magic-bytes-pos int-length)]
      (if-not
          (and (valid-magic-bytes? (utils/sub-byte-buffer byte-buffer 0 magic-bytes-length))
               (valid-magic-bytes? (utils/sub-byte-buffer byte-buffer last-magic-bytes-pos magic-bytes-length)))
        (throw (IllegalArgumentException.
                "Provided byte buffer does not contain a valid dendrite serialization."))
        (let [metadata-length (-> byte-buffer
                                  (utils/sub-byte-buffer metadata-length-pos int-length)
                                  utils/byte-buffer->int)]
          (-> byte-buffer
              (utils/sub-byte-buffer (- metadata-length-pos metadata-length) metadata-length)
              metadata/read)))))
  (record-group-byte-buffers [_ record-groups-metadata]
    (map (partial utils/sub-byte-buffer byte-buffer)
         (record-group-offsets record-groups-metadata magic-bytes-length)
         (record-group-lengths record-groups-metadata)))
  (close-reader! [_]))

(defrecord FileChannelBackendReader [^FileChannel file-channel]
  IBackendReader
  (length [_]
    (.size file-channel))
  (read-metadata [_]
    (let [length (.size file-channel)
          last-magic-bytes-pos (- length magic-bytes-length)
          metadata-length-pos (- last-magic-bytes-pos int-length)]
      (if-not
          (and (valid-magic-bytes? (utils/map-file-channel file-channel 0 magic-bytes-length))
               (valid-magic-bytes? (utils/map-file-channel file-channel
                                                           last-magic-bytes-pos
                                                           magic-bytes-length)))
        (throw (IllegalArgumentException. "File is not a valid dendrite file."))
        (let [metadata-length (-> file-channel
                                  (utils/map-file-channel metadata-length-pos int-length)
                                  utils/byte-buffer->int)]
          (-> file-channel
              (utils/map-file-channel (- metadata-length-pos metadata-length) metadata-length)
              metadata/read)))))
  (record-group-byte-buffers [_ record-groups-metadata]
    (map (partial utils/map-file-channel file-channel)
         (record-group-offsets record-groups-metadata magic-bytes-length)
         (record-group-lengths record-groups-metadata)))
  (close-reader! [_]
    (.close file-channel)))

(defn- record-group-readers [backend-reader record-groups-metadata type-store queried-schema]
  (utils/pmap-1 #(record-group/byte-buffer-reader %1 %2 type-store queried-schema)
                (record-group-byte-buffers backend-reader record-groups-metadata)
                record-groups-metadata))

(defrecord Reader [backend-reader metadata type-store]
  IReader
  (read* [_ opts]
    (let [{:keys [query missing-fields-as-nil? readers pmap-fn]} (parse-read-options opts)
          queried-schema (cond-> (schema/apply-query (:schema metadata)
                                                     query
                                                     type-store
                                                     missing-fields-as-nil?
                                                     readers)
                                 pmap-fn (update-in [:reader-fn] #(if % (comp pmap-fn %) pmap-fn)))
          assemble (assembly/assemble-fn queried-schema)]
      (->> (record-group-readers backend-reader (:record-groups-metadata metadata) type-store queried-schema)
           (map record-group/read)
           utils/flatten-1
           (utils/chunked-pmap assemble))))
  (foldable* [_ opts]
    (let [{:keys [query missing-fields-as-nil? readers]} (parse-read-options opts)
          queried-schema (schema/apply-query (:schema metadata)
                                             query
                                             type-store
                                             missing-fields-as-nil?
                                             readers)
          read-striped #(->> (record-group-readers backend-reader
                                                   (:record-groups-metadata metadata)
                                                   type-store
                                                   queried-schema)
                             (map record-group/read)
                             utils/flatten-1)
          assemble (assembly/assemble-fn queried-schema)]
      (reify
        clojure.core.protocols/CollReduce
        (coll-reduce [_ f]
          (reduce f (utils/chunked-pmap assemble (read-striped))))
        (coll-reduce [_ f init]
          (reduce f init (utils/chunked-pmap assemble (read-striped))))
        CollFold
        (coll-fold [_ n combinef reducef]
          (utils/chunked-fold n assemble reducef combinef (read-striped))))))
  (stats [_]
    (let [full-query (schema/apply-query (:schema metadata) '_ type-store true nil)
          all-stats (->> (record-group-readers backend-reader
                                               (:record-groups-metadata metadata)
                                               type-store
                                               full-query)
                         (map record-group/stats)
                         seq)
          record-groups-stats (some->> all-stats (map :record-group))
          columns-stats (some->> all-stats
                                 (map :column-chunks)
                                 (apply map vector)
                                 (map stats/column-chunks->column-stats))]
      {:record-groups record-groups-stats
       :columns columns-stats
       :global (stats/record-groups->global-stats (length backend-reader) record-groups-stats)}))
  (metadata [_]
    (:custom metadata))
  (schema [_]
    (-> metadata :schema schema/unparse))
  Closeable
  (close [_]
    (close-reader! backend-reader)))

(defn- reader [backend-reader options]
  (let [metadata (read-metadata backend-reader)
        {:keys [custom-types]} (parse-reader-options options)]
    (map->Reader
     {:backend-reader backend-reader
      :metadata metadata
      :type-store (->> custom-types
                       (merge (:custom-types metadata))
                       encoding/parse-custom-derived-types
                       encoding/type-store)})))

(defn byte-buffer-reader
  "Returns a dendrite reader for the provided java.nio.ByteBuffer.

  If provided, the options map supports the following keys:
  :custom-types  - a map of of custom-type symbol to custom-type specification. Default: nil. See README for
                   full explanation."
  (^java.io.Closeable [^ByteBuffer byte-buffer] (byte-buffer-reader nil byte-buffer))
  (^java.io.Closeable [options ^ByteBuffer byte-buffer]
     (reader (->ByteBufferBackendReader byte-buffer) options)))

(defn file-reader
  "Returns a dendrite reader for the provided filename.

  If provided, the options map supports the following keys:
  :custom-types  - a map of of custom-type symbol to custom-type specification. Default: nil. See README for
                   full explanation."
  (^java.io.Closeable [filename] (file-reader nil filename))
  (^java.io.Closeable [options filename]
    (reader (->FileChannelBackendReader (utils/file-channel filename :read)) options)))

(defn read
  "Returns a lazy-seq of all the records in the reader.

  If provided, the options map supports the following keys:
  :missing-fields-as-nil? - should be true (default) or false. If true, then fields that are specified in the
                            query but are not present in this reader's schema will be read as nil values. If
                            false, querying for fields not present in the schema will throw an exception.
  :query                  - the query. Default: '_. See README for full explanation.
  :readers                - a map of query tag symbol to tag function. Default: nil. See README for full
                            explanantion."
  ([reader] (read nil reader))
  ([options reader]
     (read* reader options)))

(defn pmap
  "Like read but applies the function f to all queried records. Convenience function that leverages the query
  tagging functionality. Since it applies f as part of the parallelized record assembly, it is likely more
  efficient to calling (map f ..) or (pmap f ..) outside of dendrite. If provided, the options map supports
  the same options as read."
  ([f reader] (pmap nil f reader))
  ([options f reader]
     (read* reader (assoc options :pmap-fn f))))

(defn foldable
  "Like read but returns a foldable collection (as per core.reducers) instead of a lazy-seq. The fold
  operation on this collection applies the reduce function in each of the parallel record assembly threads for
  increased performance. If provided, the options map supports the same options as read."
  ([reader] (foldable nil reader))
  ([options reader]
     (foldable* reader options)))
