;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
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
            [dendrite.metadata :as metadata]
            [dendrite.record-group :as record-group]
            [dendrite.schema :as schema]
            [dendrite.stats :as stats]
            [dendrite.utils :as utils])
  (:import [dendrite.java Bytes Estimator IOutputBuffer MemoryOutputStream StripedRecordBundleSeq
            StripedRecordBundle]
           [dendrite.record_group RecordGroupWriter]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.util.concurrent Future LinkedBlockingQueue])
  (:refer-clojure :exclude [read pmap]))

(set! *warn-on-reflection* true)

(def ^:private magic-str "den1")
(def ^{:private true :tag 'bytes} magic-bytes (into-array Byte/TYPE magic-str))
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
   :entrypoint nil
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
      (if-not (fn? v)
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
    (if-not (fn? v)
      (throw (IllegalArgumentException. (format ":reader value for tag '%s' should be a function." k)))
      v)))

(defn- parse-read-option [k v]
  (case k
    :query v
    :entrypoint
    (when v
      (if-not (sequential? v)
        (throw (IllegalArgumentException. (format ":entrypoint expects a sequence but got '%s'" v)))
        v))
    :missing-fields-as-nil?
    (if-not (utils/boolean? v)
      (throw (IllegalArgumentException. (format ":missing-fields-as-nil? expects a boolean but got '%s'" v)))
      v)
    :readers
    (reduce-kv (fn [m t f] (assoc m t (parse-tag-reader t f))) {} v)
    :pmap-fn
    (when v
      (if-not (fn? v)
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

(definterface IBackendWriter
  (flushRecordGroup [^dendrite.record_group.RecordGroupWriter record-group-writer])
  (finish [metadata])
  (close []))

(def ^:private queue-poison ::poison)

(defn- blocking-queue-seq [^LinkedBlockingQueue queue]
  (lazy-seq (let [v (.take queue)
                  v (when-not (= v ::nil-record) v)]
              (when-not (= queue-poison v)
                (cons v (blocking-queue-seq queue))))))

(defn- close-queue! [^LinkedBlockingQueue queue]
  (.put queue queue-poison))

(defn- done? [fut]
  (.isDone ^Future fut))

(deftype MemoryOutputStreamBackendWriter [^MemoryOutputStream memory-output-stream]
  IBackendWriter
  (flushRecordGroup [_ record-group-writer]
    (.writeTo record-group-writer memory-output-stream))
  (finish [_ metadata]
    (let [mbb (metadata/write metadata)]
      (.write memory-output-stream (.array mbb) (.position mbb) (.limit mbb) )
      (Bytes/writeFixedInt memory-output-stream (.limit mbb))
      (.write memory-output-stream magic-bytes)))
  (close [_]))

(defn- memory-output-stream-backend-writer []
  (MemoryOutputStreamBackendWriter. (doto (MemoryOutputStream.) (.write magic-bytes))))

(deftype FileChannelBackendWriter [^FileChannel file-channel]
  IBackendWriter
  (flushRecordGroup [_ record-group-writer]
    (.flushToFileChannel record-group-writer file-channel))
  (finish [_ metadata]
    (let [metadata-byte-buffer (metadata/write metadata)]
      (.write file-channel metadata-byte-buffer)
      (.write file-channel (utils/int->byte-buffer (.limit metadata-byte-buffer)))
      (.write file-channel (ByteBuffer/wrap magic-bytes))))
  (close [_]
    (.close file-channel)))

(defn- file-channel-backend-writer [filename]
  (FileChannelBackendWriter. (doto (utils/file-channel filename :write)
                               (.write (ByteBuffer/wrap magic-bytes)))))

(defn- is-default-column-spec? [column-spec]
  (and (= (:encoding column-spec) :plain)
       (= (:compression column-spec) :none)))

(defn- all-default-column-specs? [schema]
  (every? is-default-column-spec? (schema/column-specs schema)))

(defn- complete-record-group! [^IBackendWriter backend-writer ^RecordGroupWriter record-group-writer]
  (.finish record-group-writer)
  (let [metadata (.metadata record-group-writer)]
    (.flushRecordGroup backend-writer record-group-writer)
    (.reset record-group-writer)
    metadata))

(defn- write-striped-records
  [^RecordGroupWriter record-group-writer backend-writer target-record-group-length optimize-count
   compression-threshold-map ^LinkedBlockingQueue striped-record-bundles-queue]
  (with-local-vars [rgw record-group-writer
                    ocnt optimize-count
                    record-groups-metadata []
                    next-num-records-for-length-check 10]
    (loop []
      (if (>= (.numRecords ^RecordGroupWriter @rgw) @next-num-records-for-length-check)
        (let [estimated-length (.estimatedLength ^RecordGroupWriter @rgw)]
          (if (>= estimated-length target-record-group-length)
            (if (pos? @ocnt)
              (let [^RecordGroupWriter orgw (record-group/optimize! @rgw compression-threshold-map)]
                (var-set rgw orgw)
                (var-set ocnt (dec @ocnt))
                (recur))
              (let [metadata (complete-record-group! backend-writer @rgw)]
                (var-set record-groups-metadata (conj @record-groups-metadata metadata))
                (var-set next-num-records-for-length-check (quot (:num-records metadata) 2))
                (recur)))
            (do (var-set next-num-records-for-length-check
                         (Estimator/nextCheckThreshold (.numRecords ^RecordGroupWriter @rgw)
                                                       estimated-length
                                                       target-record-group-length))
                (recur))))
        (let [obj (.take striped-record-bundles-queue)]
          (if-not (= obj queue-poison)
            (do (.write ^RecordGroupWriter @rgw ^StripedRecordBundle obj)
                (recur))
            (do (when (pos? @ocnt)
                  (var-set rgw (record-group/optimize! @rgw compression-threshold-map)))
                (when (pos? (.numRecords ^RecordGroupWriter @rgw))
                  (var-set record-groups-metadata
                           (conj @record-groups-metadata (complete-record-group! backend-writer @rgw))))
                (.awaitIOCompletion ^RecordGroupWriter @rgw)
                {:record-groups-metadata @record-groups-metadata
                 :column-specs (.columnSpecs ^RecordGroupWriter @rgw)})))))))

(defrecord Writer
    [metadata-atom record-bundle-atom ^LinkedBlockingQueue record-bundle-queue striping-thread
     writing-thread ^IBackendWriter backend-writer custom-types closed]
  IWriter
  (write! [this record]
    (if (or (done? striping-thread) (done? writing-thread))
      (cond
        (instance? Exception @striping-thread) (throw @striping-thread)
        (instance? Exception @writing-thread) (throw @writing-thread)
        :else (throw (IllegalStateException. "Cannot write to closed writer.")))
      (do (when (> (count @record-bundle-atom) 255)
            (do (.put record-bundle-queue @record-bundle-atom)
                (reset! record-bundle-atom [])))
          (swap! record-bundle-atom conj record)))
    this)
  Closeable
  (close [_]
    (when-not @closed
      (.put record-bundle-queue @record-bundle-atom)
      (close-queue! record-bundle-queue)
      (try @striping-thread
           (let [{:keys [record-groups-metadata column-specs writing-error]} @writing-thread
                 metadata (-> @metadata-atom
                              (assoc :record-groups-metadata record-groups-metadata
                                     :custom-types custom-types)
                              (update-in [:schema] schema/with-optimal-column-specs column-specs))]
             (.finish backend-writer metadata))
           (catch Exception e
             (throw e))
           (finally (.close backend-writer)
                    (reset! closed true))))))

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
          record-bundle-queue (LinkedBlockingQueue. 100)
          writing-queue (LinkedBlockingQueue. 100)
          column-specs (schema/column-specs parsed-schema)
          num-columns (count column-specs)
          record-group-writer (record-group/writer data-page-length
                                                   type-store
                                                   column-specs)
          optimize? (or optimize-columns? (and (nil? optimize-columns?)
                                               (all-default-column-specs? parsed-schema)))
          stripe (striping/stripe-fn parsed-schema type-store invalid-input-handler)
          stripe-bundle #(StripedRecordBundle/stripe % stripe num-columns)]
      (map->Writer
       {:metadata-atom (atom (metadata/map->Metadata {:schema parsed-schema}))
        :record-bundle-atom (atom [])
        :writing-queue writing-queue
        :record-bundle-queue record-bundle-queue
        :striping-thread (future (try (doseq [striped-record-bundle (->> record-bundle-queue
                                                                         blocking-queue-seq
                                                                         (clojure.core/pmap stripe-bundle))]
                                        (.put writing-queue striped-record-bundle))
                                      (catch Exception e
                                        (throw e))
                                      (finally
                                        (close-queue! writing-queue))))
        :writing-thread (future (write-striped-records record-group-writer
                                                       backend-writer
                                                       record-group-length
                                                       (if optimize? 1 0)
                                                       compression-thresholds
                                                       writing-queue))
        :backend-writer backend-writer
        :custom-types parsed-custom-types
        :closed (atom false)}))))

(defn byte-buffer-writer
  "Returns a dendrite writer that outputs to a byte-buffer. schema should be a valid dendrite schema. The
  optional options map support the same options as for file-writer."
  (^java.io.Closeable [schema] (byte-buffer-writer nil schema))
  (^java.io.Closeable [options schema]
    (writer (memory-output-stream-backend-writer) schema options)))

(defn byte-buffer!
  "Closes a byte-buffer writer and returns a java.nio.ByteBuffer containing the full dendrite serialization."
  ^java.nio.ByteBuffer [^Closeable writer]
  (let [backend-writer (:backend-writer writer)
        ^MemoryOutputStream memory-output-stream
          (if (instance? MemoryOutputStreamBackendWriter backend-writer)
            (.memory-output-stream ^MemoryOutputStreamBackendWriter backend-writer)
            (throw (UnsupportedOperationException. "byte-buffer! is only supported on byte-buffer writers.")))]
    (.close writer)
    (.byteBuffer memory-output-stream)))

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
  (map #(record-group/byte-buffer-reader %1 %2 type-store queried-schema)
       (record-group-byte-buffers backend-reader record-groups-metadata)
       record-groups-metadata))

(defrecord Reader [backend-reader metadata type-store]
  IReader
  (read* [_ opts]
    (let [{:keys [query missing-fields-as-nil? readers pmap-fn entrypoint]} (parse-read-options opts)
          queried-schema (cond-> (schema/apply-query (schema/sub-schema-in (:schema metadata) entrypoint)
                                                     query
                                                     type-store
                                                     missing-fields-as-nil?
                                                     readers)
                           pmap-fn (update-in [:reader-fn] #(if % (comp pmap-fn %) pmap-fn)))
          assemble (assembly/assemble-fn queried-schema)]
      (if (seq (schema/column-specs queried-schema))
        (->> (record-group-readers backend-reader (:record-groups-metadata metadata) type-store queried-schema)
             (map record-group/read)
             (StripedRecordBundleSeq/create 256)
             (clojure.core/pmap (fn [^StripedRecordBundle srb] (.assemble srb assemble)))
             utils/flatten-1)
        (->> (:record-groups-metadata metadata)
             (map #(repeat (:num-records %) (assemble nil)))
             utils/flatten-1))))
  (foldable* [_ opts]
    (let [{:keys [query missing-fields-as-nil? readers entrypoint]} (parse-read-options opts)
          queried-schema (schema/apply-query (schema/sub-schema-in (:schema metadata) entrypoint)
                                             query
                                             type-store
                                             missing-fields-as-nil?
                                             readers)
          read-record-groups #(map record-group/read
                                   (record-group-readers backend-reader
                                                         (:record-groups-metadata metadata)
                                                         type-store
                                                         queried-schema))
          assemble (assembly/assemble-fn queried-schema)
          read-records #(->> (read-record-groups)
                             (StripedRecordBundleSeq/create 256)
                             (clojure.core/pmap (fn [^StripedRecordBundle srb] (.assemble srb assemble)))
                             utils/flatten-1)]
      (if (seq (schema/column-specs queried-schema))
        (reify
          clojure.core.protocols/CollReduce
          (coll-reduce [_ f]
            (reduce f (read-records)))
          (coll-reduce [_ f init]
            (reduce f init (read-records)))
          CollFold
          (coll-fold [_ n combinef reducef]
            (let [init (combinef)]
              (->> (read-record-groups)
                   (StripedRecordBundleSeq/create n)
                   (clojure.core/pmap (fn [^StripedRecordBundle srb] (.reduce srb reducef assemble init)))
                   (reduce combinef init)))))
        (let [nil-record (assemble nil)
              read-nil-records #(->> (:record-groups-metadata metadata)
                                     (map (fn [{:keys [num-records]}] (repeat num-records nil-record)))
                                     utils/flatten-1)]
          (reify
            clojure.core.protocols/CollReduce
            (coll-reduce [_ f]
              (reduce f (read-nil-records)))
            (coll-reduce [_ f init]
              (reduce f init (read-nil-records)))
            CollFold
            (coll-fold [_ n combinef reducef]
              (let [init (combinef)]
                (->> (:record-groups-metadata metadata)
                     (map #(let [num-records (:num-records %)]
                             (concat (repeat (quot num-records n) (reduce reducef init (repeat n nil-record)))
                                     (reduce reducef init (repeat (mod num-records n) nil-record)))))
                     utils/flatten-1
                     (reduce combinef init)))))))))
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
  :entrypoint             - a sequence of keys to begin the query within a subset of the schema. Cannot
                            contain any keys to repeated fields. See README for full explanation.
  :readers                - a map of query tag symbol to tag function. Default: nil. See README for full
                            explanation."
  ([reader] (read nil reader))
  ([options reader] (read* reader options)))

(defn pmap
  "Like read but applies the function f to all queried records. Convenience function that leverages the query
  tagging functionality. Since it applies f as part of the parallelized record assembly, it is likely more
  efficient to calling (map f ..) or (pmap f ..) outside of dendrite. If provided, the options map supports
  the same options as read."
  ([f reader] (pmap nil f reader))
  ([options f reader] (read* reader (assoc options :pmap-fn f))))

(defn foldable
  "Like read but returns a foldable collection (as per core.reducers) instead of a lazy-seq. The fold
  operation on this collection applies the reduce function in each of the parallel record assembly threads for
  increased performance. If provided, the options map supports the same options as read."
  ([reader] (foldable nil reader))
  ([options reader] (foldable* reader options)))
