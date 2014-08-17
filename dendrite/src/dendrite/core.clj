(ns dendrite.core
  (:require [clojure.core.async :as async :refer [<! >! <!! >!!]]
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
           [java.nio.channels FileChannel])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(def ^:private magic-str "den1")
(def ^:private magic-bytes (into-array Byte/TYPE magic-str))
(def ^:private magic-bytes-length (count magic-bytes))
(def ^:private int-length 4)

(defn- valid-magic-bytes? [^ByteBuffer bb]
  (= magic-str (utils/byte-buffer->str bb)))

(def default-write-options
  {:target-record-group-length (* 256 1024 1024)  ; 256 MB
   :target-data-page-length (* 8 1024)            ; 8KB
   :optimize-columns? :default
   :compression-thresholds {:lz4 0.9 :deflate 0.5}
   :invalid-input-handler nil
   :custom-types nil})

(def default-reader-options
  {:custom-types nil})

(def default-read-options
  {:query '_
   :missing-fields-as-nil? true
   :readers nil
   :pmap-fn nil})

(defn- parse-custom-types [custom-types]
  (reduce-kv (fn [m k v] (assoc m (keyword k) (update-in v [:base-type] keyword))) {} custom-types))

(defprotocol IWriter
  (write! [_ records])
  (set-metadata! [_ metadata]))

(defprotocol IReader
  (read-with-opts [_ opts])
  (stats [_])
  (metadata [_])
  (schema [_]))

(defprotocol IBackendWriter
  (flush-record-group! [_ record-group-writer])
  (finish! [_ metadata])
  (close-writer! [_]))

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

(defn- write-loop
  [striped-record-ch ^BufferedByteArrayWriter record-group-writer backend-writer target-record-group-length
   optimize? compression-threshold-map]
  (try
    (loop [next-num-records-for-length-check 10
           record-groups-metadata []
           optimized? (not optimize?)
           rg-writer record-group-writer]
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
                       optimized-rg-writer))
              (let [metadata (complete-record-group! backend-writer rg-writer)
                    next-threshold-check (long (/ (:num-records metadata) 2))]
                (recur next-threshold-check (conj record-groups-metadata metadata) optimized? rg-writer)))
            (recur (estimation/next-threshold-check (record-group/num-records rg-writer)
                                                    estimated-length
                                                    target-record-group-length)
                   record-groups-metadata
                   optimized?
                   rg-writer)))
        (if-let [striped-record (<!! striped-record-ch)]
          (do (record-group/write! rg-writer striped-record)
              (recur next-num-records-for-length-check record-groups-metadata optimized? rg-writer))
          (let [final-rg-writer (if (and optimize? (not optimized?))
                                  (record-group/optimize! rg-writer compression-threshold-map)
                                  rg-writer)
                final-rg-metadata (if (pos? (record-group/num-records final-rg-writer))
                                    (->> (complete-record-group! backend-writer final-rg-writer)
                                         (conj record-groups-metadata))
                                    record-groups-metadata)]
            (record-group/await-io-completion final-rg-writer)
            {:record-groups-metadata final-rg-metadata
             :column-specs (record-group/column-specs final-rg-writer)}))))
    (catch Exception e
      (async/close! striped-record-ch)
      {:error e})))

(defn- <!!-coll [ch]
  (lazy-seq (let [v (<!! ch)]
              (when-not (nil? v)
                (cons v (<!!-coll ch))))))

(defn- >!!-coll [ch coll]
  (loop [[x & xs] coll]
    (if (>!! ch x)
      (if (seq xs)
        (recur xs)
        :end)
      :error)))

(defrecord Writer [metadata-atom stripe-fn striped-record-ch write-thread backend-writer]
  IWriter
  (write! [this records]
    (if (= :error (->> records
                       (utils/chunked-pmap stripe-fn)
                       (remove nil?)
                       (>!!-coll striped-record-ch)))
      (throw (:error (<!! write-thread)))
      this))
  (set-metadata! [_ metadata]
    (swap! metadata-atom assoc :custom metadata))
  Closeable
  (close [_]
    (async/close! striped-record-ch)
    (let [{:keys [record-groups-metadata column-specs error]} (<!! write-thread)]
      (if error
        (try (close-writer! backend-writer)
             (finally (throw error)))
        (try (let [metadata (-> @metadata-atom
                                (assoc :record-groups-metadata record-groups-metadata)
                                (update-in [:schema] schema/with-optimal-column-specs column-specs))]
               (finish! backend-writer metadata))
             (catch Exception e
                 (throw e))
             (finally
               (close-writer! backend-writer)))))))

(defn- writer [backend-writer schema options]
  (let [{:keys [target-record-group-length target-data-page-length optimize-columns? custom-types
                compression-thresholds invalid-input-handler]} (merge default-write-options options)]
    (let [type-store (encoding/type-store (parse-custom-types custom-types))
          parsed-schema (schema/parse schema type-store)
          striped-record-ch (async/chan 100)
          record-group-writer (record-group/writer target-data-page-length
                                                   type-store
                                                   (schema/column-specs parsed-schema))
          optimize? (case optimize-columns?
                      :all true
                      :default (all-default-column-specs? parsed-schema)
                      :none false)]
      (map->Writer
       {:metadata-atom (atom (metadata/map->Metadata {:schema parsed-schema}))
        :stripe-fn (striping/stripe-fn parsed-schema type-store invalid-input-handler)
        :striped-record-ch striped-record-ch
        :write-thread (async/thread (write-loop striped-record-ch
                                                record-group-writer
                                                backend-writer
                                                target-record-group-length
                                                optimize?
                                                compression-thresholds))
        :backend-writer backend-writer}))))

(defn byte-buffer-writer ^java.io.Closeable [schema & {:as options}]
  (writer (byte-array-backend-writer) schema options))

(defn byte-buffer! ^java.nio.ByteBuffer [^Closeable writer]
  (if-let [^ByteArrayWriter byte-array-writer (get-in writer [:backend-writer :byte-array-writer])]
    (do (.close writer)
        (ByteBuffer/wrap (.buffer byte-array-writer) 0 (.position byte-array-writer)))
    (throw (UnsupportedOperationException. "byte-buffer! is only supported on byte-buffer writers."))))

(defn file-writer ^java.io.Closeable [filename schema & {:as options}]
  (writer (file-channel-backend-writer filename) schema options))

(defn- record-group-offsets [record-groups-metadata offset]
  (->> record-groups-metadata (map :length) butlast (reductions + offset)))

(defn- byte-buffer->metadata [^ByteBuffer byte-buffer]
  (let [length (.limit byte-buffer)
        last-magic-bytes-pos (- length magic-bytes-length)
        metadata-length-pos (- last-magic-bytes-pos int-length)]
    (if-not
        (and (valid-magic-bytes? (utils/sub-byte-buffer byte-buffer 0 magic-bytes-length))
             (valid-magic-bytes? (utils/sub-byte-buffer byte-buffer last-magic-bytes-pos magic-bytes-length)))
      (throw (IllegalArgumentException.
              (if (.hasArray byte-buffer)
                "Provided byte buffer does not contain a valid dendrite serialization."
                "File is not a valid dendrite file.")))
      (let [metadata-length (-> byte-buffer
                                (utils/sub-byte-buffer metadata-length-pos int-length)
                                utils/byte-buffer->int)]
        (-> byte-buffer
            (utils/sub-byte-buffer (- metadata-length-pos metadata-length) metadata-length)
            metadata/read)))))

(defprotocol IBackendReader
  (close-reader! [_]))

(defrecord ByteBufferBackendReader [^ByteBuffer byte-buffer]
  IBackendReader
  (close-reader! [_]))

(defrecord FileChannelBackendReader [^FileChannel file-channel ^ByteBuffer byte-buffer]
  IBackendReader
  (close-reader! [_]
    (.close file-channel)))

(defn- record-group-readers [backend-reader record-groups-metadata type-store queried-schema]
  (utils/pmap-1 #(record-group/byte-buffer-reader (:byte-buffer backend-reader) %1 %2
                                                  type-store queried-schema)
                (record-group-offsets record-groups-metadata magic-bytes-length)
                record-groups-metadata))

(defrecord Reader [backend-reader metadata type-store]
  IReader
  (read-with-opts [_ opts]
    (let [{:keys [query missing-fields-as-nil? readers pmap-fn]} (merge default-read-options opts)
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
  (stats [_]
    (let [full-query (schema/apply-query (:schema metadata) '_ type-store true nil)
          all-stats (->> (record-group-readers backend-reader
                                               (:record-groups-metadata metadata)
                                               type-store
                                               full-query)
                         (map record-group/stats))
          record-groups-stats (map :record-group all-stats)
          columns-stats (->> (map :column-chunks all-stats)
                             (apply map vector)
                             (map stats/column-chunks->column-stats))]
      {:record-groups record-groups-stats
       :columns columns-stats
       :global (stats/record-groups->global-stats (.limit ^ByteBuffer (:byte-buffer backend-reader))
                                                  record-groups-stats)}))
  (metadata [_]
    (:custom metadata))
  (schema [_]
    (-> metadata :schema schema/unparse))
  Closeable
  (close [_]
    (close-reader! backend-reader)))

(defn- reader [backend-reader options]
  (let [{:keys [custom-types]} (merge default-reader-options options)
        metadata (-> backend-reader :byte-buffer byte-buffer->metadata)]
    (map->Reader
     {:backend-reader backend-reader
      :metadata metadata
      :type-store (encoding/type-store (parse-custom-types custom-types))})))

(defn byte-buffer-reader ^java.io.Closeable [^ByteBuffer byte-buffer & {:as options}]
  (reader (->ByteBufferBackendReader byte-buffer) options))

(defn file-reader ^java.io.Closeable [f & {:as options}]
  (let [file-channel (utils/file-channel f :read)
        mapped-byte-buffer (utils/map-file-channel file-channel)]
    (reader (->FileChannelBackendReader file-channel mapped-byte-buffer) options)))

(defn read [reader & {:as opts}]
  (read-with-opts reader opts))

(defn pmap-records [f reader & {:as opts}]
  (read-with-opts reader (assoc opts :pmap-fn f)))
