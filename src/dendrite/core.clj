(ns dendrite.core
  (:require [clojure.core.async :as async :refer [<! >! <!! >!!]]
            [dendrite.assembly :as assembly]
            [dendrite.striping :as striping]
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

(def default-options
  {:target-record-group-length (* 256 1024 1024)  ; 256 MB
   :target-data-page-length 1024})        ; 1KB

(defprotocol IWriter
  (write! [_ records])
  (set-metadata! [_ metadata]))

(defprotocol IReader
  (read [_])
  (stats [_])
  (metadata [_])
  (schema [_]))

(defprotocol IBackendWriter
  (flush-record-group! [_ record-group-writer])
  (finish! [_ metadata]))

(defrecord ByteArrayBackendWriter [^ByteArrayWriter byte-array-writer]
  IBackendWriter
  (flush-record-group! [_ record-group-writer]
    (.writeTo ^BufferedByteArrayWriter record-group-writer byte-array-writer))
  (finish! [_ metadata]
    (let [metadata-byte-buffer (metadata/write metadata)]
      (.write byte-array-writer metadata-byte-buffer)
      (.writeFixedInt byte-array-writer (.limit metadata-byte-buffer))
      (.writeByteArray byte-array-writer magic-bytes))))

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
      (.write file-channel (ByteBuffer/wrap magic-bytes)))
    (.close file-channel)))

(defn- file-channel-backend-writer [filename]
  (->FileChannelBackendWriter (doto (utils/file-channel filename :write)
                                (.write (ByteBuffer/wrap magic-bytes)))))

(defn- complete-record-group! [backend-writer ^BufferedByteArrayWriter record-group-writer]
  (.finish ^BufferedByteArrayWriter record-group-writer)
  (let [metadata (record-group/metadata record-group-writer)]
    (flush-record-group! backend-writer record-group-writer)
    (.reset record-group-writer)
    metadata))

(defn- write-loop
  [striped-record-ch ^BufferedByteArrayWriter record-group-writer backend-writer target-record-group-length]
  (loop [next-num-records-for-length-check 10
         record-groups-metadata []]
    (if (>= (record-group/num-records record-group-writer) next-num-records-for-length-check)
      (let [estimated-length (.estimatedLength record-group-writer)]
        (if (>= estimated-length target-record-group-length)
          (let [metadata (complete-record-group! backend-writer record-group-writer)]
            (recur (long (/ (:num-records metadata) 2)) (conj record-groups-metadata metadata)))
          (recur (estimation/next-threshold-check (record-group/num-records record-group-writer)
                                                  estimated-length
                                                  target-record-group-length)
                 record-groups-metadata)))
      (if-let [striped-record (<!! striped-record-ch)]
        (do (record-group/write! record-group-writer striped-record)
            (recur next-num-records-for-length-check record-groups-metadata))
        (let [metadata (complete-record-group! backend-writer record-group-writer)]
          (record-group/await-io-completion record-group-writer)
          (conj record-groups-metadata metadata))))))

(defn- <!!-coll [ch]
  (lazy-seq (let [v (<!! ch)]
              (when-not (nil? v)
                (cons v (<!!-coll ch))))))

(defn- >!!-coll [ch coll]
  (loop [[x & xs] coll]
    (when (and (>!! ch x) (seq xs))
      (recur xs))))

(defrecord Writer [metadata-atom stripe-fn striped-record-ch write-thread backend-writer]
  IWriter
  (write! [this records]
    (->> records
         (utils/chunked-pmap stripe-fn)
         (>!!-coll striped-record-ch))
    this)
  (set-metadata! [_ metadata]
    (swap! metadata-atom assoc :custom metadata))
  Closeable
  (close [_]
    (async/close! striped-record-ch)
    (let [record-groups-metadata (<!! write-thread)
          metadata (assoc @metadata-atom :record-groups-metadata record-groups-metadata)]
      (finish! backend-writer metadata))))

(defn- writer [backend-writer schema options]
  (let [{:keys [target-record-group-length target-data-page-length]} (merge default-options options)
        parsed-schema (schema/parse schema)
        striped-record-ch (async/chan 100)
        record-group-writer (record-group/writer target-data-page-length (schema/column-specs parsed-schema))]
    (map->Writer
     {:metadata-atom (atom (metadata/map->Metadata {:schema parsed-schema}))
      :stripe-fn (striping/stripe-fn parsed-schema)
      :striped-record-ch striped-record-ch
      :write-thread (async/thread (write-loop striped-record-ch
                                              record-group-writer
                                              backend-writer
                                              target-record-group-length))
      :backend-writer backend-writer})))

(defn byte-buffer-writer [schema & {:as options}]
  (writer (byte-array-backend-writer) schema options))

(defn byte-buffer! [^Closeable writer]
  (if-let [byte-array-writer ^ByteArrayWriter (get-in writer [:backend-writer :byte-array-writer])]
    (do (.close writer)
        (ByteBuffer/wrap (.buffer byte-array-writer) 0 (.position byte-array-writer)))
    (throw (UnsupportedOperationException. "byte-buffer! is only supported on byte-buffer writers."))))

(defn file-writer [filename schema & {:as options}]
  (writer (file-channel-backend-writer filename) schema options))

(defn- record-group-offsets [record-groups-metadata offset]
  (->> record-groups-metadata (map :length) butlast (reductions + offset)))

(defprotocol IBackendReader
  (record-group-readers [_ record-groups-metadata queried-schema])
  (length [_])
  (close [_]))

(defrecord ByteBufferBackendReader [^ByteBuffer byte-buffer]
  IBackendReader
  (length [_]
    (.limit byte-buffer))
  (record-group-readers [_ record-groups-metadata queried-schema]
    (let [byte-array-reader (ByteArrayReader. byte-buffer)]
      (map #(record-group/byte-array-reader (.sliceAhead byte-array-reader %1) %2 queried-schema)
           (record-group-offsets record-groups-metadata magic-bytes-length)
           record-groups-metadata)))
  (close [_]))

(defrecord FileChannelBackendReader [^FileChannel file-channel]
  IBackendReader
  (length [_]
    (.size file-channel))
  (record-group-readers [_ record-groups-metadata queried-schema]
    (utils/pmap-next #(record-group/file-channel-reader file-channel %1 %2 queried-schema)
                     (record-group-offsets record-groups-metadata magic-bytes-length)
                     record-groups-metadata))
  (close [_]
    (.close file-channel)))

(let [chs [(async/chan) (async/chan) (async/chan)]]
  [chs (remove (partial = (second chs)) chs)])

(defrecord Reader [backend-reader metadata queried-schema open-channels]
  IReader
  (read [_]
    (let [ch (async/chan 100)]
      (swap! open-channels conj ch)
      (async/thread
        (->> (record-group-readers backend-reader (:record-groups-metadata metadata) queried-schema)
             (mapcat record-group/read)
             (>!!-coll ch))
        (async/close! ch)
        (swap! open-channels #(remove (partial = ch) %)))
      (->> (<!!-coll ch)
           (utils/chunked-pmap #(assembly/assemble % queried-schema)))))
  (stats [_]
    (let [all-stats (->> (record-group-readers backend-reader
                                               (:record-groups-metadata metadata)
                                               queried-schema)
                         (map record-group/stats))
          record-groups-stats (map :record-group all-stats)
          columns-stats (->> (map :column-chunks all-stats)
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
    (doseq [ch @open-channels]
      (async/close! open-channels))
    (close backend-reader)))

(defn byte-buffer-reader
  [^ByteBuffer byte-buffer & {:as opts :keys [query] :or {query '_}}]
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
                                utils/byte-buffer->int)
            metadata (-> byte-buffer
                         (utils/sub-byte-buffer (- metadata-length-pos metadata-length) metadata-length)
                         metadata/read)]
        (map->Reader
         {:backend-reader (->ByteBufferBackendReader byte-buffer)
          :metadata metadata
          :open-channels (atom [])
          :queried-schema (apply schema/apply-query (:schema metadata) query (-> opts seq flatten))})))))

(defn file-reader [filename & {:as opts :keys [query] :or {query '_}}]
  (let [file-channel (utils/file-channel filename :read)
        length (.size file-channel)
        last-magic-bytes-pos (- length magic-bytes-length)
        metadata-length-pos (- last-magic-bytes-pos int-length)]
    (if-not
        (and (valid-magic-bytes? (utils/map-bytes file-channel 0 magic-bytes-length))
             (valid-magic-bytes? (utils/map-bytes file-channel last-magic-bytes-pos magic-bytes-length)))
      (throw (IllegalArgumentException.
              "File is not a valid dendrite file."))
      (let [metadata-length (-> file-channel
                                (utils/map-bytes metadata-length-pos int-length)
                                utils/byte-buffer->int)
            metadata (-> file-channel
                         (utils/map-bytes (- metadata-length-pos metadata-length) metadata-length)
                         metadata/read)]
        (map->Reader
         {:backend-reader (->FileChannelBackendReader file-channel)
          :metadata metadata
          :open-channels (atom [])
          :queried-schema (apply schema/apply-query (:schema metadata) query (-> opts seq flatten))})))))
