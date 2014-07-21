(ns dendrite.core
  (:require [dendrite.assembly :as assembly]
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

(def default-options
  {:target-record-group-length (* 256 1024 1024)  ; 256 MB
   :target-data-page-length 1024})        ; 1KB

(defprotocol IWriter
  (write! [_ record])
  (set-metadata! [_ metadata])
  (check-record-group-length! [_])
  (flush! [_])
  (flush-record-group-writer! [_]))

(defprotocol IReader
  (read [_])
  (record-group-readers [_])
  (length [_])
  (stats [_])
  (metadata [_])
  (schema [_]))

(defprotocol IByteBufferWriter
  (byte-buffer! [_]))

(def ^:private common-writer-impl
  {:check-record-group-length!
   (fn [{:keys [^BufferedByteArrayWriter record-group-writer
                next-num-records-for-record-group-length-check
                target-record-group-length] :as w}]
     (when (>= (record-group/num-records record-group-writer) @next-num-records-for-record-group-length-check)
       (let [estimated-record-group-length (.estimatedLength record-group-writer)]
         (if (>= estimated-record-group-length target-record-group-length)
           (flush! w)
           (reset! next-num-records-for-record-group-length-check
                   (estimation/next-threshold-check (record-group/num-records record-group-writer)
                                                    estimated-record-group-length
                                                    target-record-group-length))))))
   :write!
   (fn [{:keys [record-group-writer stripe-fn] :as w} record]
     (check-record-group-length! w)
     (record-group/write! record-group-writer (stripe-fn record))
     w)
   :set-metadata!
   (fn [{:keys [metadata-atom]} metadata]
     (swap! metadata-atom assoc :custom metadata))
   :flush!
   (fn [{:keys [^BufferedByteArrayWriter record-group-writer
                next-num-records-for-record-group-length-check
                metadata-atom] :as w}]
     (.finish record-group-writer)
     (swap! metadata-atom update-in [:record-groups-metadata] conj (record-group/metadata record-group-writer))
     (flush-record-group-writer! w)
     (reset! next-num-records-for-record-group-length-check
             (int (/ (record-group/num-records record-group-writer) 2)))
     (.reset record-group-writer))})

(defrecord ByteBufferWriter [next-num-records-for-record-group-length-check
                             target-record-group-length
                             ^BufferedByteArrayWriter record-group-writer
                             ^ByteArrayWriter byte-array-writer
                             metadata-atom
                             stripe-fn]
  IByteBufferWriter
  (byte-buffer! [this]
    (when (pos? (record-group/num-records record-group-writer))
      (flush! this))
    (let [metadata-byte-buffer (metadata/write @metadata-atom)]
      (.write byte-array-writer metadata-byte-buffer)
      (.writeFixedInt byte-array-writer (.limit metadata-byte-buffer))
      (.writeByteArray byte-array-writer magic-bytes)
      (ByteBuffer/wrap (.buffer byte-array-writer) 0 (.position byte-array-writer)))))

(extend ByteBufferWriter
  IWriter
  (assoc common-writer-impl
    :flush-record-group-writer!
    (fn [{:keys [^BufferedByteArrayWriter record-group-writer
                 ^ByteArrayWriter byte-array-writer]}]
      (.writeTo record-group-writer byte-array-writer))))

(defn byte-buffer-writer [schema & {:as options}]
  (let [{:keys [target-record-group-length target-data-page-length]} (merge default-options options)
        parsed-schema (schema/parse schema)]
    (map->ByteBufferWriter
     {:next-num-records-for-record-group-length-check (atom 10)
      :target-record-group-length target-record-group-length
      :record-group-writer (record-group/writer target-data-page-length (schema/column-specs parsed-schema))
      :byte-array-writer (doto (ByteArrayWriter.) (.writeByteArray magic-bytes))
      :metadata-atom (atom (metadata/map->Metadata {:record-groups-metadata [] :schema parsed-schema}))
      :stripe-fn (striping/stripe-fn parsed-schema)})))

(defrecord FileWriter [next-num-records-for-record-group-length-check
                       target-record-group-length
                       ^BufferedByteArrayWriter record-group-writer
                       ^FileChannel file-channel
                       metadata-atom
                       stripe-fn]
  Closeable
  (close [this]
    (when (pos? (record-group/num-records record-group-writer))
      (flush! this))
    (record-group/await-io-completion record-group-writer)
    (let [metadata-byte-buffer (metadata/write @metadata-atom)]
      (.write file-channel metadata-byte-buffer)
      (.write file-channel (utils/int->byte-buffer (.limit metadata-byte-buffer)))
      (.write file-channel (ByteBuffer/wrap magic-bytes)))
    (.close file-channel)))

(extend FileWriter
  IWriter
  (assoc common-writer-impl
    :flush-record-group-writer!
    (fn [{:keys [^BufferedByteArrayWriter record-group-writer
                 ^FileChannel file-channel]}]
      (record-group/flush-to-file-channel! record-group-writer file-channel))))

(defn file-writer [filename schema & {:as options}]
  (let [{:keys [target-record-group-length target-data-page-length]} (merge default-options options)
        parsed-schema (schema/parse schema)]
    (map->FileWriter
     {:next-num-records-for-record-group-length-check (atom 10)
      :target-record-group-length target-record-group-length
      :record-group-writer (record-group/writer target-data-page-length (schema/column-specs parsed-schema))
      :file-channel (doto (utils/file-channel filename :write)
                          (.write (ByteBuffer/wrap magic-bytes)))
      :metadata-atom (atom (metadata/map->Metadata {:record-groups-metadata [] :schema parsed-schema}))
      :stripe-fn (striping/stripe-fn parsed-schema)})))

(defn- valid-magic-bytes? [^ByteBuffer bb] (= magic-str (utils/byte-buffer->str bb)))

(def ^:private common-reader-impl
  {:read
   (fn [{:keys [queried-schema] :as r}]
     (->> (record-group-readers r)
          (mapcat record-group/read)
          (map #(assembly/assemble % queried-schema))))
   :stats
   (fn [r]
     (let [all-stats (->> (record-group-readers r)
                          (map record-group/stats))
           record-groups-stats (map :record-group all-stats)
           columns-stats (->> (map :column-chunks all-stats)
                              (apply map vector)
                              (map stats/column-chunks->column-stats))]
       {:record-groups record-groups-stats
        :columns columns-stats
        :global (stats/record-groups->global-stats (length r) record-groups-stats)}))
   :metadata
   (fn [{:keys [metadata]}]
     (:custom metadata))
   :schema
   (fn [{:keys [metadata]}]
     (-> metadata :schema schema/unparse))})

(defrecord ByteBufferReader [^ByteBuffer byte-buffer metadata queried-schema])

(extend ByteBufferReader
  IReader
  (assoc common-reader-impl
    :length
    (fn [{:keys [^ByteBuffer byte-buffer]}]
      (.limit byte-buffer))
    :record-group-readers
    (fn [{:keys [^ByteBuffer byte-buffer metadata queried-schema]}]
      (let [byte-array-reader (-> byte-buffer ByteArrayReader. (.sliceAhead magic-bytes-length))]
        (->> metadata
             :record-groups-metadata
             (map :length)
             butlast
             (reductions #(.sliceAhead ^ByteArrayReader %1 %2) byte-array-reader)
             (map (fn [record-group-metadata bar]
                    (record-group/byte-array-reader bar record-group-metadata queried-schema))
                  (:record-groups-metadata metadata)))))))

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
        (map->ByteBufferReader
         {:byte-buffer byte-buffer
          :metadata metadata
          :queried-schema (apply schema/apply-query (:schema metadata) query (-> opts seq flatten))})))))

(defrecord FileReader [^FileChannel file-channel metadata queried-schema]
  Closeable
  (close [_]
    (.close file-channel)))

(extend FileReader
  IReader
  (assoc common-reader-impl
    :length
    (fn [{:keys [^FileChannel file-channel]}]
      (.size file-channel))
    :record-group-readers
    (fn [{:keys [^FileChannel file-channel metadata queried-schema]}]
      (let [record-group-offsets (->> metadata
                                      :record-groups-metadata
                                      (map :length)
                                      butlast
                                      (reductions + magic-bytes-length))]
        (utils/pmap-next #(record-group/file-channel-reader file-channel %1 %2 queried-schema)
                         record-group-offsets
                         (:record-groups-metadata metadata))))))

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
        (map->FileReader
         {:file-channel file-channel
          :metadata metadata
          :queried-schema (apply schema/apply-query (:schema metadata) query (-> opts seq flatten))})))))
