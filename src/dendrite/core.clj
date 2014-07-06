(ns dendrite.core
  (:require [dendrite.assembly :as assembly]
            [dendrite.striping :as striping]
            [dendrite.estimation :as estimation]
            [dendrite.metadata :as metadata]
            [dendrite.record-group :as record-group]
            [dendrite.schema :as schema]
            [dendrite.stats :as stats])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader]
           [java.nio ByteBuffer ByteOrder])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(def magic-bytes (->> [\d \e \n \1] (map byte) byte-array))

(def default-options
  {:target-record-group-size (* 256 1024 1024)  ; 256 MB
   :target-data-page-size 1024})        ; 1KB

(defprotocol IWriter
  (write! [_ record])
  (set-metadata! [_ metadata])
  (flush-record-group-writer! [_]))

(defprotocol IReader
  (read [_])
  (stats [_])
  (metadata [_])
  (schema [_]))

(defprotocol IByteBufferWriter
  (byte-buffer! [_]))

(deftype ByteBufferWriter [^:unsynchronized-mutable next-num-records-for-record-group-size-check
                           ^int target-record-group-size
                           ^BufferedByteArrayWriter record-group-writer
                           ^ByteArrayWriter byte-array-writer
                           metadata-atom
                           stripe-fn]
  IWriter
  (write! [this record]
    (when (>= (record-group/num-records record-group-writer) next-num-records-for-record-group-size-check)
      (let [estimated-record-group-size (.estimatedSize record-group-writer)]
        (if (>= estimated-record-group-size target-record-group-size)
          (flush-record-group-writer! this)
          (set! next-num-records-for-record-group-size-check
                (estimation/next-threshold-check (record-group/num-records record-group-writer)
                                                 estimated-record-group-size target-record-group-size)))))
    (record-group/write! record-group-writer (stripe-fn record))
    this)
  (set-metadata! [_ metadata]
    (swap! metadata-atom assoc :custom metadata))
  (flush-record-group-writer! [_]
    (.finish record-group-writer)
    (swap! metadata-atom update-in [:record-groups-metadata] conj (record-group/metadata record-group-writer))
    (.write byte-array-writer record-group-writer)
    (set! next-num-records-for-record-group-size-check
          (int (/ (record-group/num-records record-group-writer) 2)))
    (.reset record-group-writer))
  IByteBufferWriter
  (byte-buffer! [this]
    (when (pos? (record-group/num-records record-group-writer))
      (flush-record-group-writer! this))
    (let [metadata-byte-buffer ^ByteBuffer (metadata/write @metadata-atom)]
      (.write byte-array-writer metadata-byte-buffer)
      (.writeFixedInt byte-array-writer (.limit metadata-byte-buffer))
      (.writeByteArray byte-array-writer magic-bytes)
      (ByteBuffer/wrap (.buffer byte-array-writer) 0 (.position byte-array-writer)))))

(defn byte-buffer-writer [schema & {:as options}]
  (let [{:keys [target-record-group-size target-data-page-size]} (merge default-options options)
        byte-array-writer (doto (ByteArrayWriter.) (.writeByteArray magic-bytes))
        parsed-schema (schema/parse schema)
        record-group-writer (record-group/writer target-data-page-size (schema/column-specs parsed-schema))]
    (ByteBufferWriter. 10
                       target-record-group-size
                       record-group-writer
                       byte-array-writer
                       (atom (metadata/map->Metadata {:record-groups-metadata [] :schema parsed-schema}))
                       (striping/stripe-fn parsed-schema))))

(defn- valid-magic-bytes? [^ByteBuffer bb]
  (and (= (.get bb) (byte \d))
       (= (.get bb) (byte \e))
       (= (.get bb) (byte \n))
       (= (.get bb) (byte \1))))

(defn- record-group-readers [^ByteArrayReader byte-array-reader record-groups-metadata queried-schema]
  (->> record-groups-metadata
       (map :num-bytes)
       butlast
       (reductions #(.sliceAhead ^ByteArrayReader %1 ^int %2) byte-array-reader)
       (interleave record-groups-metadata)
       (partition 2)
       (map (fn [[record-group-metadata bar]]
              (record-group/record-group-byte-array-reader bar record-group-metadata queried-schema)))))

(defrecord ByteBufferReader [^ByteArrayReader byte-array-reader buffer-num-bytes metadata queried-schema]
  IReader
  (read [_]
    (->> (record-group-readers byte-array-reader (:record-groups-metadata metadata) queried-schema)
         (mapcat record-group/read)
         (map #(assembly/assemble % queried-schema))))
  (stats [_]
    (let [all-stats (->> (record-group-readers byte-array-reader
                                               (:record-groups-metadata metadata)
                                               queried-schema)
                         (map record-group/stats))
          record-groups-stats (map :record-group all-stats)
          columns-stats (->> (map :column-chunks all-stats)
                             (apply map vector)
                             (map stats/column-chunks->column-stats))]
      {:record-groups record-groups-stats
       :columns columns-stats
       :global (stats/record-groups->global-stats buffer-num-bytes record-groups-stats)}))
  (metadata [_]
    (:custom metadata))
  (schema [_]
    (:schema metadata)))

(defn- sub-byte-buffer ^ByteBuffer [^ByteBuffer bb offset num-bytes]
  (doto (.slice bb)
    (.position offset)
    (.limit (+ offset num-bytes))))

(defn byte-buffer-reader
  [^ByteBuffer byte-buffer & {:as opts :keys [query] :or {query '_}}]
  (let [num-bytes (.limit byte-buffer)
        magic-num-bytes (count magic-bytes)
        int-num-bytes 4]
    (if-not (and (valid-magic-bytes? (sub-byte-buffer byte-buffer 0 magic-num-bytes))
                 (valid-magic-bytes? (sub-byte-buffer byte-buffer
                                                      (- num-bytes magic-num-bytes)
                                                      magic-num-bytes)))
      (throw (IllegalArgumentException.
              "Provided byte buffer does not contain a valid dendrite serialization."))
      (let [metadata-num-bytes (.getInt (doto (sub-byte-buffer byte-buffer
                                                               (- num-bytes magic-num-bytes int-num-bytes)
                                                               int-num-bytes)
                                          (.order ByteOrder/LITTLE_ENDIAN)))
            metadata (-> (sub-byte-buffer byte-buffer
                                          (- num-bytes magic-num-bytes int-num-bytes metadata-num-bytes)
                                          metadata-num-bytes)
                         metadata/read)
            queried-schema (apply schema/apply-query (:schema metadata) query (-> opts seq flatten))]
        (ByteBufferReader. (-> byte-buffer ByteArrayReader. (.sliceAhead magic-num-bytes))
                           num-bytes
                           metadata
                           queried-schema)))))
