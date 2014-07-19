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

(def magic-str "den1")

(def magic-bytes (into-array Byte/TYPE magic-str))

(def default-options
  {:target-record-group-length (* 256 1024 1024)  ; 256 MB
   :target-data-page-length 1024})        ; 1KB

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

(defrecord ByteBufferWriter [next-num-records-for-record-group-length-check
                             target-record-group-length
                             ^BufferedByteArrayWriter record-group-writer
                             ^ByteArrayWriter byte-array-writer
                             metadata-atom
                             stripe-fn]
  IWriter
  (write! [this record]
    (when (>= (record-group/num-records record-group-writer) @next-num-records-for-record-group-length-check)
      (let [estimated-record-group-length (.estimatedLength record-group-writer)]
        (if (>= estimated-record-group-length target-record-group-length)
          (flush-record-group-writer! this)
          (reset! next-num-records-for-record-group-length-check
                  (estimation/next-threshold-check (record-group/num-records record-group-writer)
                                                   estimated-record-group-length
                                                   target-record-group-length)))))
    (record-group/write! record-group-writer (stripe-fn record))
    this)
  (set-metadata! [_ metadata]
    (swap! metadata-atom assoc :custom metadata))
  (flush-record-group-writer! [_]
    (.finish record-group-writer)
    (swap! metadata-atom update-in [:record-groups-metadata] conj (record-group/metadata record-group-writer))
    (.write byte-array-writer record-group-writer)
    (reset! next-num-records-for-record-group-length-check
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
  (let [{:keys [target-record-group-length target-data-page-length]} (merge default-options options)
        parsed-schema (schema/parse schema)]
    (map->ByteBufferWriter
     {:next-num-records-for-record-group-length-check (atom 10)
      :target-record-group-length target-record-group-length
      :record-group-writer (record-group/writer target-data-page-length (schema/column-specs parsed-schema))
      :byte-array-writer (doto (ByteArrayWriter.) (.writeByteArray magic-bytes))
      :metadata-atom (atom (metadata/map->Metadata {:record-groups-metadata [] :schema parsed-schema}))
      :stripe-fn (striping/stripe-fn parsed-schema)})))

(defn- valid-magic-bytes? [^ByteBuffer bb]
  (->> (repeatedly 4 #(char (.get bb)))
       (apply str)
       (= magic-str)))

(defn- record-group-readers [^ByteArrayReader byte-array-reader record-groups-metadata queried-schema]
  (->> record-groups-metadata
       (map :length)
       butlast
       (reductions #(.sliceAhead ^ByteArrayReader %1 %2) byte-array-reader)
       (map (fn [record-group-metadata bar]
              (record-group/record-group-byte-array-reader bar record-group-metadata queried-schema))
            record-groups-metadata)))

(defrecord ByteBufferReader [^ByteArrayReader byte-array-reader buffer-length metadata queried-schema]
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
       :global (stats/record-groups->global-stats buffer-length record-groups-stats)}))
  (metadata [_]
    (:custom metadata))
  (schema [_]
    (:schema metadata)))

(defn- sub-byte-buffer ^ByteBuffer [^ByteBuffer bb offset length]
  (doto (.slice bb)
    (.position offset)
    (.limit (+ offset length))))

(defn byte-buffer-reader
  [^ByteBuffer byte-buffer & {:as opts :keys [query] :or {query '_}}]
  (let [length (.limit byte-buffer)
        magic-length (count magic-bytes)
        int-length 4]
    (if-not (and (valid-magic-bytes? (sub-byte-buffer byte-buffer 0 magic-length))
                 (valid-magic-bytes? (sub-byte-buffer byte-buffer
                                                      (- length magic-length)
                                                      magic-length)))
      (throw (IllegalArgumentException.
              "Provided byte buffer does not contain a valid dendrite serialization."))
      (let [metadata-length (.getInt (doto (sub-byte-buffer byte-buffer
                                                               (- length magic-length int-length)
                                                               int-length)
                                          (.order ByteOrder/LITTLE_ENDIAN)))
            metadata (-> (sub-byte-buffer byte-buffer
                                          (- length magic-length int-length metadata-length)
                                          metadata-length)
                         metadata/read)
            queried-schema (apply schema/apply-query (:schema metadata) query (-> opts seq flatten))]
        (map->ByteBufferReader
         {:byte-array-reader (-> byte-buffer ByteArrayReader. (.sliceAhead magic-length))
          :buffer-length length
          :metadata metadata
          :queried-schema queried-schema})))))
