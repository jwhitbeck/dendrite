(ns dendrite.record-group
  (:require [dendrite.column-chunk :as column-chunk]
            [dendrite.metadata :as metadata]
            [dendrite.schema :as schema]
            [dendrite.stats :as stats]
            [dendrite.utils :as utils])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IRecordGroupWriter
  (write! [_ striped-record])
  (flush-to-file-channel! [_ file-channel])
  (await-io-completion [_])
  (num-records [_])
  (metadata [_]))

(defn- ensure-direct-byte-buffer-large-enough [^ByteBuffer byte-buffer length margin]
  (if (and byte-buffer (>= (.capacity byte-buffer) length))
    byte-buffer
    (ByteBuffer/allocateDirect (int (* length (+ 1 margin))))))

(defn- flush-column-chunks-to-byte-buffer [^ByteBuffer byte-buffer column-chunk-writers length]
  (.clear byte-buffer)
  (doseq [column-chunk-writer column-chunk-writers]
    (.writeTo ^ByteArrayWriter (:byte-array-writer column-chunk-writer) byte-buffer))
  (doto byte-buffer
    (.limit length)
    .rewind))

(defrecord RecordGroupWriter [num-records column-chunk-writers direct-byte-buffer-agent]
  IRecordGroupWriter
  (write! [this striped-record]
    (dorun (map column-chunk/write! column-chunk-writers striped-record))
    (swap! num-records inc)
    this)
  (num-records [_]
    @num-records)
  (metadata [this]
    (metadata/map->RecordGroupMetadata
     {:length (.length this)
      :num-records @num-records
      :column-chunks-metadata (mapv column-chunk/metadata column-chunk-writers)}))
  (flush-to-file-channel! [this file-channel]
    (.finish this)
    (await direct-byte-buffer-agent)
    (let [length (.length this)]
      (send direct-byte-buffer-agent ensure-direct-byte-buffer-large-enough length 0.2)
      (send direct-byte-buffer-agent flush-column-chunks-to-byte-buffer column-chunk-writers length)
      (await direct-byte-buffer-agent)
      (send-off direct-byte-buffer-agent #(do (.write ^FileChannel file-channel ^ByteBuffer %) %)))
    this)
  (await-io-completion [_]
    (await direct-byte-buffer-agent))
  BufferedByteArrayWriter
  (reset [_]
    (reset! num-records 0)
    (doseq [column-chunk-writer column-chunk-writers]
      (.reset ^BufferedByteArrayWriter column-chunk-writer)))
  (finish [this]
    (doseq [column-chunk-writer column-chunk-writers]
      (.finish ^BufferedByteArrayWriter column-chunk-writer)))
  (length [_]
    (->> column-chunk-writers
         (map #(.length ^BufferedByteArrayWriter %))
         (reduce +)))
  (estimatedLength [_]
    (->> column-chunk-writers
         (map #(.estimatedLength ^BufferedByteArrayWriter %))
         (reduce +)))
  (writeTo [this baw]
    (.finish this)
    (doseq [column-chunk-writer column-chunk-writers]
      (.writeTo ^BufferedByteArrayWriter column-chunk-writer baw))))

(defn writer [target-data-page-length column-specs]
  (map->RecordGroupWriter
   {:num-records (atom 0)
    :column-chunk-writers (mapv (partial column-chunk/writer target-data-page-length) column-specs)
    :direct-byte-buffer-agent (agent nil)}))

(defprotocol IRecordGroupReader
  (read [_])
  (stats [_]))

(defrecord RecordGroupReader [num-records column-chunk-readers]
  IRecordGroupReader
  (read [_]
    (if-not (seq column-chunk-readers)
      (repeat num-records nil)
      (->> (map column-chunk/read column-chunk-readers)
           (apply map vector)
           (take num-records))))
  (stats [_]
    (let [column-chunks-stats (map column-chunk/stats column-chunk-readers)]
      {:record-group (stats/column-chunks->record-group-stats num-records column-chunks-stats)
       :column-chunks column-chunks-stats})))

(defn- column-chunk-lengths [record-group-metadata]
  (->> record-group-metadata :column-chunks-metadata (map :length)))

(defn- column-chunk-byte-offsets
  [record-group-metadata offset]
  (->> record-group-metadata column-chunk-lengths butlast (reductions + offset)))

(defn byte-array-reader [^ByteArrayReader byte-array-reader record-group-metadata queried-schema]
  (let [byte-array-readers
          (->> (column-chunk-byte-offsets record-group-metadata 0)
               (utils/filter-indices (schema/queried-column-indices-set queried-schema))
               (map #(.sliceAhead byte-array-reader %)))]
    (map->RecordGroupReader
     {:num-records (:num-records record-group-metadata)
      :column-chunk-readers (map column-chunk/reader
                                 byte-array-readers
                                 (:column-chunks-metadata record-group-metadata)
                                 (schema/column-specs queried-schema))})))

(defn file-channel-reader [^FileChannel file-channel offset record-group-metadata queried-schema]
  (let [byte-array-readers
          (->> (column-chunk-lengths record-group-metadata)
               (map vector (column-chunk-byte-offsets record-group-metadata offset))
               (utils/filter-indices (schema/queried-column-indices-set queried-schema))
               (map (fn [[offset length]]
                      (ByteArrayReader. (utils/map-bytes file-channel offset length)))))]
    (map->RecordGroupReader
     {:num-records (:num-records record-group-metadata)
      :column-chunk-readers (map column-chunk/reader
                                 byte-array-readers
                                 (:column-chunks-metadata record-group-metadata)
                                 (schema/column-specs queried-schema))})))
