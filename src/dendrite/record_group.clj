(ns dendrite.record-group
  (:require [dendrite.column-chunk :as column-chunk]
            [dendrite.metadata :as metadata]
            [dendrite.schema :as schema]
            [dendrite.stats :as stats])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IRecordGroupWriter
  (write! [_ striped-record])
  (num-records [_])
  (metadata [_]))

(deftype RecordGroupWriter
    [^:unsynchronized-mutable num-records
     column-chunk-writers]
  IRecordGroupWriter
  (write! [this striped-record]
    (dorun (map column-chunk/write! column-chunk-writers striped-record))
    (set! num-records (inc num-records))
    this)
  (num-records [_]
    num-records)
  (metadata [this]
    (metadata/map->RecordGroupMetadata
     {:num-bytes (.size this)
      :num-records num-records
      :column-chunks-metadata (mapv column-chunk/metadata column-chunk-writers)}))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-records 0)
    (doseq [column-chunk-writer column-chunk-writers]
      (.reset ^BufferedByteArrayWriter column-chunk-writer)))
  (finish [this]
    (doseq [column-chunk-writer column-chunk-writers]
      (.finish ^BufferedByteArrayWriter column-chunk-writer)))
  (size [_]
    (->> column-chunk-writers
         (map #(.size ^BufferedByteArrayWriter %))
         (reduce +)))
  (estimatedSize [_]
    (->> column-chunk-writers
         (map #(.estimatedSize ^BufferedByteArrayWriter %))
         (reduce +)))
  (writeTo [this baw]
    (.finish this)
    (doseq [column-chunk-writer column-chunk-writers]
      (.writeTo ^BufferedByteArrayWriter column-chunk-writer baw))))

(defn writer [target-data-page-size column-specs]
  (RecordGroupWriter. 0 (mapv (partial column-chunk/writer target-data-page-size) column-specs)))

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

(defn column-byte-offsets [record-group-metadata]
  (->> record-group-metadata
       :column-chunks-metadata
       (map :num-bytes)
       butlast
       (reductions + 0)))

(defn filter-column-byte-offsets [queried-column-indices-set column-byte-offsets]
  (->> column-byte-offsets
       (interleave (range))
       (partition 2)
       (filter (comp queried-column-indices-set first))
       (map second)))

(defn record-group-byte-array-reader [^ByteArrayReader byte-array-reader record-group-metadata queried-schema]
  (let [byte-array-readers
          (->> record-group-metadata
               column-byte-offsets
               (filter-column-byte-offsets (schema/queried-column-indices-set queried-schema))
               (map #(.sliceAhead byte-array-reader %)))]
    (map->RecordGroupReader
     {:num-records (:num-records record-group-metadata)
      :column-chunk-readers (map column-chunk/reader
                                 byte-array-readers
                                 (:column-chunks-metadata record-group-metadata)
                                 (schema/column-specs queried-schema))})))
