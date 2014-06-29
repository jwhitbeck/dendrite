(ns dendrite.record-group
  (:require [dendrite.column :as column]
            [dendrite.metadata :as metadata]
            [dendrite.schema :as schema])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IRecordGroupWriter
  (write! [_ striped-record])
  (metadata [_]))

(deftype RecordGroupWriter
    [^:unsynchronized-mutable num-records
     column-writers]
  IRecordGroupWriter
  (write! [this striped-record]
    (->> (interleave column-writers striped-record)
         (partition 2)
         (map (partial apply column/write!))
         doall)
    (set! num-records (inc num-records))
    this)
  (metadata [this]
    (metadata/map->RecordGroupMetadata {:bytes-size (.size this)
                                        :num-records num-records
                                        :column-chunks-metadata (mapv column/metadata column-writers)}))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-records 0)
    (doseq [column-writer column-writers]
      (.reset ^BufferedByteArrayWriter column-writer)))
  (finish [this]
    (doseq [column-writer column-writers]
      (.finish ^BufferedByteArrayWriter column-writer)))
  (size [_]
    (->> column-writers
         (map #(.size ^BufferedByteArrayWriter %))
         (reduce +)))
  (estimatedSize [_]
    (->> column-writers
         (map #(.estimatedSize ^BufferedByteArrayWriter %))
         (reduce +)))
  (writeTo [this baw]
    (.finish this)
    (doseq [column-writer column-writers]
      (.writeTo ^BufferedByteArrayWriter column-writer baw))))

(defn writer [target-data-page-size column-specs]
  (RecordGroupWriter. 0 (mapv (partial column/writer target-data-page-size) column-specs)))

(defprotocol IRecordGroupReader
  (read [_])
  (stats [_]))

(defrecord RecordGroupReader [num-records column-readers]
  IRecordGroupReader
  (read [_] (->> (map column/read column-readers)
                 (apply map vector)
                 (take num-records)))
  (stats [_] (map column/stats column-readers)))

(defn column-byte-offsets [record-group-metadata]
  (->> record-group-metadata
       :column-chunks-metadata
       (map :bytes-size)
       butlast
       (reductions + 0)))

(defn filter-column-byte-offsets [queried-columns-set column-byte-offsets]
  (->> column-byte-offsets
       (interleave (range))
       (partition 2)
       (filter (comp queried-columns-set first))
       (map second)))

(defn record-group-byte-array-reader [^ByteArrayReader byte-array-reader record-group-metadata queried-schema]
  (let [byte-array-readers
          (->> record-group-metadata
               column-byte-offsets
               (filter-column-byte-offsets (schema/queried-columns-set queried-schema))
               (map #(.sliceAhead byte-array-reader %)))]
    (RecordGroupReader. (:num-records record-group-metadata)
                        (map column/reader
                             byte-array-readers
                             (:column-chunks-metadata record-group-metadata)
                             (schema/column-specs queried-schema)
                             (schema/column-reader-fns queried-schema)))))
