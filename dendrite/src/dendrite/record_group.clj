;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.record-group
  (:require [dendrite.column-chunk :as column-chunk]
            [dendrite.metadata :as metadata]
            [dendrite.schema :as schema]
            [dendrite.stats :as stats]
            [dendrite.utils :as utils])
  (:import [dendrite.java MemoryOutputStream IOutputBuffer]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode]))

(set! *warn-on-reflection* true)

(definterface IRecordGroupWriter
  (write [record-stripes])
  (flushToFileChannel [file-channel])
  (awaitIOCompletion [])
  (numRecords [])
  (metadata [])
  (columnSpecs []))

(defn- write-byte-buffer [^FileChannel file-channel ^ByteBuffer byte-buffer]
  (try (.write file-channel byte-buffer)
       :success
       (catch Exception e
         e)))

(deftype RecordGroupWriter [^{:unsynchronized-mutable true :tag long} num-records
                            column-chunk-writers
                            ^MemoryOutputStream memory-output-stream
                            io-thread
                            type-store]
  IRecordGroupWriter
  (write [this record-stripes]
    (dorun (pmap column-chunk/write! column-chunk-writers record-stripes))
    (set! num-records (+ num-records (count (first record-stripes))))
    this)
  (numRecords [_]
    num-records)
  (metadata [this]
    (metadata/map->RecordGroupMetadata
     {:length (.length this)
      :num-records num-records
      :column-chunks-metadata (mapv column-chunk/metadata column-chunk-writers)}))
  (flushToFileChannel [this file-channel]
    (.finish this)
    (.awaitIOCompletion this)
    (.reset memory-output-stream)
    (.writeTo this memory-output-stream)
    (reset! io-thread (future (write-byte-buffer file-channel (.byteBuffer memory-output-stream)))))
  (awaitIOCompletion [_]
    (when @io-thread
      (let [ret @@io-thread]
        (if (instance? Exception ret)
          (throw ret)))))
  (columnSpecs [_]
    (map column-chunk/column-spec column-chunk-writers))
  IOutputBuffer
  (reset [_]
    (set! num-records 0)
    (doseq [^IOutputBuffer column-chunk-writer column-chunk-writers]
      (.reset column-chunk-writer)))
  (finish [this]
    (doseq [^IOutputBuffer column-chunk-writer column-chunk-writers]
      (.finish column-chunk-writer)))
  (length [_]
    (->> column-chunk-writers
         (map #(.length ^IOutputBuffer %))
         (reduce +)))
  (estimatedLength [_]
    (->> column-chunk-writers
         (map #(.estimatedLength ^IOutputBuffer %))
         (reduce +)))
  (writeTo [this mos]
    (.finish this)
    (doseq [^IOutputBuffer column-chunk-writer column-chunk-writers]
      (.writeTo column-chunk-writer mos))))

(defn writer
  ^dendrite.record_group.RecordGroupWriter
  [target-data-page-length type-store column-specs]
  (RecordGroupWriter. 0
                      (mapv (partial column-chunk/writer target-data-page-length type-store) column-specs)
                      (MemoryOutputStream.)
                      (atom nil)
                      type-store))

(defprotocol IRecordGroupReader
  (read [_])
  (stats [_]))

(defrecord RecordGroupReader [num-records column-chunk-readers]
  IRecordGroupReader
  (read [_]
    (when (pos? num-records)
      (map column-chunk/read column-chunk-readers)))
  (stats [_]
    (let [column-chunks-stats (map column-chunk/stats column-chunk-readers)]
      {:record-group (stats/column-chunks->record-group-stats num-records column-chunks-stats)
       :column-chunks column-chunks-stats})))

(defn- column-chunk-lengths [record-group-metadata]
  (->> record-group-metadata :column-chunks-metadata (map :length)))

(defn- column-chunk-byte-offsets
  [record-group-metadata]
  (->> record-group-metadata column-chunk-lengths butlast (reductions + 0)))

(defn byte-buffer-reader [^ByteBuffer byte-buffer record-group-metadata type-store queried-schema]
  (let [queried-indices (schema/queried-column-indices-set queried-schema)
        byte-buffers
        (->> (column-chunk-lengths record-group-metadata)
             (map vector (column-chunk-byte-offsets record-group-metadata))
             (utils/filter-indices queried-indices)
             (map (fn [[offset length]] (utils/sub-byte-buffer byte-buffer offset length))))
        column-chunks-metadata (->> record-group-metadata
                                    :column-chunks-metadata
                                    (utils/filter-indices queried-indices))]
    (map->RecordGroupReader
     {:num-records (:num-records record-group-metadata)
      :column-chunk-readers (mapv column-chunk/reader
                                  byte-buffers
                                  column-chunks-metadata
                                  (repeat type-store)
                                  (schema/column-specs queried-schema))})))

(defn optimize! [^RecordGroupWriter record-group-writer compression-threshold-map]
  (if-not (pos? (.numRecords record-group-writer))
    record-group-writer
    (let [type-store (:type-store record-group-writer)
          optimized-column-chunk-writers
            (->> record-group-writer
                 :column-chunk-writers
                 (sort-by #(.estimatedLength ^IOutputBuffer %))
                 reverse
                 (utils/upmap #(column-chunk/optimize! % type-store compression-threshold-map))
                 (sort-by (comp :column-index column-chunk/column-spec)))]
      (assoc record-group-writer :column-chunk-writers optimized-column-chunk-writers))))
