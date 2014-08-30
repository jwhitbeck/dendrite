;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.column-chunk
  (:require [dendrite.estimation :as estimation]
            [dendrite.encoding :as encoding]
            [dendrite.leveled-value :as lv]
            [dendrite.metadata :as metadata]
            [dendrite.page :as page]
            [dendrite.stats :as stats])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader LeveledValue]
           [dendrite.page DataPageWriter DictionaryPageWriter]
           [java.nio ByteBuffer]
           [java.util HashMap])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IColumnChunkWriter
  (write! [this leveled-values])
  (metadata [_]))

(defprotocol IColumnChunkWriterImpl
  (target-data-page-length [_])
  (flush-to-byte-buffer! [_ byte-buffer]))

(defprotocol IDataColumnChunkWriter
  (flush-data-page-writer! [_]))

(defrecord DataColumnChunkWriter [next-num-values-for-page-length-check
                                  num-pages
                                  target-data-page-length
                                  length-estimator
                                  column-spec
                                  ^ByteArrayWriter byte-array-writer
                                  ^DataPageWriter page-writer]
  IColumnChunkWriter
  (write! [this leveled-values]
    (when (>= (page/num-values page-writer) @next-num-values-for-page-length-check)
      (let [estimated-page-length (.estimatedLength page-writer)]
        (if (>= estimated-page-length target-data-page-length)
          (flush-data-page-writer! this)
          (reset! next-num-values-for-page-length-check
                  (estimation/next-threshold-check (page/num-values page-writer) estimated-page-length
                                                   target-data-page-length)))))
    (page/write! page-writer leveled-values)
    this)
  (metadata [this]
    (metadata/map->ColumnChunkMetadata {:length (.length this)
                                        :num-data-pages @num-pages
                                        :data-page-offset 0
                                        :dictionary-page-offset 0}))
  IColumnChunkWriterImpl
  (target-data-page-length [_]
    target-data-page-length)
  IDataColumnChunkWriter
  (flush-data-page-writer! [_]
    (when (pos? (page/num-values page-writer))
      (.write byte-array-writer page-writer)
      (swap! num-pages inc)
      (reset! next-num-values-for-page-length-check (int (/ (page/num-values page-writer) 2)))
      (.reset page-writer)))
  (flush-to-byte-buffer! [this byte-buffer]
    (.finish this)
    (.flush byte-array-writer ^ByteBuffer byte-buffer))
  BufferedByteArrayWriter
  (reset [_]
    (reset! num-pages 0)
    (.reset byte-array-writer)
    (.reset page-writer))
  (finish [this]
    (when (pos? (page/num-values page-writer))
      (let [estimated-length (+ (.length byte-array-writer) (.estimatedLength page-writer))]
        (flush-data-page-writer! this)
        (estimation/update! length-estimator (.length this) estimated-length))))
  (length [_]
    (.length byte-array-writer))
  (estimatedLength [this]
    (estimation/correct length-estimator (+ (.length byte-array-writer) (.estimatedLength page-writer))))
  (flush [this baw]
    (.finish this)
    (.write baw byte-array-writer)))

(defn- data-column-chunk-writer [target-data-page-length type-store column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (map->DataColumnChunkWriter
     {:next-num-values-for-page-length-check (atom 10)
      :num-pages (atom 0)
      :target-data-page-length target-data-page-length
      :length-estimator (estimation/ratio-estimator)
      :byte-array-writer (ByteArrayWriter.)
      :column-spec column-spec
      :page-writer (page/data-page-writer max-repetition-level max-definition-level type-store type
                                          encoding compression)})))

(def ^:private array-of-bytes-type (Class/forName "[B"))

(defn- bytes? [x] (instance? array-of-bytes-type x))

(defn- keyable [x]
  (if (bytes? x)
    (seq x)
    x))

(defprotocol IDictionaryColumChunkWriter
  (value-index [this v]))

(defrecord DictionaryColumnChunkWriter [^HashMap reverse-dictionary
                                        ^DictionaryPageWriter dictionary-writer
                                        ^DataColumnChunkWriter data-column-chunk-writer
                                        column-spec]
  IColumnChunkWriter
  (write! [this leveled-values]
    (->> leveled-values
         (map (fn [^LeveledValue leveled-value]
                (let [v (.value leveled-value)]
                  (if (nil? v)
                    leveled-value
                    (.assoc leveled-value (value-index this v))))))
         (write! data-column-chunk-writer))
    this)
  (metadata [this]
    (metadata/map->ColumnChunkMetadata {:length (.length this)
                                        :num-data-pages (-> data-column-chunk-writer metadata :num-data-pages)
                                        :data-page-offset (.length dictionary-writer)
                                        :dictionary-page-offset 0}))
  IColumnChunkWriterImpl
  (target-data-page-length [_]
    (target-data-page-length data-column-chunk-writer))
  (flush-to-byte-buffer! [this byte-buffer]
    (.finish this)
    (.flush (doto (ByteArrayWriter. (.length dictionary-writer)) (.write dictionary-writer))
              ^ByteBuffer byte-buffer)
    (flush-to-byte-buffer! data-column-chunk-writer ^ByteBuffer byte-buffer))
  IDictionaryColumChunkWriter
  (value-index [_ v]
    (let [k (keyable v)]
      (or (.get reverse-dictionary k)
          (do (let [idx (.size reverse-dictionary)]
                (.put reverse-dictionary k idx)
                (page/write-entry! dictionary-writer v)
                idx)))))
  BufferedByteArrayWriter
  (reset [_]
    (.clear reverse-dictionary)
    (.reset dictionary-writer)
    (.reset data-column-chunk-writer))
  (finish [_]
    (.finish dictionary-writer)
    (.finish data-column-chunk-writer))
  (length [_]
    (+ (.length dictionary-writer) (.length data-column-chunk-writer)))
  (estimatedLength [_]
    (+ (.estimatedLength dictionary-writer) (.estimatedLength data-column-chunk-writer)))
  (flush [this baw]
    (.finish this)
    (.write baw dictionary-writer)
    (.write baw data-column-chunk-writer)))

(defn- dictionary-indices-column-spec [column-spec]
  (-> column-spec
      (assoc :type :int
             :encoding :packed-run-length
             :compression :none)
      (dissoc :map-fn)))

(defn- dictionary-column-chunk-writer [target-data-page-length type-store column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (map->DictionaryColumnChunkWriter
     {:column-spec column-spec
      :reverse-dictionary (HashMap.)
      :dictionary-writer (page/dictionary-page-writer type-store type :plain compression)
      :data-column-chunk-writer (data-column-chunk-writer target-data-page-length type-store
                                                          (dictionary-indices-column-spec column-spec))})))

(defn writer [target-data-page-length type-store column-spec]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-chunk-writer target-data-page-length type-store column-spec)
    (data-column-chunk-writer target-data-page-length type-store column-spec)))

(defprotocol IColumnChunkReader
  (read [_])
  (page-headers [_]))

(defn stats [column-chunk-reader]
  (->> (page-headers column-chunk-reader)
       (map page/stats)
       (stats/pages->column-chunk-stats (:column-spec column-chunk-reader))))

(defrecord DataColumnChunkReader [^ByteArrayReader byte-array-reader
                                  column-chunk-metadata
                                  type-store
                                  column-spec]
  IColumnChunkReader
  (read [this]
    (let [map-fn (:map-fn column-spec)
          {:keys [type encoding compression max-repetition-level max-definition-level]} column-spec]
      (page/read-data-pages
       (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
       (:num-data-pages column-chunk-metadata)
       max-repetition-level
       max-definition-level
       type-store
       type
       encoding
       compression
       map-fn)))
  (page-headers [_]
    (page/read-data-page-headers (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                                 (:num-data-pages column-chunk-metadata))))

(defprotocol IDictionaryColumnChunkReader
  (indices-page-headers [_])
  (read-dictionary [_]))

(defrecord DictionaryColumnChunkReader [^ByteArrayReader byte-array-reader
                                        column-chunk-metadata
                                        type-store
                                        column-spec]
  IColumnChunkReader
  (read [this]
    (let [ ^objects dict-array (read-dictionary this)]
      (read (->DataColumnChunkReader byte-array-reader
                                     column-chunk-metadata
                                     type-store
                                     (-> (dictionary-indices-column-spec column-spec)
                                         (assoc :map-fn #(aget dict-array (int %))))))))
  (page-headers [this]
    (let [dictionary-page-header (->> column-chunk-metadata
                                      :dictionary-page-offset
                                      (.sliceAhead byte-array-reader)
                                      page/read-dictionary-header)]
      (cons dictionary-page-header (indices-page-headers this))))
  IDictionaryColumnChunkReader
  (read-dictionary [_]
    (page/read-dictionary (.sliceAhead byte-array-reader (:dictionary-page-offset column-chunk-metadata))
                          type-store
                          (:type column-spec)
                          :plain
                          (:compression column-spec)
                          (:map-fn column-spec)))
  (indices-page-headers [_]
    (page-headers (->DataColumnChunkReader byte-array-reader
                                           column-chunk-metadata
                                           type-store
                                           (dictionary-indices-column-spec column-spec)))))

(defn reader
  [byte-array-reader column-chunk-metadata type-store column-spec]
  (if (= :dictionary (:encoding column-spec))
    (->DictionaryColumnChunkReader byte-array-reader column-chunk-metadata type-store column-spec)
    (->DataColumnChunkReader byte-array-reader column-chunk-metadata type-store column-spec)))

(defn- compute-length-for-column-spec [column-chunk-reader new-colum-spec target-data-page-length type-store]
  (let [^BufferedByteArrayWriter w (reduce write!
                                           (writer target-data-page-length type-store new-colum-spec)
                                           (read column-chunk-reader))]
    (.finish w)
    (if (and (= :dictionary (:encoding new-colum-spec))
             (> (-> w metadata :data-page-offset) target-data-page-length))
      Long/MAX_VALUE                    ;      Disqualify dictionary encoding if the dictionnary is too long.
      (.length w))))

(defn find-best-encoding [column-chunk-reader target-data-page-length]
  (let [ct (-> column-chunk-reader :column-spec (assoc :compression :none))
        eligible-encodings (encoding/list-encodings-for-type (:type-store column-chunk-reader) (:type ct))]
    (->> eligible-encodings
         (reduce (fn [results encoding]
                   (assoc results encoding
                          (compute-length-for-column-spec column-chunk-reader
                                                          (assoc ct :encoding encoding)
                                                          target-data-page-length
                                                          (:type-store column-chunk-reader))))
                 {})
         (sort-by val)
         first
         key)))

(defn find-best-compression
  [column-chunk-reader target-data-page-length candidates-treshold-map & {:keys [encoding]}]
  (let [ct  (cond-> (:column-spec column-chunk-reader)
                    encoding (assoc :encoding encoding))
        compressed-lengths-map
          (->> (assoc candidates-treshold-map :none 1)
               keys
               (reduce (fn [results compression]
                         (assoc results compression
                                (compute-length-for-column-spec column-chunk-reader
                                                                (assoc ct :compression compression)
                                                                target-data-page-length
                                                                (:type-store column-chunk-reader))))
                       {}))
        no-compression-length (:none compressed-lengths-map)
        best-compression
          (->> (dissoc compressed-lengths-map :none)
               (filter (fn [[compression length]]
                         (>= (/ no-compression-length length) (get candidates-treshold-map compression))))
               (sort-by val)
               ffirst)]
    (or best-compression :none)))

(defn find-best-column-spec
  [column-chunk-reader target-data-page-length compression-candidates-treshold-map]
  (let [best-encoding (find-best-encoding column-chunk-reader target-data-page-length)
        best-compression (find-best-compression column-chunk-reader target-data-page-length
                                                compression-candidates-treshold-map
                                                :encoding best-encoding)]
    (assoc (:column-spec column-chunk-reader)
      :encoding best-encoding
      :compression best-compression)))

(defn writer->reader! [^BufferedByteArrayWriter column-chunk-writer type-store]
  (.finish column-chunk-writer)
  (let [metadata (metadata column-chunk-writer)]
    (-> (doto (ByteArrayWriter. (.length column-chunk-writer))
          (.write column-chunk-writer))
        .buffer
        ByteArrayReader.
        (reader metadata type-store (:column-spec column-chunk-writer)))))

(defn optimize! [column-chunk-writer type-store compression-candidates-treshold-map]
  (let [column-type (get-in column-chunk-writer [:column-spec :type])
        base-type-rdr (-> column-chunk-writer
                          (writer->reader! type-store)
                          (update-in [:column-spec :type] (partial encoding/base-type type-store)))
        target-data-page-length (target-data-page-length column-chunk-writer)
        optimal-column-spec (-> (find-best-column-spec base-type-rdr
                                                       target-data-page-length
                                                       compression-candidates-treshold-map)
                                (assoc :type column-type))
        derived-type-rdr (assoc-in base-type-rdr [:column-spec :type] column-type)]
    (reduce write! (writer target-data-page-length type-store optimal-column-spec) (read derived-type-rdr))))
