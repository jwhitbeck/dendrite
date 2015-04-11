;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
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
            [dendrite.stats :as stats]
            [dendrite.utils :as utils])
  (:import [dendrite.java LeveledValue MemoryOutputStream IOutputBuffer]
           [dendrite.page DataPageWriter DictionaryPageWriter IPageHeader]
           [java.nio ByteBuffer]
           [java.util HashMap])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IColumnChunkWriter
  (write! [this leveled-values])
  (metadata [_]))

(defprotocol IColumnChunkReader
  (read [_])
  (page-headers [_]))

(defprotocol IDictionaryColumnChunkReader
  (indices-page-headers [_])
  (read-dictionary [_]))

(defprotocol IColumnChunkWriterImpl
  (target-data-page-length [_]))

(defprotocol IDataColumnChunkWriter
  (flush-data-page-writer! [_]))

(defrecord DataColumnChunkWriter [next-num-values-for-page-length-check
                                  num-pages
                                  target-data-page-length
                                  length-estimator
                                  column-spec
                                  ^MemoryOutputStream memory-output-stream
                                  ^DataPageWriter page-writer]
  IColumnChunkWriter
  (write! [this v]
    (when (>= (.numValues page-writer) @next-num-values-for-page-length-check)
      (let [estimated-page-length (.estimatedLength page-writer)]
        (if (>= estimated-page-length target-data-page-length)
          (flush-data-page-writer! this)
          (reset! next-num-values-for-page-length-check
                  (estimation/next-threshold-check (.numValues page-writer) estimated-page-length
                                                   target-data-page-length)))))
    ;; Some pages compress "infinitely" well (e.g., a run-length encoded list of zeros). Since pages are fully
    ;; realized when read, this can lead to memory issues when deserializng so we cap the total number of
    ;; values in a page here.
    (let [max-num-values-per-page target-data-page-length]
      (when (>= (.numValues page-writer) max-num-values-per-page)
        (flush-data-page-writer! this)))
    (.write page-writer v)
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
    (when (pos? (.numValues page-writer))
      (.writeTo page-writer memory-output-stream)
      (swap! num-pages inc)
      (reset! next-num-values-for-page-length-check (int (/ (.numValues page-writer) 2)))
      (.reset page-writer)))
  IOutputBuffer
  (reset [_]
    (reset! num-pages 0)
    (.reset memory-output-stream)
    (.reset page-writer))
  (finish [this]
    (when (pos? (.numValues page-writer))
      (let [estimated-length (+ (.length memory-output-stream) (.estimatedLength page-writer))]
        (flush-data-page-writer! this)
        (estimation/update! length-estimator (.length this) estimated-length))))
  (length [_]
    (.length memory-output-stream))
  (estimatedLength [this]
    (estimation/correct length-estimator (+ (.length memory-output-stream) (.estimatedLength page-writer))))
  (writeTo [this mos]
    (.finish this)
    (.writeTo memory-output-stream mos)))

(defn- data-column-chunk-writer [target-data-page-length type-store column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (map->DataColumnChunkWriter
     {:next-num-values-for-page-length-check (atom 10)
      :num-pages (atom 0)
      :target-data-page-length target-data-page-length
      :length-estimator (estimation/ratio-estimator)
      :memory-output-stream (MemoryOutputStream.)
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
  (write! [this v]
    (if (pos? (:max-repetition-level column-spec))
      (->> v
           (map (fn [^LeveledValue leveled-value]
                  (let [v (.value leveled-value)]
                    (if (nil? v)
                      leveled-value
                      (.assoc leveled-value (value-index this v))))))
           (write! data-column-chunk-writer))
      (write! data-column-chunk-writer (when-not (nil? v) (value-index this v))))
    this)
  (metadata [this]
    (metadata/map->ColumnChunkMetadata {:length (.length this)
                                        :num-data-pages (-> data-column-chunk-writer metadata :num-data-pages)
                                        :data-page-offset (.length dictionary-writer)
                                        :dictionary-page-offset 0}))
  IColumnChunkWriterImpl
  (target-data-page-length [_]
    (target-data-page-length data-column-chunk-writer))
  IDictionaryColumChunkWriter
  (value-index [_ v]
    (let [k (keyable v)]
      (or (.get reverse-dictionary k)
          (let [idx (.size reverse-dictionary)]
            (.put reverse-dictionary k idx)
            (.writeEntry dictionary-writer v)
            idx))))
  IOutputBuffer
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
  (writeTo [this mos]
    (.finish this)
    (.writeTo dictionary-writer mos)
    (.writeTo data-column-chunk-writer mos)))

(defn- dictionary-indices-column-spec [column-spec]
  (-> column-spec
      (assoc :type :int
             :encoding :packed-run-length)
      (dissoc :map-fn)))

(defn- dictionary-column-chunk-writer [target-data-page-length type-store column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (map->DictionaryColumnChunkWriter
     {:column-spec column-spec
      :reverse-dictionary (HashMap.)
      :dictionary-writer (page/dictionary-page-writer type-store type :plain compression)
      :data-column-chunk-writer (data-column-chunk-writer target-data-page-length type-store
                                                          (dictionary-indices-column-spec column-spec))})))

(declare writer->reader!)

(defn flat-read [reader] (utils/flatten-1 (read reader)))

(defrecord FrequencyColumnChunkWriter [^HashMap reverse-dictionary
                                       ^HashMap index-frequencies
                                       ^DictionaryPageWriter dictionary-writer
                                       ^DataColumnChunkWriter data-column-chunk-writer
                                       ^DataColumnChunkWriter buffer-column-chunk-writer
                                       column-spec
                                       type-store
                                       finished?]
  IColumnChunkWriter
  (write! [this v]
    (if (pos? (:max-repetition-level column-spec))
      (->> v
           (map (fn [^LeveledValue leveled-value]
                  (let [v (.value leveled-value)]
                    (if (nil? v)
                      leveled-value
                      (.assoc leveled-value (value-index this v))))))
           (write! buffer-column-chunk-writer))
      (write! buffer-column-chunk-writer (when-not (nil? v) (value-index this v))))
    this)
  (metadata [this]
    (metadata/map->ColumnChunkMetadata {:length (.length this)
                                        :num-data-pages (-> data-column-chunk-writer metadata :num-data-pages)
                                        :data-page-offset (.length dictionary-writer)
                                        :dictionary-page-offset 0}))
  IColumnChunkWriterImpl
  (target-data-page-length [_]
    (target-data-page-length data-column-chunk-writer))
  IDictionaryColumChunkWriter
  (value-index [_ v]
    (let [k (keyable v)
          i (or (.get reverse-dictionary k)
                (let [idx (.size reverse-dictionary)]
                  (.put reverse-dictionary k idx)
                  (.writeEntry dictionary-writer v)
                  idx))]
      (.put index-frequencies i (inc (or (.get index-frequencies i) 0)))
      i))
  IOutputBuffer
  (reset [_]
    (reset! finished? false)
    (.clear reverse-dictionary)
    (.clear index-frequencies)
    (.reset dictionary-writer)
    (.reset data-column-chunk-writer)
    (.reset buffer-column-chunk-writer))
  (finish [_]
    (when-not @finished?
      (let [^ints index-map (->> index-frequencies
                                 (sort-by val)
                                 (map key)
                                 reverse
                                 (map vector (range))
                                 (reduce (fn [^ints a [oi di]] (do (aset a (int oi) (int di)) a))
                                         (make-array Integer/TYPE (.numValues dictionary-writer))))
            ^objects dictionary-array (let [mos (MemoryOutputStream.)]
                                        (.writeTo dictionary-writer mos)
                                        (page/read-dictionary (.byteBuffer mos)
                                                              type-store
                                                              (:type column-spec)
                                                              :plain
                                                              (:compression column-spec)
                                                              nil))
            sorted-dictionnary-array (->> dictionary-array
                                          (map vector (range))
                                          (reduce (fn [^objects a [oi v]]
                                                    (do (aset a (aget index-map oi) v)
                                                        a))
                                                  (object-array (alength dictionary-array))))]
        (.reset dictionary-writer)
        (doseq [e sorted-dictionnary-array]
          (.writeEntry dictionary-writer e))
        (doseq [v (flat-read (writer->reader! buffer-column-chunk-writer type-store))]
          (if (pos? (:max-repetition-level column-spec))
            (->> v
                 (map (fn [^LeveledValue leveled-value]
                        (let [oi (.value leveled-value)]
                          (if (nil? oi)
                            leveled-value
                            (.assoc leveled-value (aget index-map oi))))))
                 (write! data-column-chunk-writer))
            (write! data-column-chunk-writer (when-not (nil? v) (aget index-map v))))))
      (reset! finished? true))
    (.finish dictionary-writer)
    (.finish data-column-chunk-writer))
  (length [_]
    (+ (.length dictionary-writer) (.length data-column-chunk-writer)))
  (estimatedLength [_]
    (+ (.estimatedLength dictionary-writer) (.estimatedLength buffer-column-chunk-writer)))
  (writeTo [this mos]
    (.finish this)
    (.writeTo dictionary-writer mos)
    (.writeTo data-column-chunk-writer mos)))

(defn- frequency-indices-column-spec [column-spec]
  (-> column-spec
      (assoc :type :int
             :encoding :vlq)
      (dissoc :map-fn)))

(defn- frequency-column-chunk-writer [target-data-page-length type-store column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (map->FrequencyColumnChunkWriter
     {:column-spec column-spec
      :reverse-dictionary (HashMap.)
      :index-frequencies (HashMap.)
      :dictionary-writer (page/dictionary-page-writer type-store type :plain compression)
      :buffer-column-chunk-writer (data-column-chunk-writer target-data-page-length type-store
                                                            (frequency-indices-column-spec column-spec))
      :data-column-chunk-writer (data-column-chunk-writer target-data-page-length type-store
                                                          (frequency-indices-column-spec column-spec))
      :type-store type-store
      :finished? (atom false)})))

(defn writer [target-data-page-length type-store column-spec]
  (case (:encoding column-spec)
    :dictionary (dictionary-column-chunk-writer target-data-page-length type-store column-spec)
    :frequency (frequency-column-chunk-writer target-data-page-length type-store column-spec)
    (data-column-chunk-writer target-data-page-length type-store column-spec)))

(defn stats [column-chunk-reader]
  (->> (page-headers column-chunk-reader)
       (map #(.stats ^IPageHeader %))
       (stats/pages->column-chunk-stats (:column-spec column-chunk-reader))))

(defrecord DataColumnChunkReader [^ByteBuffer byte-buffer
                                  column-chunk-metadata
                                  type-store
                                  column-spec]
  IColumnChunkReader
  (read [this]
    (let [map-fn (:map-fn column-spec)
          {:keys [type encoding compression max-repetition-level max-definition-level]} column-spec]
      (page/read-data-pages
       (utils/skip byte-buffer (:data-page-offset column-chunk-metadata))
       (:num-data-pages column-chunk-metadata)
       max-repetition-level
       max-definition-level
       type-store
       type
       encoding
       compression
       map-fn)))
  (page-headers [_]
    (page/read-data-page-headers (utils/skip byte-buffer (:data-page-offset column-chunk-metadata))
                                 (:num-data-pages column-chunk-metadata))))

(defrecord DictionaryColumnChunkReader [^ByteBuffer byte-buffer
                                        column-chunk-metadata
                                        type-store
                                        column-spec]
  IColumnChunkReader
  (read [this]
    (let [^objects dict-array (read-dictionary this)]
      (read (->DataColumnChunkReader byte-buffer
                                     column-chunk-metadata
                                     type-store
                                     (assoc (if (= :dictionary (:encoding column-spec))
                                              (dictionary-indices-column-spec column-spec)
                                              (frequency-indices-column-spec column-spec))
                                            :map-fn
                                            #(aget dict-array (int %)))))))
  (page-headers [this]
    (let [dictionary-page-header (->> column-chunk-metadata
                                      :dictionary-page-offset
                                      (utils/skip byte-buffer)
                                      page/read-dictionary-header)]
      (cons dictionary-page-header (indices-page-headers this))))
  IDictionaryColumnChunkReader
  (read-dictionary [_]
    (page/read-dictionary (utils/skip byte-buffer (:dictionary-page-offset column-chunk-metadata))
                          type-store
                          (:type column-spec)
                          :plain
                          (:compression column-spec)
                          (:map-fn column-spec)))
  (indices-page-headers [_]
    (page-headers (->DataColumnChunkReader byte-buffer
                                           column-chunk-metadata
                                           type-store
                                           (if (= :dictionary (:encoding column-spec))
                                             (dictionary-indices-column-spec column-spec)
                                             (frequency-indices-column-spec column-spec))))))

(defn reader
  [byte-array-reader column-chunk-metadata type-store column-spec]
  (if (#{:dictionary :frequency} (:encoding column-spec))
    (->DictionaryColumnChunkReader byte-array-reader column-chunk-metadata type-store column-spec)
    (->DataColumnChunkReader byte-array-reader column-chunk-metadata type-store column-spec)))

(defn- compute-length-for-column-spec [column-chunk-reader new-colum-spec target-data-page-length type-store]
  (let [^IOutputBuffer w (writer target-data-page-length type-store new-colum-spec)]
    (doseq [lv (flat-read column-chunk-reader)]
      (write! w lv))
    (.finish w)
    (if (and (#{:dictionary :frequency} (:encoding new-colum-spec))
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

(defn writer->reader! [^IOutputBuffer column-chunk-writer type-store]
  (.finish column-chunk-writer)
  (let [metadata (metadata column-chunk-writer)
        mos (MemoryOutputStream. (.length column-chunk-writer))]
    (.writeTo column-chunk-writer mos)
    (reader (.byteBuffer mos) metadata type-store (:column-spec column-chunk-writer))))

(defn optimize! [column-chunk-writer type-store compression-candidates-treshold-map]
  (let [column-type (get-in column-chunk-writer [:column-spec :type])
        base-type-rdr (-> column-chunk-writer
                          (writer->reader! type-store)
                          (update-in [:column-spec :type] (partial encoding/base-type type-store)))
        target-data-page-length (target-data-page-length column-chunk-writer)
        optimal-column-spec (assoc (find-best-column-spec base-type-rdr
                                                          target-data-page-length
                                                          compression-candidates-treshold-map)
                                   :type
                                   column-type)
        derived-type-rdr (assoc-in base-type-rdr [:column-spec :type] column-type)]
    (reduce write!
            (writer target-data-page-length type-store optimal-column-spec)
            (flat-read derived-type-rdr))))
