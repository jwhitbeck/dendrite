(ns dendrite.column
  (:require [dendrite.estimation :as estimation]
            [dendrite.metadata :as metadata]
            [dendrite.page :as page])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader]
           [dendrite.page DataPageWriter DictionaryPageWriter]
           [java.util HashMap]))

(set! *warn-on-reflection* true)

(defprotocol IColumnWriter
  (write-row [this row-values])
  (metadata [_]))

(defn write-rows [column-writer rows]
  (reduce write-row column-writer rows))

(defprotocol IDataColumnWriter
  (flush-data-page-writer [_]))

(deftype DataColumnWriter [^{:unsynchronized-mutable :int} next-num-values-for-page-size-check
                           ^{:unsynchronized-mutable :int} num-pages
                           ^int target-data-page-size
                           size-estimator
                           ^ByteArrayWriter byte-array-writer
                           ^DataPageWriter page-writer]
  IColumnWriter
  (write-row [this wrapped-values]
    (when (>= (page/num-values page-writer) next-num-values-for-page-size-check)
      (let [estimated-page-size (.estimatedSize page-writer)]
        (if (>= estimated-page-size target-data-page-size)
          (flush-data-page-writer this)
          (set! next-num-values-for-page-size-check
                (estimation/next-threshold-check (page/num-values page-writer) estimated-page-size
                                                 target-data-page-size)))))
    (page/write-all page-writer wrapped-values)
    this)
  (metadata [this]
    (metadata/column-chunk-metadata (.size this) num-pages 0 0))
  IDataColumnWriter
  (flush-data-page-writer [_]
    (when (pos? (page/num-values page-writer))
      (.write byte-array-writer page-writer)
      (set! num-pages (inc num-pages))
      (set! next-num-values-for-page-size-check (/ (.size page-writer) 2))
      (.reset page-writer)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-pages 0)
    (.reset byte-array-writer)
    (.reset page-writer))
  (finish [this]
    (when (pos? (page/num-values page-writer))
      (let [estimated-size (+ (.size byte-array-writer) (.estimatedSize page-writer))]
        (flush-data-page-writer this)
        (estimation/update! size-estimator (.size this) estimated-size))))
  (size [_]
    (.size byte-array-writer))
  (estimatedSize [this]
    (estimation/correct size-estimator (+ (.size byte-array-writer) (.estimatedSize page-writer))))
  (writeTo [this baw]
    (.finish this)
    (.write baw byte-array-writer)))

(defn- data-column-writer [target-data-page-size schema-path column-type]
  (let [{:keys [required? value-type encoding compression-type]} column-type
        max-definition-level (count schema-path)]
    (DataColumnWriter. 10
                       0
                       target-data-page-size
                       (estimation/ratio-estimator)
                       (ByteArrayWriter.)
                       (page/data-page-writer max-definition-level required? value-type
                                              encoding compression-type))))

(defprotocol IDictionaryColumWriter
  (value-index [this v]))

(deftype DictionaryColumnWriter [^HashMap reverse-dictionary
                                  ^DictionaryPageWriter dictionary-writer
                                  ^DataColumnWriter data-column-writer]
  IColumnWriter
  (write-row [this wrapped-values]
    (->> wrapped-values
         (map (fn [wrapped-value]
                (let [v (:value wrapped-value)]
                  (if (nil? v)
                    wrapped-value
                    (assoc wrapped-value :value (value-index this v))))))
         (write-row data-column-writer))
    this)
  (metadata [this]
    (metadata/column-chunk-metadata (.size this) (-> data-column-writer metadata :num-data-pages)
                                    (.size dictionary-writer) 0))
  IDictionaryColumWriter
  (value-index [_ v]
    (or (.get reverse-dictionary v)
        (do (let [idx (.size reverse-dictionary)]
              (.put reverse-dictionary v idx)
              (page/write dictionary-writer v)
              idx))))
  BufferedByteArrayWriter
  (reset [_]
    (.clear reverse-dictionary)
    (.reset dictionary-writer)
    (.reset data-column-writer))
  (finish [_]
    (.finish dictionary-writer)
    (.finish data-column-writer))
  (size [_]
    (+ (.size dictionary-writer) (.size data-column-writer)))
  (estimatedSize [_]
    (+ (.estimatedSize dictionary-writer) (.estimatedSize data-column-writer)))
  (writeTo [this baw]
    (.finish this)
    (.write baw dictionary-writer)
    (.write baw data-column-writer)))

(defn- dictionary-indices-column-type [column-type]
  (assoc column-type
    :value-type :int32
    :encoding :packed-run-length
    :compression-type :none))

(defn- dictionary-column-writer [target-data-page-size schema-path column-type]
  (let [{:keys [required? value-type encoding compression-type]} column-type
        max-definition-level (count schema-path)]
    (DictionaryColumnWriter.
     (HashMap.)
     (page/dictionary-page-writer (:value-type column-type) :plain (:compression-type column-type))
     (data-column-writer target-data-page-size schema-path (dictionary-indices-column-type column-type)))))

(defn column-writer [target-data-page-size schema-path column-type]
  (if (= :dictionary (:encoding column-type))
    (dictionary-column-writer target-data-page-size schema-path column-type)
    (data-column-writer target-data-page-size schema-path column-type)))

(defprotocol IColumnReader
  (read-column [_] [_ map-fn])
  (stats [_]))

(defn- apply-to-wrapped-value [f wrapped-value]
  (let [v (:value wrapped-value)]
    (if (nil? v)
      wrapped-value
      (assoc wrapped-value :value (f v)))))

(defrecord ColumnStats [num-values num-pages header-bytes repetition-level-bytes
                        definition-level-bytes data-bytes dictionary-header-bytes dictionary-bytes])

(defn data-page-header->partial-column-stats [data-page-header]
  (ColumnStats. (:num-values data-page-header)
                1
                (page/header-length data-page-header)
                (:repetition-levels-size data-page-header)
                (:definition-levels-size data-page-header)
                (:compressed-data-size data-page-header)
                0
                0))

(defn add-column-stats [column-stats-a column-stats-b]
  (->> (map + (vals column-stats-a) (vals column-stats-b))
       (apply ->ColumnStats)))

(defrecord DataColumnReader [^ByteArrayReader byte-array-reader
                             column-chunk-metadata
                             schema-path
                             column-type]
  IColumnReader
  (read-column [this]
    (read-column this identity))
  (read-column [_ map-fn]
    (let [{:keys [value-type encoding compression-type]} column-type]
      (->> (page/read-data-pages (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                                 (:num-data-pages column-chunk-metadata)
                                 (count schema-path)
                                 value-type
                                 encoding
                                 compression-type)
           (map (partial apply-to-wrapped-value map-fn)))))
  (stats [_]
    (->> (page/read-data-page-headers (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                                      (:num-data-pages column-chunk-metadata))
         (map data-page-header->partial-column-stats)
         (reduce add-column-stats))))

(defn- data-column-reader
  [byte-array-reader column-chunk-metadata schema-path column-type]
  (DataColumnReader. byte-array-reader column-chunk-metadata schema-path column-type))

(defprotocol IDictionaryColumnReader
  (read-indices [_])
  (read-dictionary [_])
  (indices-stats [_]))

(defrecord DictionaryColumnReader [^ByteArrayReader byte-array-reader
                                    column-chunk-metadata
                                    schema-path
                                    column-type]
  IColumnReader
  (read-column [this]
    (read-column this identity))
  (read-column [this map-fn]
    (let [dictionary-array (into-array (->> (read-dictionary this) (map map-fn)))]
      (->> (read-indices this)
           (map (fn [wrapped-value]
                  (let [i (:value wrapped-value)]
                    (if (nil? i)
                      wrapped-value
                      (assoc wrapped-value :value (aget ^objects dictionary-array (int i))))))))))
  (stats [this]
    (let [dictionary-header (->> column-chunk-metadata
                                  :dictionary-page-offset
                                  (.sliceAhead byte-array-reader)
                                  page/read-dictionary-header)
          data-column-stats (indices-stats this)]
      (assoc data-column-stats
        :dictionary-header-bytes (page/header-length dictionary-header)
        :dictionary-bytes (page/body-length dictionary-header))))
  IDictionaryColumnReader
  (read-dictionary [_]
    (page/read-dictionary (.sliceAhead byte-array-reader (:dictionary-page-offset column-chunk-metadata))
                           (:value-type column-type)
                           :plain
                           (:compression-type column-type)))
  (read-indices [_]
    (-> (data-column-reader byte-array-reader
                            column-chunk-metadata
                            schema-path
                            (dictionary-indices-column-type column-type))
        read-column))
  (indices-stats [_]
    (-> (data-column-reader byte-array-reader
                            column-chunk-metadata
                            schema-path
                            (dictionary-indices-column-type column-type))
        stats)))

(defn- dictionary-column-reader
  [byte-array-reader column-chunk-metadata schema-path column-type]
  (DictionaryColumnReader. byte-array-reader column-chunk-metadata schema-path column-type))

(defn column-reader
  [byte-array-reader column-chunk-metadata schema-path column-type]
  (if (= :dictionary (:encoding column-type))
    (dictionary-column-reader byte-array-reader column-chunk-metadata schema-path column-type)
    (data-column-reader byte-array-reader column-chunk-metadata schema-path column-type)))
