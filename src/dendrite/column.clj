(ns dendrite.column
  (:require [dendrite.page :as page]
            [dendrite.estimation :as estimation])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader]
           [dendrite.page DataPageWriter]))

(set! *warn-on-reflection* true)

(defprotocol IColumnWriter
  (write-row [this row-values])
  (num-pages [_]))

(defn write-rows [column-writer rows]
  (reduce write-row column-writer rows))

(defprotocol IDataColumnWriter
  (flush-data-page-writer [_]))

(deftype ColumnWriter [^{:unsynchronized-mutable :int} next-num-values-for-page-size-check
                       ^{:unsynchronized-mutable :int} num-values
                       ^{:unsynchronized-mutable :int} num-pages
                       ^int target-data-page-size
                       size-estimator
                       ^ByteArrayWriter byte-array-writer
                       ^DataPageWriter page-writer]
  IColumnWriter
  (write-row [this row-values]
    (set! num-values (+ num-values (count row-values)))
    (when (>= num-values next-num-values-for-page-size-check)
      (let [estimated-page-size (.estimatedSize page-writer)]
        (if (>= estimated-page-size target-data-page-size)
          (flush-data-page-writer this)
          (set! next-num-values-for-page-size-check
                (estimation/next-threshold-check num-values estimated-page-size target-data-page-size)))))
    (page/write-all page-writer row-values)
    this)
  (num-pages [_]
    num-pages)
  IDataColumnWriter
  (flush-data-page-writer [_]
    (.write byte-array-writer page-writer)
    (set! num-pages (inc num-pages))
    (set! next-num-values-for-page-size-check (/ (.size page-writer) 2))
    (.reset page-writer))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-values 0)
    (set! num-pages 0)
    (.reset byte-array-writer)
    (.reset page-writer))
  (finish [this]
    (let [estimated-size (+ (.size byte-array-writer) (.estimatedSize page-writer))]
      (flush-data-page-writer this)
      (estimation/update! size-estimator (.size this) estimated-size)))
  (size [this]
    (.size byte-array-writer))
  (estimatedSize [this]
    (estimation/correct size-estimator (+ (.size byte-array-writer) (.estimatedSize page-writer))))
  (writeTo [this baw]
    (.finish this)
    (.write baw byte-array-writer)))

(defn column-writer [target-data-page-size schema-path column-type]
  (let [{:keys [required? value-type encoding compression-type]} column-type
        max-definition-level (count schema-path)]
    (ColumnWriter. 10
                   0
                   0
                   target-data-page-size
                   (estimation/ratio-estimator)
                   (ByteArrayWriter.)
                   (page/data-page-writer max-definition-level required? value-type
                                          encoding compression-type))))

(defprotocol IColumnReader
  (read-column [_ map-fn])
  (stats [_]))

(defn read-column [column-reader] (read-column column-reader identity))

(defn- apply-to-wrapped-value [f wrapped-value]
  (if-let [value (:value wrapped-value)]
    (assoc wrapped-value :value (f value))
    wrapped-value))

(defrecord ColumnStats [num-values num-pages header-bytes repetition-level-bytes
                        definition-level-bytes data-bytes])

(defn- data-page-header->partial-column-stats [data-page-header]
  (ColumnStats. (:num-values data-page-header)
                1
                (page/header-length data-page-header)
                (:repetition-levels-size data-page-header)
                (:definition-levels-size data-page-header)
                (:compressed-data-size data-page-header)))

(defn add-column-stats [column-stats-a column-stats-b]
  (->> (map + (vals column-stats-a) (vals column-stats-b))
       (apply ->ColumnStats)))

(defrecord ColumnReader [^ByteArrayReader byte-array-reader
                         num-data-pages
                         schema-path
                         column-type]
  IColumnReader
  (read-column [_ map-fn]
    (let [{:keys [value-type encoding compression-type]} column-type]
      (->> (page/read-data-pages byte-array-reader num-data-pages (count schema-path) value-type encoding
                                 compression-type)
           (map (partial apply-to-wrapped-value map-fn)))))
  (stats [_]
    (->> (page/read-data-page-headers byte-array-reader (:num-data-pages column-chunk-metadata))
         (map data-page-header->partial-column-stats)
         (reduce add-column-stats))))

(defn column-reader
  [byte-array-reader column-type schema-path num-data-pages]
  (ColumnReader. byte-array-reader
                 num-data-pages
                 schema-path
                 column-type))
