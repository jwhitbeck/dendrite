(ns dendrite.column
  (:require [dendrite.estimation :as estimation]
            [dendrite.encoding :as encoding]
            [dendrite.metadata :as metadata]
            [dendrite.page :as page])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader]
           [dendrite.page DataPageWriter DictionaryPageWriter]
           [java.util HashMap])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IColumnWriter
  (write! [this leveled-values])
  (metadata [_]))

(defprotocol IDataColumnWriter
  (flush-data-page-writer! [_]))

(deftype DataColumnWriter [^:unsynchronized-mutable next-num-values-for-page-size-check
                           ^:unsynchronized-mutable num-pages
                           ^int target-data-page-size
                           size-estimator
                           ^ByteArrayWriter byte-array-writer
                           ^DataPageWriter page-writer]
  IColumnWriter
  (write! [this leveled-values]
    (when (>= (page/num-values page-writer) next-num-values-for-page-size-check)
      (let [estimated-page-size (.estimatedSize page-writer)]
        (if (>= estimated-page-size target-data-page-size)
          (flush-data-page-writer! this)
          (set! next-num-values-for-page-size-check
                (estimation/next-threshold-check (page/num-values page-writer) estimated-page-size
                                                 target-data-page-size)))))
    (page/write! page-writer leveled-values)
    this)
  (metadata [this]
    (metadata/column-chunk-metadata (.size this) num-pages 0 0))
  IDataColumnWriter
  (flush-data-page-writer! [_]
    (when (pos? (page/num-values page-writer))
      (.write byte-array-writer page-writer)
      (set! num-pages (inc num-pages))
      (set! next-num-values-for-page-size-check (int (/ (.size page-writer) 2)))
      (.reset page-writer)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-pages 0)
    (.reset byte-array-writer)
    (.reset page-writer))
  (finish [this]
    (when (pos? (page/num-values page-writer))
      (let [estimated-size (+ (.size byte-array-writer) (.estimatedSize page-writer))]
        (flush-data-page-writer! this)
        (estimation/update! size-estimator (.size this) estimated-size))))
  (size [_]
    (.size byte-array-writer))
  (estimatedSize [this]
    (estimation/correct size-estimator (+ (.size byte-array-writer) (.estimatedSize page-writer))))
  (writeTo [this baw]
    (.finish this)
    (.write baw byte-array-writer)))

(defn- data-column-writer [target-data-page-size column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (DataColumnWriter. 10
                       0
                       target-data-page-size
                       (estimation/ratio-estimator)
                       (ByteArrayWriter.)
                       (page/data-page-writer max-repetition-level max-definition-level type
                                              encoding compression))))

(defn- bytes? [x] (instance? (Class/forName "[B") x))

(defn- keyable [x]
  (if (bytes? x)
    (vec x)
    x))

(defprotocol IDictionaryColumWriter
  (value-index [this v]))

(deftype DictionaryColumnWriter [^HashMap reverse-dictionary
                                 ^DictionaryPageWriter dictionary-writer
                                 ^DataColumnWriter data-column-writer]
  IColumnWriter
  (write! [this leveled-values]
    (->> leveled-values
         (map (fn [leveled-value]
                (let [v (:value leveled-value)]
                  (if (nil? v)
                    leveled-value
                    (assoc leveled-value :value (value-index this v))))))
         (write! data-column-writer))
    this)
  (metadata [this]
    (metadata/column-chunk-metadata (.size this) (-> data-column-writer metadata :num-data-pages)
                                    (.size dictionary-writer) 0))
  IDictionaryColumWriter
  (value-index [_ v]
    (let [k (keyable v)]
      (or (.get reverse-dictionary k)
          (do (let [idx (.size reverse-dictionary)]
                (.put reverse-dictionary k idx)
                (page/write-value! dictionary-writer v)
                idx)))))
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

(defn- dictionary-indices-column-spec [column-spec]
  (assoc column-spec
    :type :int
    :encoding :packed-run-length
    :compression :none))

(defn- dictionary-column-writer [target-data-page-size column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (DictionaryColumnWriter.
     (HashMap.)
     (page/dictionary-page-writer type :plain compression)
     (data-column-writer target-data-page-size (dictionary-indices-column-spec column-spec)))))

(defn column-writer [target-data-page-size column-spec]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-writer target-data-page-size column-spec)
    (data-column-writer target-data-page-size column-spec)))

(defprotocol IColumnReader
  (read [_])
  (stats [_]))

(defn- apply-to-leveled-value [f leveled-value]
  (let [v (:value leveled-value)]
    (if (nil? v)
      leveled-value
      (assoc leveled-value :value (f v)))))

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
                             column-spec
                             map-fn]
  IColumnReader
  (read [this]
    (let [{:keys [type encoding compression max-definition-level]} column-spec
          leveled-values (page/read-data-pages
                           (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                           (:num-data-pages column-chunk-metadata)
                           max-definition-level
                           type
                           encoding
                           compression)]
      (cond->> leveled-values
               map-fn (map (partial apply-to-leveled-value map-fn)))))
  (stats [_]
    (->> (page/read-data-page-headers (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                                      (:num-data-pages column-chunk-metadata))
         (map data-page-header->partial-column-stats)
         (reduce add-column-stats))))

(defn- data-column-reader
  [byte-array-reader column-chunk-metadata column-spec map-fn]
  (DataColumnReader. byte-array-reader column-chunk-metadata column-spec map-fn))

(defprotocol IDictionaryColumnReader
  (read-indices [_])
  (read-dictionary [_])
  (indices-stats [_]))

(defrecord DictionaryColumnReader [^ByteArrayReader byte-array-reader
                                   column-chunk-metadata
                                   column-spec
                                   map-fn]
  IColumnReader
  (read [this]
    (let [dictionary-array (into-array (cond->> (read-dictionary this)
                                                map-fn (map map-fn)))]
      (->> (read-indices this)
           (map (fn [leveled-value]
                  (let [i (:value leveled-value)]
                    (if (nil? i)
                      leveled-value
                      (assoc leveled-value :value (aget ^objects dictionary-array (int i)))))))
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
                          (:type column-spec)
                          :plain
                          (:compression column-spec)))
  (read-indices [_]
    (read (data-column-reader byte-array-reader
                              column-chunk-metadata
                              (dictionary-indices-column-spec column-spec)
                              nil)))
  (indices-stats [_]
    (stats (data-column-reader byte-array-reader
                               column-chunk-metadata
                               (dictionary-indices-column-spec column-spec)
                               nil))))

(defn- dictionary-column-reader
  [byte-array-reader column-chunk-metadata column-spec map-fn]
  (DictionaryColumnReader. byte-array-reader column-chunk-metadata column-spec map-fn))

(defn column-reader
  [byte-array-reader column-chunk-metadata column-spec map-fn]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-reader byte-array-reader column-chunk-metadata column-spec map-fn)
    (data-column-reader byte-array-reader column-chunk-metadata column-spec map-fn)))

(defn- compute-size-for-column-spec [column-reader new-colum-spec target-data-page-size]
  (let [writer (-> (column-writer target-data-page-size new-colum-spec)
                   (write! (read column-reader)))]
    (-> (doto ^BufferedByteArrayWriter writer .finish)
        .size)))

(defn find-best-encoding [column-reader target-data-page-size]
  (let [ct (-> column-reader :column-spec (assoc :compression :none))
        eligible-encodings (cons :dictionary (encoding/list-encodings-for-type (:type ct)))]
    (->> eligible-encodings
         (reduce (fn [results encoding]
                   (assoc results encoding
                          (compute-size-for-column-spec column-reader
                                                        (assoc ct :encoding encoding)
                                                        target-data-page-size)))
                 {})
         (sort-by val)
         first
         key)))

(defn find-best-compression
  [column-reader target-data-page-size candidates-treshold-map & {:keys [encoding]}]
  (let [ct  (cond-> (:column-spec column-reader)
                    encoding (assoc :encoding encoding))
        compressed-sizes-map
          (->> (assoc candidates-treshold-map :none 1)
               keys
               (reduce (fn [results compression]
                         (assoc results compression
                                (compute-size-for-column-spec column-reader
                                                              (assoc ct :compression compression)
                                                              target-data-page-size)))
                       {}))
        no-compression-size (:none compressed-sizes-map)
        best-compression
          (->> (dissoc compressed-sizes-map :none)
               (filter (fn [[compression size]]
                         (<= (/ size no-compression-size) (get candidates-treshold-map compression))))
               (sort-by val)
               ffirst)]
    (or best-compression :none)))

(defn find-best-column-spec [column-reader target-data-page-size compression-candidates-treshold-map]
  (let [best-encoding (find-best-encoding column-reader target-data-page-size)
        best-compression (find-best-compression column-reader target-data-page-size
                                                          compression-candidates-treshold-map
                                                          :encoding best-encoding)]
    (assoc (:column-spec column-reader)
      :encoding best-encoding
      :compression best-compression)))
