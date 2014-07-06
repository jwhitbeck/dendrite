(ns dendrite.column-chunk
  (:require [dendrite.estimation :as estimation]
            [dendrite.encoding :as encoding]
            [dendrite.metadata :as metadata]
            [dendrite.page :as page]
            [dendrite.stats :as stats])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayWriter ByteArrayReader]
           [dendrite.page DataPageWriter DictionaryPageWriter]
           [java.util HashMap])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defprotocol IColumnChunkWriter
  (write! [this leveled-values])
  (metadata [_]))

(defprotocol IDataColumnChunkWriter
  (flush-data-page-writer! [_]))

(deftype DataColumnChunkWriter [^:unsynchronized-mutable next-num-values-for-page-size-check
                                ^:unsynchronized-mutable num-pages
                                ^int target-data-page-size
                                size-estimator
                                ^ByteArrayWriter byte-array-writer
                                ^DataPageWriter page-writer]
  IColumnChunkWriter
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
    (metadata/map->ColumnChunkMetadata {:bytes (.size this)
                                        :num-data-pages num-pages
                                        :data-page-offset 0
                                        :dictionary-page-offset 0}))
  IDataColumnChunkWriter
  (flush-data-page-writer! [_]
    (when (pos? (page/num-values page-writer))
      (.write byte-array-writer page-writer)
      (set! num-pages (inc num-pages))
      (set! next-num-values-for-page-size-check (int (/ (page/num-values page-writer) 2)))
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

(defn- data-column-chunk-writer [target-data-page-size column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (DataColumnChunkWriter. 10
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

(defprotocol IDictionaryColumChunkWriter
  (value-index [this v]))

(deftype DictionaryColumnChunkWriter [^HashMap reverse-dictionary
                                      ^DictionaryPageWriter dictionary-writer
                                      ^DataColumnChunkWriter data-column-chunk-writer]
  IColumnChunkWriter
  (write! [this leveled-values]
    (->> leveled-values
         (map (fn [leveled-value]
                (let [v (:value leveled-value)]
                  (if (nil? v)
                    leveled-value
                    (assoc leveled-value :value (value-index this v))))))
         (write! data-column-chunk-writer))
    this)
  (metadata [this]
    (metadata/map->ColumnChunkMetadata {:bytes (.size this)
                                        :num-data-pages (-> data-column-chunk-writer metadata :num-data-pages)
                                        :data-page-offset (.size dictionary-writer)
                                        :dictionary-page-offset 0}))
  IDictionaryColumChunkWriter
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
    (.reset data-column-chunk-writer))
  (finish [_]
    (.finish dictionary-writer)
    (.finish data-column-chunk-writer))
  (size [_]
    (+ (.size dictionary-writer) (.size data-column-chunk-writer)))
  (estimatedSize [_]
    (+ (.estimatedSize dictionary-writer) (.estimatedSize data-column-chunk-writer)))
  (writeTo [this baw]
    (.finish this)
    (.write baw dictionary-writer)
    (.write baw data-column-chunk-writer)))

(defn- dictionary-indices-column-spec [column-spec]
  (assoc column-spec
    :type :int
    :encoding :packed-run-length
    :compression :none))

(defn- dictionary-column-chunk-writer [target-data-page-size column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (DictionaryColumnChunkWriter.
     (HashMap.)
     (page/dictionary-page-writer type :plain compression)
     (data-column-chunk-writer target-data-page-size (dictionary-indices-column-spec column-spec)))))

(defn writer [target-data-page-size column-spec]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-chunk-writer target-data-page-size column-spec)
    (data-column-chunk-writer target-data-page-size column-spec)))

(defprotocol IColumnChunkReader
  (stream [_])
  (page-headers [_]))

(defn stats [column-chunk-reader]
  (->> (page-headers column-chunk-reader)
       (map page/stats)
       (stats/pages->column-chunk-stats (:column-spec column-chunk-reader))))

(defn partition-by-record [leveled-values]
  (lazy-seq
   (when-let [coll (seq leveled-values)]
     (let [fst (first coll)
           run (cons fst (take-while (comp not zero? :repetition-level) (next coll)))]
       (cons run (partition-by-record (drop (count run) coll)))))))

(defn read [column-chunk-reader]
  (-> column-chunk-reader stream partition-by-record))

(defn- apply-to-leveled-value [f leveled-value]
  (let [v (:value leveled-value)]
    (if (nil? v)
      leveled-value
      (assoc leveled-value :value (f v)))))

(defrecord DataColumnChunkReader [^ByteArrayReader byte-array-reader
                                  column-chunk-metadata
                                  column-spec
                                  map-fn]
  IColumnChunkReader
  (stream [this]
    (let [{:keys [type encoding compression max-repetition-level max-definition-level]} column-spec
          leveled-values (page/read-data-pages
                           (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                           (:num-data-pages column-chunk-metadata)
                           max-repetition-level
                           max-definition-level
                           type
                           encoding
                           compression)]
      (cond->> leveled-values
               map-fn (map (partial apply-to-leveled-value map-fn)))))
  (page-headers [_]
    (page/read-data-page-headers (.sliceAhead byte-array-reader (:data-page-offset column-chunk-metadata))
                                 (:num-data-pages column-chunk-metadata))))

(defn- data-column-chunk-reader
  [byte-array-reader column-chunk-metadata column-spec map-fn]
  (DataColumnChunkReader. byte-array-reader column-chunk-metadata column-spec map-fn))

(defprotocol IDictionaryColumnChunkReader
  (stream-indices [_])
  (indices-page-headers [_])
  (read-dictionary [_]))

(defrecord DictionaryColumnChunkReader [^ByteArrayReader byte-array-reader
                                        column-chunk-metadata
                                        column-spec
                                        map-fn]
  IColumnChunkReader
  (stream [this]
    (let [dictionary-array (into-array (cond->> (read-dictionary this)
                                                map-fn (map map-fn)))]
      (->> (stream-indices this)
           (map (fn [leveled-value]
                  (let [i (:value leveled-value)]
                    (if (nil? i)
                      leveled-value
                      (assoc leveled-value :value (aget ^objects dictionary-array (int i))))))))))
  (page-headers [this]
    (let [dictionary-page-header (->> column-chunk-metadata
                                      :dictionary-page-offset
                                      (.sliceAhead byte-array-reader)
                                      page/read-dictionary-header)]
      (cons dictionary-page-header (indices-page-headers this))))
  IDictionaryColumnChunkReader
  (read-dictionary [_]
    (page/read-dictionary (.sliceAhead byte-array-reader (:dictionary-page-offset column-chunk-metadata))
                          (:type column-spec)
                          :plain
                          (:compression column-spec)))
  (stream-indices [_]
    (stream (data-column-chunk-reader byte-array-reader
                                      column-chunk-metadata
                                      (dictionary-indices-column-spec column-spec)
                                      nil)))
  (indices-page-headers [_]
    (page-headers (data-column-chunk-reader byte-array-reader
                                            column-chunk-metadata
                                            (dictionary-indices-column-spec column-spec)
                                            nil))))

(defn- dictionary-column-chunk-reader
  [byte-array-reader column-chunk-metadata column-spec map-fn]
  (DictionaryColumnChunkReader. byte-array-reader column-chunk-metadata column-spec map-fn))

(defn reader
  [byte-array-reader column-chunk-metadata column-spec map-fn]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-chunk-reader byte-array-reader column-chunk-metadata column-spec map-fn)
    (data-column-chunk-reader byte-array-reader column-chunk-metadata column-spec map-fn)))

(defn- compute-size-for-column-spec [column-chunk-reader new-colum-spec target-data-page-size]
  (let [w (reduce write! (writer target-data-page-size new-colum-spec) (read column-chunk-reader))]
    (.size (doto ^BufferedByteArrayWriter w .finish))))

(defn find-best-encoding [column-chunk-reader target-data-page-size]
  (let [ct (-> column-chunk-reader :column-spec (assoc :compression :none))
        eligible-encodings (cons :dictionary (encoding/list-encodings-for-type (:type ct)))]
    (->> eligible-encodings
         (reduce (fn [results encoding]
                   (assoc results encoding
                          (compute-size-for-column-spec column-chunk-reader
                                                        (assoc ct :encoding encoding)
                                                        target-data-page-size)))
                 {})
         (sort-by val)
         first
         key)))

(defn find-best-compression
  [column-chunk-reader target-data-page-size candidates-treshold-map & {:keys [encoding]}]
  (let [ct  (cond-> (:column-spec column-chunk-reader)
                    encoding (assoc :encoding encoding))
        compressed-sizes-map
          (->> (assoc candidates-treshold-map :none 1)
               keys
               (reduce (fn [results compression]
                         (assoc results compression
                                (compute-size-for-column-spec column-chunk-reader
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

(defn find-best-column-spec [column-chunk-reader target-data-page-size compression-candidates-treshold-map]
  (let [best-encoding (find-best-encoding column-chunk-reader target-data-page-size)
        best-compression (find-best-compression column-chunk-reader target-data-page-size
                                                compression-candidates-treshold-map
                                                :encoding best-encoding)]
    (assoc (:column-spec column-chunk-reader)
      :encoding best-encoding
      :compression best-compression)))
