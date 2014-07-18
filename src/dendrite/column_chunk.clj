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

(deftype DataColumnChunkWriter [^:unsynchronized-mutable next-num-values-for-page-length-check
                                ^:unsynchronized-mutable num-pages
                                target-data-page-length
                                length-estimator
                                ^ByteArrayWriter byte-array-writer
                                ^DataPageWriter page-writer]
  IColumnChunkWriter
  (write! [this leveled-values]
    (when (>= (page/num-values page-writer) next-num-values-for-page-length-check)
      (let [estimated-page-length (.estimatedLength page-writer)]
        (if (>= estimated-page-length target-data-page-length)
          (flush-data-page-writer! this)
          (set! next-num-values-for-page-length-check
                (estimation/next-threshold-check (page/num-values page-writer) estimated-page-length
                                                 target-data-page-length)))))
    (page/write! page-writer leveled-values)
    this)
  (metadata [this]
    (metadata/map->ColumnChunkMetadata {:num-bytes (.length this)
                                        :num-data-pages num-pages
                                        :data-page-offset 0
                                        :dictionary-page-offset 0}))
  IDataColumnChunkWriter
  (flush-data-page-writer! [_]
    (when (pos? (page/num-values page-writer))
      (.write byte-array-writer page-writer)
      (set! num-pages (inc num-pages))
      (set! next-num-values-for-page-length-check (int (/ (page/num-values page-writer) 2)))
      (.reset page-writer)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-pages 0)
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
  (writeTo [this baw]
    (.finish this)
    (.write baw byte-array-writer)))

(defn- data-column-chunk-writer [target-data-page-length column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (DataColumnChunkWriter. 10
                            0
                            target-data-page-length
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
    (metadata/map->ColumnChunkMetadata {:num-bytes (.length this)
                                        :num-data-pages (-> data-column-chunk-writer metadata :num-data-pages)
                                        :data-page-offset (.length dictionary-writer)
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
  (length [_]
    (+ (.length dictionary-writer) (.length data-column-chunk-writer)))
  (estimatedLength [_]
    (+ (.estimatedLength dictionary-writer) (.estimatedLength data-column-chunk-writer)))
  (writeTo [this baw]
    (.finish this)
    (.write baw dictionary-writer)
    (.write baw data-column-chunk-writer)))

(defn- dictionary-indices-column-spec [column-spec]
  (-> column-spec
      (assoc :type :int
             :encoding :packed-run-length
             :compression :none)
      (dissoc :map-fn)))

(defn- dictionary-column-chunk-writer [target-data-page-length column-spec]
  (let [{:keys [max-repetition-level max-definition-level type encoding compression]} column-spec]
    (DictionaryColumnChunkWriter.
     (HashMap.)
     (page/dictionary-page-writer type :plain compression)
     (data-column-chunk-writer target-data-page-length (dictionary-indices-column-spec column-spec)))))

(defn writer [target-data-page-length column-spec]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-chunk-writer target-data-page-length column-spec)
    (data-column-chunk-writer target-data-page-length column-spec)))

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
                                  column-spec]
  IColumnChunkReader
  (stream [this]
    (let [map-fn (:map-fn column-spec)
          {:keys [type encoding compression max-repetition-level max-definition-level]} column-spec
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
  [byte-array-reader column-chunk-metadata column-spec]
  (->DataColumnChunkReader byte-array-reader column-chunk-metadata column-spec))

(defprotocol IDictionaryColumnChunkReader
  (stream-indices [_])
  (indices-page-headers [_])
  (read-dictionary [_]))

(defrecord DictionaryColumnChunkReader [^ByteArrayReader byte-array-reader
                                        column-chunk-metadata
                                        column-spec]
  IColumnChunkReader
  (stream [this]
    (let [map-fn (:map-fn column-spec)
          dictionary-array (into-array (cond->> (read-dictionary this)
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
                                      (dictionary-indices-column-spec column-spec))))
  (indices-page-headers [_]
    (page-headers (data-column-chunk-reader byte-array-reader
                                            column-chunk-metadata
                                            (dictionary-indices-column-spec column-spec)))))

(defn- dictionary-column-chunk-reader
  [byte-array-reader column-chunk-metadata column-spec]
  (->DictionaryColumnChunkReader byte-array-reader column-chunk-metadata column-spec))

(defn reader
  [byte-array-reader column-chunk-metadata column-spec]
  (if (= :dictionary (:encoding column-spec))
    (dictionary-column-chunk-reader byte-array-reader column-chunk-metadata column-spec)
    (data-column-chunk-reader byte-array-reader column-chunk-metadata column-spec)))

(defn- compute-length-for-column-spec [column-chunk-reader new-colum-spec target-data-page-length]
  (let [w (reduce write! (writer target-data-page-length new-colum-spec) (read column-chunk-reader))]
    (.length (doto ^BufferedByteArrayWriter w .finish))))

(defn find-best-encoding [column-chunk-reader target-data-page-length]
  (let [ct (-> column-chunk-reader :column-spec (assoc :compression :none))
        eligible-encodings (cons :dictionary (encoding/list-encodings-for-type (:type ct)))]
    (->> eligible-encodings
         (reduce (fn [results encoding]
                   (assoc results encoding
                          (compute-length-for-column-spec column-chunk-reader
                                                          (assoc ct :encoding encoding)
                                                          target-data-page-length)))
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
                                                                target-data-page-length)))
                       {}))
        no-compression-length (:none compressed-lengths-map)
        best-compression
          (->> (dissoc compressed-lengths-map :none)
               (filter (fn [[compression length]]
                         (<= (/ length no-compression-length) (get candidates-treshold-map compression))))
               (sort-by val)
               ffirst)]
    (or best-compression :none)))

(defn find-best-column-spec [column-chunk-reader target-data-page-length compression-candidates-treshold-map]
  (let [best-encoding (find-best-encoding column-chunk-reader target-data-page-length)
        best-compression (find-best-compression column-chunk-reader target-data-page-length
                                                compression-candidates-treshold-map
                                                :encoding best-encoding)]
    (assoc (:column-spec column-chunk-reader)
      :encoding best-encoding
      :compression best-compression)))
