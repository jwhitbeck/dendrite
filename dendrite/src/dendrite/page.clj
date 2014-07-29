(ns dendrite.page
  (:require [dendrite.compression :refer [compressor decompressor-ctor]]
            [dendrite.encoding :refer [encode-value! decode levels-encoder levels-decoder encoder
                                       decoder-ctor]]
            [dendrite.estimation :as estimation]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.stats :as stats]
            [dendrite.utils :refer [defenum]])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter Flushable
            Compressor Decompressor])
  (:refer-clojure :exclude [read type]))

(set! *warn-on-reflection* true)

(defenum page-type [:data :dictionary])

(defn- read-next-page-type [^ByteArrayReader bar]
  (-> bar .readUInt int->page-type))

(defn- read-next-data-page-type [^ByteArrayReader bar]
  (let [data-page-type (read-next-page-type bar)]
    (when-not (= data-page-type :data)
      (throw
       (IllegalArgumentException. (format "Page type %s is not a supported data page type" data-page-type))))
    data-page-type))

(defn- read-next-dictionary-page-type [^ByteArrayReader bar]
  (let [dictionary-page-type (read-next-page-type bar)]
    (when-not (= dictionary-page-type :dictionary)
      (throw
       (IllegalArgumentException.
        (format "Page type %s is not a supported dictionary page type" dictionary-page-type))))
    dictionary-page-type))

(defprotocol IPageHeader
  (type [this])
  (header-length [this])
  (body-length [this])
  (byte-offset-body [this])
  (stats [_]))

(defprotocol IDataPageHeader
  (has-repetition-levels? [this])
  (has-definition-levels? [this])
  (byte-offset-repetition-levels [this])
  (byte-offset-definition-levels [this]))

(defn- uints32-encoded-length [coll]
  (reduce #(+ %1 (ByteArrayWriter/getNumUIntBytes %2)) 0 coll))

(defn- encode-uints32 [^ByteArrayWriter byte-array-writer coll]
  (reduce #(doto ^ByteArrayWriter %1 (.writeUInt %2)) byte-array-writer coll))

(defrecord DataPageHeader [encoded-page-type
                           num-values
                           repetition-levels-length
                           definition-levels-length
                           compressed-data-length
                           uncompressed-data-length]
  IPageHeader
  (type [this]
    (int->page-type encoded-page-type))
  (header-length [this]
    (uints32-encoded-length (vals this)))
  (body-length [_]
    (+ repetition-levels-length definition-levels-length compressed-data-length))
  (byte-offset-body [_]
    (+ repetition-levels-length definition-levels-length))
  (stats [this]
    (stats/map->PageStats
     {:num-values num-values
      :length (+ (header-length this) (body-length this))
      :byte-stats (stats/map->ByteStats {:header-length (header-length this)
                                         :repetition-levels-length repetition-levels-length
                                         :definition-levels-length definition-levels-length
                                         :data-length compressed-data-length})}))
  IDataPageHeader
  (has-repetition-levels? [_]
    (pos? repetition-levels-length))
  (has-definition-levels? [_]
    (pos? definition-levels-length))
  (byte-offset-repetition-levels [_]
    0)
  (byte-offset-definition-levels [_]
    repetition-levels-length)
  Flushable
  (flush [this byte-array-writer]
    (encode-uints32 byte-array-writer (vals this))))

(defn- read-data-page-header [^ByteArrayReader bar data-page-type]
  (map->DataPageHeader
   {:encoded-page-type (page-type->int data-page-type)
    :num-values (.readUInt bar)
    :repetition-levels-length (.readUInt bar)
    :definition-levels-length (.readUInt bar)
    :compressed-data-length (.readUInt bar)
    :uncompressed-data-length (.readUInt bar)}))

(defrecord DictionaryPageHeader [encoded-page-type
                                 num-values
                                 compressed-data-length
                                 uncompressed-data-length]
  IPageHeader
  (type [this]
    (int->page-type encoded-page-type))
  (header-length [this]
    (uints32-encoded-length (vals this)))
  (body-length [_]
    compressed-data-length)
  (byte-offset-body [_]
    0)
  (stats [this]
    (stats/map->PageStats
     {:num-values num-values
      :length (+ (header-length this) (body-length this))
      :byte-stats (stats/map->ByteStats {:dictionary-header-bytes (header-length this)
                                         :dictionary-bytes compressed-data-length})}))
  Flushable
  (flush [this byte-array-writer]
    (encode-uints32 byte-array-writer (vals this))))

(defn- read-dictionary-page-header [^ByteArrayReader bar dictionary-page-type]
  (map->DictionaryPageHeader
   {:encoded-page-type (page-type->int dictionary-page-type)
    :num-values (.readUInt bar)
    :compressed-data-length (.readUInt bar)
    :uncompressed-data-length (.readUInt bar)}))

(defprotocol IPageWriter
  (write-value! [this value])
  (num-values [this]))

(defprotocol IPageWriterImpl
  (provisional-header [this])
  (header [this]))

(defn write! [page-writer values]
  (reduce write-value! page-writer values))

(defrecord DataPageWriter
    [num-values
     body-length-estimator
     ^BufferedByteArrayWriter repetition-level-encoder
     ^BufferedByteArrayWriter definition-level-encoder
     ^BufferedByteArrayWriter data-encoder
     ^Compressor data-compressor
     finished?]
  IPageWriter
  (write-value! [this leveled-value]
    (let [v (:value leveled-value)]
      (when-not (nil? v)
        (encode-value! data-encoder v)))
    (when repetition-level-encoder
      (encode-value! repetition-level-encoder (:repetition-level leveled-value)))
    (when definition-level-encoder
      (encode-value! definition-level-encoder (:definition-level leveled-value)))
    (swap! num-values inc)
    this)
  (num-values [_] @num-values)
  IPageWriterImpl
  (provisional-header [_]
    (map->DataPageHeader
     {:encoded-page-type (page-type->int :data)
      :num-values @num-values
      :repetition-levels-length (if repetition-level-encoder (.estimatedLength repetition-level-encoder) 0)
      :definition-levels-length (if definition-level-encoder (.estimatedLength definition-level-encoder) 0)
      :compressed-data-length (.estimatedLength data-encoder)
      :uncompressed-data-length (.estimatedLength data-encoder)}))
  (header [_]
    (map->DataPageHeader
     {:encoded-page-type (page-type->int :data)
      :num-values @num-values
      :repetition-levels-length (if repetition-level-encoder (.length repetition-level-encoder) 0)
      :definition-levels-length (if definition-level-encoder (.length definition-level-encoder) 0)
      :compressed-data-length (if data-compressor
                                   (.compressedLength data-compressor)
                                   (.length data-encoder))
      :uncompressed-data-length (.length data-encoder)}))
  BufferedByteArrayWriter
  (reset [_]
    (reset! num-values 0)
    (reset! finished? false)
    (when repetition-level-encoder
      (.reset repetition-level-encoder))
    (when definition-level-encoder
      (.reset definition-level-encoder))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not @finished?
      (let [estimated-body-length (-> this provisional-header body-length)]
        (when repetition-level-encoder
          (.finish repetition-level-encoder))
        (when definition-level-encoder
          (.finish definition-level-encoder))
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (estimation/update! body-length-estimator
                            (-> this header body-length) estimated-body-length))
      (reset! finished? true)))
  (length [this]
    (let [h (header this)]
      (+ (header-length h) (body-length h))))
  (estimatedLength [this]
    (let [provisional-header (provisional-header this)]
      (+ (header-length provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (flush [this byte-array-writer]
    (.finish this)
    (.write byte-array-writer ^DataPageHeader (header this))
    (when repetition-level-encoder
      (.write byte-array-writer repetition-level-encoder))
    (when definition-level-encoder
      (.write byte-array-writer definition-level-encoder))
    (.write byte-array-writer (if data-compressor
                                ^Flushable data-compressor
                                ^Flushable data-encoder))))

(defn data-page-writer [max-repetition-level max-definition-level value-type encoding compression]
  (map->DataPageWriter
     {:num-values (atom 0)
      :body-length-estimator (estimation/ratio-estimator)
      :repetition-level-encoder (when (pos? max-repetition-level) (levels-encoder max-repetition-level))
      :definition-level-encoder (when (pos? max-definition-level) (levels-encoder max-definition-level))
      :data-encoder (encoder value-type encoding)
      :data-compressor (compressor compression)
      :finished? (atom false)}))

(defrecord DictionaryPageWriter [num-values
                                 body-length-estimator
                                 ^BufferedByteArrayWriter data-encoder
                                 ^Compressor data-compressor
                                 finished?]
  IPageWriter
  (write-value! [this value]
    (encode-value! data-encoder value)
    (swap! num-values inc)
    this)
  (num-values [_] @num-values)
  IPageWriterImpl
  (provisional-header [_]
    (map->DictionaryPageHeader
     {:encoded-page-type (page-type->int :dictionary)
      :num-values @num-values
      :compressed-data-length (.estimatedLength data-encoder)
      :uncompressed-data-length (.estimatedLength data-encoder)}))
  (header [_]
    (map->DictionaryPageHeader
     {:encoded-page-type (page-type->int :dictionary)
      :num-values @num-values
      :compressed-data-length (if data-compressor
                                   (.compressedLength data-compressor)
                                   (.length data-encoder))
      :uncompressed-data-length (.length data-encoder)}))
  BufferedByteArrayWriter
  (reset [_]
    (reset! num-values 0)
    (reset! finished? false)
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not @finished?
      (let [estimated-body-length (-> this provisional-header body-length)]
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (estimation/update! body-length-estimator
                            (-> this header body-length) estimated-body-length))
      (reset! finished? true)))
  (length [this]
    (let [h (header this)]
      (+ (header-length h) (body-length h))))
  (estimatedLength [this]
    (let [provisional-header (provisional-header this)]
      (+ (header-length provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (flush [this byte-array-writer]
    (.finish this)
    (.write byte-array-writer ^DataPageHeader (header this))
    (.write byte-array-writer (if data-compressor
                                ^Flushable data-compressor
                                ^Flushable data-encoder))))

(defn dictionary-page-writer [value-type encoding compression]
  (map->DictionaryPageWriter
   {:num-values (atom 0)
    :body-length-estimator (estimation/ratio-estimator)
    :data-encoder (encoder value-type encoding)
    :data-compressor (compressor compression)
    :finished? (atom false)}))

(defprotocol IPageReader
  (read [_]))

(defprotocol IDataPageReader
  (read-repetition-levels [_])
  (read-definition-levels [_])
  (read-data [_])
  (skip [_]))

(defrecord DataPageReader [^ByteArrayReader byte-array-reader
                           max-repetition-level
                           max-definition-level
                           data-decoder-ctor
                           decompressor-ctor
                           header]
  IPageReader
  (read [this]
    (letfn [(lazy-read-values [repetition-levels-seq definition-levels-seq values-seq]
              (lazy-seq
               (let [nil-value? (< (first definition-levels-seq) max-definition-level)]
                 (cons (->LeveledValue (first repetition-levels-seq)
                                       (first definition-levels-seq)
                                       (if nil-value? nil (first values-seq)))
                       (lazy-read-values (rest repetition-levels-seq)
                                         (rest definition-levels-seq)
                                         (if nil-value? values-seq (rest values-seq)))))))]
      (take (:num-values header)
            (lazy-read-values (read-repetition-levels this)
                              (read-definition-levels this)
                              (read-data this)))))
  IDataPageReader
  (read-repetition-levels [_]
    (if (has-repetition-levels? header)
      (-> byte-array-reader
          (.sliceAhead (byte-offset-repetition-levels header))
          (levels-decoder max-repetition-level)
          decode)
      (repeat 0)))
  (read-definition-levels [_]
    (if (has-definition-levels? header)
      (-> byte-array-reader
          (.sliceAhead (byte-offset-definition-levels header))
          (levels-decoder max-definition-level)
          decode)
      (repeat max-definition-level)))
  (read-data [_]
    (let [data-bytes-reader (-> byte-array-reader
                                (.sliceAhead (byte-offset-body header)))
          data-bytes-reader (if-let [decompressor (decompressor-ctor)]
                              (.decompress ^Decompressor decompressor
                                           data-bytes-reader
                                           (:compressed-data-length header)
                                           (:uncompressed-data-length header))
                              data-bytes-reader)]
      (decode (data-decoder-ctor data-bytes-reader))))
  (skip [_]
    (.sliceAhead byte-array-reader (body-length header))))

(defn data-page-reader
  [^ByteArrayReader byte-array-reader max-repetition-level max-definition-level value-type encoding
   compression]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-data-page-type bar)]
    (map->DataPageReader
     {:byte-array-reader bar
      :max-repetition-level max-repetition-level
      :max-definition-level max-definition-level
      :data-decoder-ctor (decoder-ctor value-type encoding)
      :decompressor-ctor (decompressor-ctor compression)
      :header (read-data-page-header bar page-type)})))

(defn data-page-readers
  [^ByteArrayReader byte-array-reader num-data-pages max-repetition-level max-definition-level
   value-type encoding compression]
  (let [num-data-pages (int num-data-pages)]
    (lazy-seq
     (when (pos? num-data-pages)
       (let [next-data-page-reader (data-page-reader byte-array-reader max-repetition-level
                                                     max-definition-level value-type encoding compression)]
         (cons next-data-page-reader
               (data-page-readers (skip next-data-page-reader) (dec num-data-pages) max-repetition-level
                                  max-definition-level value-type encoding compression)))))))

(defn read-data-pages
  [^ByteArrayReader byte-array-reader num-data-pages max-repetition-level max-definition-level
   value-type encoding compression]
  (->> (data-page-readers byte-array-reader num-data-pages max-repetition-level max-definition-level
                          value-type encoding compression)
       (mapcat read)))

(defn read-data-page-headers
  [^ByteArrayReader byte-array-reader num-data-pages]
  (let [num-data-pages (int num-data-pages)
        bar (.slice byte-array-reader)]
    (lazy-seq
     (when (pos? num-data-pages)
       (let [page-type (read-next-data-page-type bar)
             next-header (read-data-page-header bar page-type)
             next-byte-array-reader (.sliceAhead bar (body-length next-header))]
         (cons next-header (read-data-page-headers next-byte-array-reader (dec num-data-pages))))))))

(defrecord DictionaryPageReader [^ByteArrayReader byte-array-reader
                                 data-decoder-ctor
                                 decompressor-ctor
                                 header]
  IPageReader
  (read [_]
    (let [data-bytes-reader (-> byte-array-reader
                                (.sliceAhead (byte-offset-body header)))
          data-bytes-reader (if-let [decompressor (decompressor-ctor)]
                              (.decompress ^Decompressor decompressor
                                           data-bytes-reader
                                           (:compressed-data-length header)
                                           (:uncompressed-data-length header))
                              data-bytes-reader)]
      (->> (data-decoder-ctor data-bytes-reader) decode (take (:num-values header))))))

(defn dictionary-page-reader
  [^ByteArrayReader byte-array-reader value-type encoding compression]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-dictionary-page-type bar)]
    (map->DictionaryPageReader
     {:byte-array-reader bar
      :data-decoder-ctor (decoder-ctor value-type encoding)
      :decompressor-ctor (decompressor-ctor compression)
      :header (read-dictionary-page-header bar page-type)})))

(defn read-dictionary [^ByteArrayReader byte-array-reader value-type encoding compression]
  (read (dictionary-page-reader byte-array-reader value-type encoding compression)))

(defn read-dictionary-header [^ByteArrayReader byte-array-reader]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-dictionary-page-type bar)]
    (read-dictionary-page-header bar page-type)))
