(ns dendrite.page
  (:require [dendrite.compression :refer [compressor decompressor-ctor]]
            [dendrite.encoding :refer [levels-encoder levels-decoder encoder decoder-ctor]]
            [dendrite.estimation :as estimation]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.stats :as stats]
            [dendrite.utils :refer [defenum] :as utils])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter Flushable
            Compressor Decompressor Encoder Decoder LeveledValue LeveledValues Dictionary])
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
      :byte-stats (stats/map->ByteStats {:dictionary-header-length (header-length this)
                                         :dictionary-length compressed-data-length})}))
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
    [body-length-estimator
     ^Encoder repetition-level-encoder
     ^Encoder definition-level-encoder
     ^Encoder data-encoder
     ^Compressor data-compressor
     finished?]
  IPageWriter
  (write-value! [this leveled-value]
    (let [v (.value ^LeveledValue leveled-value)]
      (when-not (nil? v)
        (.encode data-encoder v)))
    (when repetition-level-encoder
      (.encode repetition-level-encoder (.repetitionLevel ^LeveledValue leveled-value)))
    (when definition-level-encoder
      (.encode definition-level-encoder (.definitionLevel ^LeveledValue leveled-value)))
    this)
  (num-values [_]
    (if definition-level-encoder
      (.numEncodedValues definition-level-encoder)
      (.numEncodedValues data-encoder)))
  IPageWriterImpl
  (provisional-header [this]
    (map->DataPageHeader
     {:encoded-page-type (page-type->int :data)
      :num-values (num-values this)
      :repetition-levels-length (if repetition-level-encoder (.estimatedLength repetition-level-encoder) 0)
      :definition-levels-length (if definition-level-encoder (.estimatedLength definition-level-encoder) 0)
      :compressed-data-length (.estimatedLength data-encoder)
      :uncompressed-data-length (.estimatedLength data-encoder)}))
  (header [this]
    (map->DataPageHeader
     {:encoded-page-type (page-type->int :data)
      :num-values (num-values this)
      :repetition-levels-length (if repetition-level-encoder (.length repetition-level-encoder) 0)
      :definition-levels-length (if definition-level-encoder (.length definition-level-encoder) 0)
      :compressed-data-length (if data-compressor
                                   (.compressedLength data-compressor)
                                   (.length data-encoder))
      :uncompressed-data-length (.length data-encoder)}))
  BufferedByteArrayWriter
  (reset [_]
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
     {:body-length-estimator (estimation/ratio-estimator)
      :repetition-level-encoder (when (pos? max-repetition-level) (levels-encoder max-repetition-level))
      :definition-level-encoder (when (pos? max-definition-level) (levels-encoder max-definition-level))
      :data-encoder (encoder value-type encoding)
      :data-compressor (compressor compression)
      :finished? (atom false)}))

(defrecord DictionaryPageWriter [body-length-estimator
                                 ^Encoder data-encoder
                                 ^Compressor data-compressor
                                 finished?]
  IPageWriter
  (write-value! [this value]
    (.encode data-encoder value)
    this)
  (num-values [_] (.numEncodedValues data-encoder))
  IPageWriterImpl
  (provisional-header [this]
    (map->DictionaryPageHeader
     {:encoded-page-type (page-type->int :dictionary)
      :num-values (num-values this)
      :compressed-data-length (.estimatedLength data-encoder)
      :uncompressed-data-length (.estimatedLength data-encoder)}))
  (header [this]
    (map->DictionaryPageHeader
     {:encoded-page-type (page-type->int :dictionary)
      :num-values (num-values this)
      :compressed-data-length (if data-compressor
                                   (.compressedLength data-compressor)
                                   (.length data-encoder))
      :uncompressed-data-length (.length data-encoder)}))
  BufferedByteArrayWriter
  (reset [_]
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
   {:body-length-estimator (estimation/ratio-estimator)
    :data-encoder (encoder value-type encoding)
    :data-compressor (compressor compression)
    :finished? (atom false)}))


(defprotocol IDataPageReader
  (read [_] [_ map-fn])
  (skip [_]))

(defrecord DataPageReader [^ByteArrayReader byte-array-reader
                           max-repetition-level
                           max-definition-level
                           data-decoder-ctor
                           decompressor-ctor
                           header]
  IDataPageReader
  (read [this]
    (read this nil))
  (read [this map-fn]
    (let [repetition-levels-decoder (when (has-repetition-levels? header)
                                      (-> byte-array-reader
                                          (.sliceAhead (byte-offset-repetition-levels header))
                                          (levels-decoder max-repetition-level)))
          definition-levels-decoder (when (has-definition-levels? header)
                                      (-> byte-array-reader
                                          (.sliceAhead (byte-offset-definition-levels header))
                                          (levels-decoder max-definition-level)))
          data-bytes-reader (.sliceAhead byte-array-reader (byte-offset-body header))
          data-bytes-reader (if-let [decompressor (decompressor-ctor)]
                              (.decompress ^Decompressor decompressor
                                           data-bytes-reader
                                           (:compressed-data-length header)
                                           (:uncompressed-data-length header))
                              data-bytes-reader)
          data-decoder (data-decoder-ctor data-bytes-reader)]
      (LeveledValues/assemble repetition-levels-decoder
                              definition-levels-decoder
                              data-decoder
                              max-definition-level
                              map-fn)))
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
   value-type encoding compression map-fn]
  (->> (data-page-readers byte-array-reader num-data-pages max-repetition-level max-definition-level
                          value-type encoding compression)
       (utils/pmap-1 #(read % map-fn))
       utils/flatten-1))

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

(defprotocol IDictionaryPageReader
  (read-array [_] [_ map-fn]))

(defrecord DictionaryPageReader [^ByteArrayReader byte-array-reader
                                 data-decoder-ctor
                                 decompressor-ctor
                                 header]
  IDictionaryPageReader
  (read-array [this]
    (read-array this nil))
  (read-array [_ map-fn]
    (let [data-bytes-reader (-> byte-array-reader
                                (.sliceAhead (byte-offset-body header)))
          data-bytes-reader (if-let [decompressor (decompressor-ctor)]
                              (.decompress ^Decompressor decompressor
                                           data-bytes-reader
                                           (:compressed-data-length header)
                                           (:uncompressed-data-length header))
                              data-bytes-reader)]
      (Dictionary/read (data-decoder-ctor data-bytes-reader) map-fn))))

(defn dictionary-page-reader
  [^ByteArrayReader byte-array-reader value-type encoding compression]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-dictionary-page-type bar)]
    (map->DictionaryPageReader
     {:byte-array-reader bar
      :data-decoder-ctor (decoder-ctor value-type encoding)
      :decompressor-ctor (decompressor-ctor compression)
      :header (read-dictionary-page-header bar page-type)})))

(defn read-dictionary [^ByteArrayReader byte-array-reader value-type encoding compression map-fn]
  (read-array (dictionary-page-reader byte-array-reader value-type encoding compression) map-fn))

(defn read-dictionary-header [^ByteArrayReader byte-array-reader]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-dictionary-page-type bar)]
    (read-dictionary-page-header bar page-type)))
