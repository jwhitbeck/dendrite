(ns dendrite.page
  (:require [dendrite.core :refer [leveled-value]]
            [dendrite.compression :refer [compressor decompressor-ctor]]
            [dendrite.encoding :refer [encode decode-values levels-encoder levels-decoder encoder
                                       decoder-ctor]]
            [dendrite.estimation :as estimation])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter ByteArrayWritable
            Compressor Decompressor])
  (:refer-clojure :exclude [read type]))

(set! *warn-on-reflection* true)

(def ^:private page-types [:data :dictionary])

(def ^:private page-type-encodings
  (reduce-kv (fn [m data-page-idx data-page-kw] (assoc m data-page-kw data-page-idx)) {} page-types))

(defn- encode-page-type [page-type] (get page-type-encodings page-type))

(defn- decode-page-type [encoded-page-type] (get page-types encoded-page-type))

(defn- read-next-page-type [^ByteArrayReader bar]
  (-> bar .readUInt decode-page-type))

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
  (byte-offset-body [this]))

(defprotocol IDataPageHeader
  (has-repetition-levels? [this])
  (has-definition-levels? [this])
  (byte-offset-repetition-levels [this])
  (byte-offset-definition-levels [this]))

(defn- uints32-encoded-size [coll]
  (reduce #(+ %1 (ByteArrayWriter/getNumUIntBytes %2)) 0 coll))

(defn- encode-uints32 [^ByteArrayWriter byte-array-writer coll]
  (reduce #(doto ^ByteArrayWriter %1 (.writeUInt %2)) byte-array-writer coll))

(defrecord DataPageHeader [^int encoded-page-type
                           ^int num-values
                           ^int repetition-levels-size
                           ^int definition-levels-size
                           ^int compressed-data-size
                           ^int uncompressed-data-size]
  IPageHeader
  (type [this]
    (decode-page-type encoded-page-type))
  (header-length [this]
    (uints32-encoded-size (vals this)))
  (body-length [_]
    (+ repetition-levels-size definition-levels-size compressed-data-size))
  (byte-offset-body [_]
    (+ repetition-levels-size definition-levels-size))
  IDataPageHeader
  (has-repetition-levels? [_]
    (pos? repetition-levels-size))
  (has-definition-levels? [_]
    (pos? definition-levels-size))
  (byte-offset-repetition-levels [_]
    0)
  (byte-offset-definition-levels [_]
    repetition-levels-size)
  ByteArrayWritable
  (writeTo [this byte-array-writer]
    (encode-uints32 byte-array-writer (vals this))))

(defn- read-data-page-header [^ByteArrayReader bar data-page-type]
  (let [num-values (.readUInt bar)
        repetition-levels-size (.readUInt bar)
        definition-levels-size (.readUInt bar)
        compressed-data-size (.readUInt bar)
        uncompressed-data-size (.readUInt bar)]
    (DataPageHeader. (encode-page-type data-page-type) num-values repetition-levels-size
                     definition-levels-size compressed-data-size uncompressed-data-size)))

(defrecord DictionaryPageHeader [^int encoded-page-type
                                 ^int num-values
                                 ^int compressed-data-size
                                 ^int uncompressed-data-size]
  IPageHeader
  (type [this]
    (decode-page-type encoded-page-type))
  (header-length [this]
    (uints32-encoded-size (vals this)))
  (body-length [_]
    compressed-data-size)
  (byte-offset-body [_]
    0)
  ByteArrayWritable
  (writeTo [this byte-array-writer]
    (encode-uints32 byte-array-writer (vals this))))

(defn- read-dictionary-page-header [^ByteArrayReader bar dictionary-page-type]
  (let [num-values (.readUInt bar)
        compressed-data-size (.readUInt bar)
        uncompressed-data-size (.readUInt bar)]
    (DictionaryPageHeader. (encode-page-type dictionary-page-type) num-values compressed-data-size
                           uncompressed-data-size)))

(defprotocol IPageWriter
  (write-value [this value])
  (num-values [this]))

(defprotocol IPageWriterImpl
  (provisional-header [this])
  (header [this]))

(defn write [page-writer values]
  (reduce write-value page-writer values))

(deftype DataPageWriter
    [^:unsynchronized-mutable num-values
     body-length-estimator
     ^BufferedByteArrayWriter repetition-level-encoder
     ^BufferedByteArrayWriter definition-level-encoder
     ^BufferedByteArrayWriter data-encoder
     ^Compressor data-compressor
     ^:unsynchronized-mutable finished?]
  IPageWriter
  (write-value [this leveled-value]
    (let [v (:value leveled-value)]
      (when-not (nil? v)
        (encode data-encoder v)))
    (when repetition-level-encoder
      (encode repetition-level-encoder (:repetition-level leveled-value)))
    (when definition-level-encoder
      (encode definition-level-encoder (:definition-level leveled-value)))
    (set! num-values (inc num-values))
    this)
  (num-values [_] num-values)
  IPageWriterImpl
  (provisional-header [_]
    (DataPageHeader. (encode-page-type :data)
                     num-values
                     (if repetition-level-encoder (.estimatedSize repetition-level-encoder) 0)
                     (if definition-level-encoder (.estimatedSize definition-level-encoder) 0)
                     (.estimatedSize data-encoder)
                     (.estimatedSize data-encoder)))
  (header [_]
    (DataPageHeader. (encode-page-type :data)
                     num-values
                     (if repetition-level-encoder (.size repetition-level-encoder) 0)
                     (if definition-level-encoder (.size definition-level-encoder) 0)
                     (if data-compressor (.compressedSize data-compressor) (.size data-encoder))
                     (.size data-encoder)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-values 0)
    (set! finished? false)
    (when repetition-level-encoder
      (.reset repetition-level-encoder))
    (when definition-level-encoder
      (.reset definition-level-encoder))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not finished?
      (let [estimated-body-length (-> this provisional-header body-length)]
        (when repetition-level-encoder
          (.finish repetition-level-encoder))
        (when definition-level-encoder
          (.finish definition-level-encoder))
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (estimation/update! body-length-estimator (-> this header body-length) estimated-body-length))
      (set! finished? true)))
  (size [this]
    (let [h (header this)]
      (+ (header-length h) (body-length h))))
  (estimatedSize [this]
    (let [provisional-header (provisional-header this)]
      (+ (.size ^DataPageHeader provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (writeTo [this byte-array-writer]
    (.finish this)
    (doto byte-array-writer
      (.write (header this)))
    (when repetition-level-encoder
      (.write byte-array-writer repetition-level-encoder))
    (when definition-level-encoder
      (.write byte-array-writer definition-level-encoder))
    (.write byte-array-writer (if data-compressor data-compressor data-encoder))))

(defn data-page-writer [max-repetition-level max-definition-level value-type encoding compression]
  (DataPageWriter. 0 (estimation/ratio-estimator)
                   (when (pos? max-repetition-level) (levels-encoder max-repetition-level))
                   (when (pos? max-definition-level) (levels-encoder max-definition-level))
                   (encoder value-type encoding)
                   (compressor compression)
                   false))

(deftype DictionaryPageWriter [^:unsynchronized-mutable num-values
                               body-length-estimator
                               ^BufferedByteArrayWriter data-encoder
                               ^Compressor data-compressor
                               ^:unsynchronized-mutable finished?]
  IPageWriter
  (write-value [this value]
    (encode data-encoder value)
    (set! num-values (inc num-values))
    this)
  (num-values [_] num-values)
  IPageWriterImpl
  (provisional-header [_]
    (DictionaryPageHeader. (encode-page-type :dictionary) num-values (.estimatedSize data-encoder)
                           (.estimatedSize data-encoder)))
  (header [_]
    (DictionaryPageHeader. (encode-page-type :dictionary)
                           num-values
                           (if data-compressor (.compressedSize data-compressor) (.size data-encoder))
                           (.size data-encoder)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-values 0)
    (set! finished? false)
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not finished?
      (let [estimated-body-length (-> this provisional-header body-length)]
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (estimation/update! body-length-estimator (-> this header body-length) estimated-body-length))
      (set! finished? true)))
  (size [this]
    (let [h (header this)]
      (+ (header-length h) (body-length h))))
  (estimatedSize [this]
    (let [provisional-header (provisional-header this)]
      (+ (.size ^DataPageHeader provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (writeTo [this byte-array-writer]
    (.finish this)
    (doto byte-array-writer
      (.write (header this))
      (.write (if data-compressor data-compressor data-encoder)))))

(defn dictionary-page-writer [value-type encoding compression]
  (DictionaryPageWriter. 0 (estimation/ratio-estimator)
                         (encoder value-type encoding)
                         (compressor compression)
                         false))

(defprotocol IPageReader
  (read [_]))

(defprotocol IDataPageReader
  (read-repetition-levels [_])
  (read-definition-levels [_])
  (read-data [_])
  (skip [_]))

(defrecord DataPageReader [^ByteArrayReader byte-array-reader
                           max-definition-level
                           data-decoder-ctor
                           decompressor-ctor
                           header]
  IPageReader
  (read [this]
    (letfn [(lazy-read-values [repetition-levels-seq definition-levels-seq values-seq]
              (lazy-seq
               (let [nil-value? (< (first definition-levels-seq) max-definition-level)]
                 (cons (leveled-value (first repetition-levels-seq)
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
          (levels-decoder max-definition-level)
          decode-values)
      (repeat 0)))
  (read-definition-levels [_]
    (if (has-definition-levels? header)
      (-> byte-array-reader
          (.sliceAhead (byte-offset-definition-levels header))
          (levels-decoder max-definition-level)
          decode-values)
      (repeat max-definition-level)))
  (read-data [_]
    (let [data-bytes-reader (-> byte-array-reader
                                (.sliceAhead (byte-offset-body header)))
          data-bytes-reader (if-let [decompressor (decompressor-ctor)]
                              (.decompress ^Decompressor decompressor
                                           data-bytes-reader
                                           (:compressed-data-size header)
                                           (:uncompressed-data-size header))
                              data-bytes-reader)]
      (decode-values (data-decoder-ctor data-bytes-reader))))
  (skip [_]
    (.sliceAhead byte-array-reader (body-length header))))

(defn data-page-reader
  [^ByteArrayReader byte-array-reader max-definition-level value-type encoding compression]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-data-page-type bar)]
    (DataPageReader. bar max-definition-level (decoder-ctor value-type encoding)
                     (decompressor-ctor compression) (read-data-page-header bar page-type))))

(defn data-page-readers
  [^ByteArrayReader byte-array-reader num-data-pages max-definition-level value-type encoding compression]
  (let [num-data-pages (int num-data-pages)]
    (lazy-seq
     (when (pos? num-data-pages)
       (let [next-data-page-reader (data-page-reader byte-array-reader max-definition-level value-type
                                                     encoding compression)]
         (cons next-data-page-reader
               (data-page-readers (skip next-data-page-reader) (dec num-data-pages) max-definition-level
                                  value-type encoding compression)))))))

(defn read-data-pages
  [^ByteArrayReader byte-array-reader num-data-pages max-definition-level value-type encoding compression]
  (->> (data-page-readers byte-array-reader num-data-pages max-definition-level value-type
                          encoding compression)
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
  (read [this]
    (let [data-bytes-reader (-> byte-array-reader
                                (.sliceAhead (byte-offset-body header)))
          data-bytes-reader (if-let [decompressor (decompressor-ctor)]
                              (.decompress ^Decompressor decompressor
                                           data-bytes-reader
                                           (:compressed-data-size header)
                                           (:uncompressed-data-size header))
                              data-bytes-reader)]
      (->> (data-decoder-ctor data-bytes-reader)
           decode-values
           (take (:num-values header))))))

(defn dictionary-page-reader
  [^ByteArrayReader byte-array-reader value-type encoding compression]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-dictionary-page-type bar)]
    (DictionaryPageReader. bar (decoder-ctor value-type encoding)
                           (decompressor-ctor compression)
                           (read-dictionary-page-header bar page-type))))

(defn read-dictionary [^ByteArrayReader byte-array-reader value-type encoding compression]
  (read (dictionary-page-reader byte-array-reader value-type encoding compression)))

(defn read-dictionary-header [^ByteArrayReader byte-array-reader]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-dictionary-page-type bar)]
    (read-dictionary-page-header bar page-type)))
