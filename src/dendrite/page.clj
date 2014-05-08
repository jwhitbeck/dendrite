(ns dendrite.page
  (:require [dendrite.core :refer [wrap-value]]
            [dendrite.compression :refer [compressor decompressor-ctor]]
            [dendrite.encoding :refer [encode decode-values levels-encoder levels-decoder encoder
                                       decoder-ctor]]
            [dendrite.estimation :as estimation])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter ByteArrayWritable
            Compressor Decompressor]))

(set! *warn-on-reflection* true)

(def ^:private page-types [:data :dictionnary])

(def ^:private page-type-encodings
  (reduce-kv (fn [m data-page-idx data-page-kw] (assoc m data-page-kw data-page-idx)) {} page-types))

(defn- encode-page-type [page-type] (get page-type-encodings page-type))

(defn- decode-page-type [encoded-page-type] (get page-types encoded-page-type))

(defn- read-next-page-type [^ByteArrayReader bar]
  (-> bar .readUInt32 decode-page-type))

(defprotocol IPageHeader
  (page-type [this])
  (length [this])
  (body-length [this])
  (byte-offset-body [this]))

(defprotocol IDataPageHeader
  (has-repetition-levels? [this])
  (has-definition-levels? [this])
  (byte-offset-repetition-levels [this])
  (byte-offset-definition-levels [this]))

(defn- uints32-encoded-size [coll]
  (reduce #(+ %1 (ByteArrayWriter/getNumUInt32Bytes %2)) 0 coll))

(defn- encode-uints32 [^ByteArrayWriter byte-array-writer coll]
  (reduce #(doto ^ByteArrayWriter %1 (.writeUInt32 %2)) byte-array-writer coll))

(defrecord DataPageHeader [^int encoded-page-type
                           ^int num-values
                           ^int repetition-levels-size
                           ^int definition-levels-size
                           ^int compressed-data-size
                           ^int uncompressed-data-size]
  IPageHeader
  (page-type [this]
    (decode-page-type encoded-page-type))
  (length [this]
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
  (let [num-values (.readUInt32 bar)
        repetition-levels-size (.readUInt32 bar)
        definition-levels-size (.readUInt32 bar)
        compressed-data-size (.readUInt32 bar)
        uncompressed-data-size (.readUInt32 bar)]
    (DataPageHeader. (encode-page-type data-page-type) num-values repetition-levels-size
                     definition-levels-size compressed-data-size uncompressed-data-size)))

(defrecord DictionnaryPageHeader [^int encoded-page-type
                                  ^int num-values
                                  ^int compressed-data-size
                                  ^int uncompressed-data-size]
  IPageHeader
  (page-type [this]
    (decode-page-type encoded-page-type))
  (length [this]
    (uints32-encoded-size (vals this)))
  (body-length [_]
    compressed-data-size)
  (byte-offset-body [_]
    0)
  ByteArrayWritable
  (writeTo [this byte-array-writer]
    (encode-uints32 byte-array-writer (vals this))))

(defn- read-dictionnary-page-header [^ByteArrayReader bar dictionnary-page-type]
  (let [num-values (.readUInt32 bar)
        compressed-data-size (.readUInt32 bar)
        uncompressed-data-size (.readUInt32 bar)]
    (DictionnaryPageHeader. (encode-page-type dictionnary-page-type) num-values compressed-data-size
                            uncompressed-data-size)))

(defprotocol IPageWriter
  (write [this value]))

(defprotocol IPageWriterImpl
  (provisional-header [this])
  (header [this]))

(defn write-all [page-writer values]
  (reduce #(write %1 %2) page-writer values))

(deftype DataPageWriter
    [^{:unsynchronized-mutable :int} num-values
     body-length-estimator
     ^BufferedByteArrayWriter repetition-level-encoder
     ^BufferedByteArrayWriter definition-level-encoder
     ^BufferedByteArrayWriter data-encoder
     ^Compressor data-compressor]
  IPageWriter
  (write [this wrapped-value]
    (when-let [v (:value wrapped-value)]
      (encode data-encoder v))
    (when repetition-level-encoder
      (encode repetition-level-encoder (:repetition-level wrapped-value)))
    (when definition-level-encoder
      (encode definition-level-encoder (:definition-level wrapped-value)))
    (set! num-values (inc num-values))
    this)
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
    (when repetition-level-encoder
      (.reset repetition-level-encoder))
    (when definition-level-encoder
      (.reset definition-level-encoder))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (let [estimated-body-length (-> this provisional-header body-length)]
      (when repetition-level-encoder
        (.finish repetition-level-encoder))
      (when definition-level-encoder
        (.finish definition-level-encoder))
      (.finish data-encoder)
      (when data-compressor
        (.compress data-compressor data-encoder))
      (estimation/update! body-length-estimator (-> this header body-length) estimated-body-length)))
  (size [this]
    (let [h (header this)]
      (+ (length h) (body-length h))))
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

(defn data-page-writer [max-definition-level required? value-type encoding compression-type]
  (DataPageWriter. 0 (estimation/ratio-estimator)
                   (when-not (= 1 max-definition-level) (levels-encoder max-definition-level))
                   (when-not required? (levels-encoder max-definition-level))
                   (encoder value-type encoding)
                   (compressor compression-type)))

(deftype DictionnaryPageWriter [^{:unsynchronized-mutable :int} num-values
                                body-length-estimator
                                ^BufferedByteArrayWriter data-encoder
                                ^Compressor data-compressor]
  IPageWriter
  (write [this value]
    (encode data-encoder value)
    (set! num-values (inc num-values))
    this)
  IPageWriterImpl
  (provisional-header [_]
    (DictionnaryPageHeader. (encode-page-type :dictionnary) num-values (.estimatedSize data-encoder)
                            (.estimatedSize data-encoder)))
  (header [_]
    (DictionnaryPageHeader. (encode-page-type :dictionnary)
                            num-values
                            (if data-compressor (.compressedSize data-compressor) (.size data-encoder))
                            (.size data-encoder)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-values 0)
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (let [estimated-body-length (-> this provisional-header body-length)]
      (.finish data-encoder)
      (when data-compressor
        (.compress data-compressor data-encoder))
      (estimation/update! body-length-estimator (-> this header body-length) estimated-body-length)))
  (size [this]
    (let [h (header this)]
      (+ (length h) (body-length h))))
  (estimatedSize [this]
    (let [provisional-header (provisional-header this)]
      (+ (.size ^DataPageHeader provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (writeTo [this byte-array-writer]
    (.finish this)
    (doto byte-array-writer
      (.write (header this))
      (.write (if data-compressor data-compressor data-encoder)))))

(defn dictionnary-page-writer [value-type encoding compression-type]
  (DictionnaryPageWriter. 0 (estimation/ratio-estimator)
                          (encoder value-type encoding)
                          (compressor compression-type)))

(defprotocol IPageReader
  (read-page [_]))

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
  (read-page [this]
    (letfn [(lazy-read-values [repetition-levels-seq definition-levels-seq values-seq]
              (lazy-seq
               (let [nil-value? (< (first definition-levels-seq) max-definition-level)]
                 (cons (wrap-value (first repetition-levels-seq)
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
  [^ByteArrayReader byte-array-reader max-definition-level value-type encoding compression-type]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-page-type bar)]
    (when-not (= page-type :data)
      (throw (IllegalArgumentException. (format "Page type %s is not a supported data page type" page-type))))
    (DataPageReader. bar max-definition-level (decoder-ctor value-type encoding)
                     (decompressor-ctor compression-type) (read-data-page-header bar page-type))))

(defn data-page-readers
  [^ByteArrayReader byte-array-reader num-data-pages max-definition-level value-type encoding compression-type]
  (lazy-seq
   (when (pos? num-data-pages)
     (let [next-data-page-reader (data-page-reader byte-array-reader max-definition-level value-type
                                                   encoding compression-type)]
       (cons next-data-page-reader
             (data-page-readers (skip next-data-page-reader) (dec num-data-pages) max-definition-level
                                value-type encoding compression-type))))))

(defrecord DictionnaryPageReader [^ByteArrayReader byte-array-reader
                                  data-decoder-ctor
                                  decompressor-ctor
                                  header]
  IPageReader
  (read-page [this]
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

(defn dictionnary-page-reader
  [^ByteArrayReader byte-array-reader value-type encoding compression-type]
  (let [bar (.slice byte-array-reader)
        page-type (read-next-page-type bar)]
    (when-not (= page-type :dictionnary)
      (throw (IllegalArgumentException.
              (format "Page type %s is not a supported dictionnary page type" page-type))))
    (DictionnaryPageReader. bar (decoder-ctor value-type encoding)
                            (decompressor-ctor compression-type)
                            (read-dictionnary-page-header bar page-type))))
