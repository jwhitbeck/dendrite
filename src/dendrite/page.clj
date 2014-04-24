(ns dendrite.page
  (:require [dendrite.core :refer [wrap-value]]
            [dendrite.compression :refer [compressor decompressor-ctor]]
            [dendrite.encoding :refer [encode decode-values levels-encoder levels-decoder encoder
                                       decoder-ctor]]
            [dendrite.estimation :as estimation])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter ByteArrayWritable
            Compressor Decompressor]))

(set! *warn-on-reflection* true)

(defprotocol IPageHeader
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

(defrecord DataPageHeader [^int num-values
                           ^int num-rows
                           ^int repetition-levels-size
                           ^int definition-levels-size
                           ^int compressed-data-size
                           ^int uncompressed-data-size]
  IPageHeader
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

(defn read-data-page-header [^ByteArrayReader bar]
  (let [num-values (.readUInt32 bar)
        num-rows (.readUInt32 bar)
        repetition-levels-size (.readUInt32 bar)
        definition-levels-size (.readUInt32 bar)
        compressed-data-size (.readUInt32 bar)
        uncompressed-data-size (.readUInt32 bar)]
    (DataPageHeader. num-values num-rows repetition-levels-size definition-levels-size compressed-data-size
                     uncompressed-data-size)))

(defrecord DictionnaryPageHeader [^int num-values
                                  ^int compressed-data-size
                                  ^int uncompressed-data-size]
  IPageHeader
  (length [this]
    (uints32-encoded-size (vals this)))
  (body-length [_]
    compressed-data-size)
  (byte-offset-body [_]
    0)
  ByteArrayWritable
  (writeTo [this byte-array-writer]
    (encode-uints32 byte-array-writer (vals this))))

(defn read-dictionnary-page-header [^ByteArrayReader bar]
  (let [num-values (.readUInt32 bar)
        compressed-data-size (.readUInt32 bar)
        uncompressed-data-size (.readUInt32 bar)]
    (DictionnaryPageHeader. num-values compressed-data-size uncompressed-data-size)))

(def page-types [:data :dictionnary])

(def ^:private page-type-encodings
  (reduce-kv (fn [m data-page-idx data-page-kw] (assoc m data-page-kw data-page-idx)) {} page-types))

(defn encode-page-type [page-type] (get page-type-encodings page-type))

(defn decode-page-type [encoded-page-type] (get page-types encoded-page-type))

(defprotocol IPageWriter
  ^IPageWriter (write [this value]))

(defprotocol IDataPageWriter
  ^IDataPageWriter (incr-num-rows [this])
  ^int (uncompressed-size-estimate [this])
  ^IDataPageHeader (header [this]))

(defn write-values [data-page-writer values]
  (reduce #(write %1 %2) data-page-writer values))

(defn write-wrapped-values-for-row [data-page-writer wrapped-values]
  (doto data-page-writer
    (write-values wrapped-values)
    incr-num-rows))

(deftype DataPageWriter
    [^{:unsynchronized-mutable :int} num-values
     ^{:unsynchronized-mutable :int} num-rows
     size-estimator
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
  IDataPageWriter
  (incr-num-rows [this]
    (set! num-rows (inc num-rows))
    this)
  (uncompressed-size-estimate [_]
    (let [provisional-header
            (DataPageHeader. num-values
                             num-rows
                             (if repetition-level-encoder (.estimatedSize repetition-level-encoder) 0)
                             (if definition-level-encoder (.estimatedSize definition-level-encoder) 0)
                             (.estimatedSize data-encoder)
                             (.estimatedSize data-encoder))]
      (+ (.size provisional-header) (body-length provisional-header))))
  (header [_]
    (DataPageHeader. num-values
                     num-rows
                     (if repetition-level-encoder (.size repetition-level-encoder) 0)
                     (if definition-level-encoder (.size definition-level-encoder) 0)
                     (if data-compressor (.compressedSize data-compressor) (.size data-encoder))
                     (.size data-encoder)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-values 0)
    (set! num-rows 0)
    (when repetition-level-encoder
      (.reset repetition-level-encoder))
    (when definition-level-encoder
      (.reset definition-level-encoder))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (let [estimated-size (.estimatedSize this)]
      (when repetition-level-encoder
        (.finish repetition-level-encoder))
      (when definition-level-encoder
        (.finish definition-level-encoder))
      (.finish data-encoder)
      (when data-compressor
        (.compress data-compressor data-encoder))
      (estimation/update! size-estimator (.size this) estimated-size)))
  (size [this]
    (let [h ^IDataPageHeader (header this)]
      (+ (length h) (body-length h))))
  (estimatedSize [this]
    (estimation/correct size-estimator (uncompressed-size-estimate this)))
  (writeTo [this byte-array-writer]
    (.finish this)
    (doto byte-array-writer
      (.writeUInt32 (encode-page-type :data))
      (.write (header this)))
    (when repetition-level-encoder
      (.write byte-array-writer repetition-level-encoder))
    (when definition-level-encoder
      (.write byte-array-writer definition-level-encoder))
    (.write byte-array-writer (if data-compressor data-compressor data-encoder))))

(defn data-page-writer [max-definition-level required? value-type encoding compression-type]
  (DataPageWriter. 0 0 (estimation/ratio-estimator)
                   (when-not (= 1 max-definition-level) (levels-encoder max-definition-level))
                   (when-not required? (levels-encoder max-definition-level))
                   (encoder value-type encoding)
                   (compressor compression-type)))

(defprotocol IDictionnaryPageWriter
  ^int (uncompressed-size-estimate [this])
  ^IDictionnaryPageHeader (header [this]))

(deftype DictionnaryPageWriter [^{:unsynchronized-mutable :int} num-values
                                size-estimator
                                ^BufferedByteArrayWriter data-encoder
                                ^Compressor data-compressor]
  IPageWriter
  (write [this value]
    (encode data-encoder value)
    (set! num-values (inc num-values))
    this)
  IDictionnaryPageWriter
  (uncompressed-size-estimate [_]
    (let [provisional-header
            (DictionnaryPageHeader. num-values (.estimatedSize data-encoder) (.estimatedSize data-encoder))]
      (+ (.size provisional-header) (body-length provisional-header))))
  (header [_]
    (DictionnaryPageHeader. num-values
                             (if data-compressor (.compressedSize data-compressor) (.size data-encoder))
                             (.size data-encoder)))
  BufferedByteArrayWriter
  (reset [_]
    (set! num-values 0)
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (let [estimated-size (.estimatedSize this)]
      (.finish data-encoder)
      (when data-compressor
        (.compress data-compressor data-encoder))
      (estimation/update! size-estimator (.size this) estimated-size)))
  (size [this]
    (let [h ^IDictionnaryPageHeader (header this)]
      (+ (length h) (body-length h))))
  (estimatedSize [this]
    (estimation/correct size-estimator (uncompressed-size-estimate this)))
  (writeTo [this byte-array-writer]
    (.finish this)
    (doto byte-array-writer
      (.writeUInt32 (encode-page-type :dictionnary))
      (.write (header this))
      (.write (if data-compressor data-compressor data-encoder)))))

(defn dictionnary-page-writer [value-type encoding compression-type]
  (DictionnaryPageWriter. 0 (estimation/ratio-estimator)
                          (encoder value-type encoding)
                          (compressor compression-type)))

(defn next-page-type [^ByteArrayReader bar]
  (-> bar .readUInt32 decode-page-type))

(defprotocol IPageReader
  (page-type [_])
  (read-page [_])
  (next-page-byte-array-reader [_]))

(defprotocol IDataPageReader
  (read-repetition-levels [_])
  (read-definition-levels [_])
  (read-data [_]))

(defrecord DataPageReader [^ByteArrayReader byte-array-reader
                           max-definition-level
                           data-decoder-ctor
                           decompressor-ctor
                           header]
  IPageReader
  (page-type [_]
    :data)
  (next-page-byte-array-reader [_]
    (.sliceAhead byte-array-reader (body-length header)))
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
          data-bytes-reader (if-let [decompressor ^Decompressor (decompressor-ctor)]
                              (.decompress decompressor
                                           data-bytes-reader
                                           (:compressed-data-size header)
                                           (:uncompressed-data-size header))
                              data-bytes-reader)]
      (decode-values (data-decoder-ctor data-bytes-reader)))))

(defn data-page-reader
  [^ByteArrayReader bar max-definition-level value-type encoding compression-type]
  (DataPageReader. bar max-definition-level (decoder-ctor value-type encoding)
                   (decompressor-ctor compression-type) (read-data-page-header bar)))

(defrecord DictionnaryPageReader [^ByteArrayReader byte-array-reader
                                  data-decoder-ctor
                                  decompressor-ctor
                                  header]
  IPageReader
  (page-type [_]
    :dictionnary)
  (next-page-byte-array-reader [_]
    (.sliceAhead byte-array-reader (body-length header)))
  (read-page [this]
    (let [data-bytes-reader (-> byte-array-reader
                                (.sliceAhead (byte-offset-body header)))
          data-bytes-reader (if-let [decompressor ^Decompressor (decompressor-ctor)]
                              (.decompress decompressor
                                           data-bytes-reader
                                           (:compressed-data-size header)
                                           (:uncompressed-data-size header))
                              data-bytes-reader)]
      (->> (data-decoder-ctor data-bytes-reader)
           decode-values
           (take (:num-values header))))))

(defn dictionnary-page-reader
  [^ByteArrayReader bar value-type encoding compression-type]
  (DictionnaryPageReader. bar (decoder-ctor value-type encoding)
                          (decompressor-ctor compression-type) (read-dictionnary-page-header bar)))
