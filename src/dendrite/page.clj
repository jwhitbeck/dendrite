(ns dendrite.page
  (:require [dendrite.core :refer [wrap-value]]
            [dendrite.compression :refer [compressor decompressor-ctor]]
            [dendrite.encoding :refer [encode decode-values levels-encoder levels-decoder encoder
                                       decoder-ctor]]
            [dendrite.estimation :as estimation])
  (:import [dendrite.java BufferedByteArrayWriter ByteArrayReader ByteArrayWriter ByteArrayWritable
            Compressor Decompressor]))

(set! *warn-on-reflection* true)

(defprotocol IDataPageHeader
  (length [this])
  (body-length [this])
  (has-repetition-levels? [this])
  (has-definition-levels? [this])
  (byte-offset-repetition-levels [this])
  (byte-offset-definition-levels [this])
  (byte-offset-body [this]))

(defrecord DataPageHeader [^int num-values
                           ^int num-rows
                           ^int repetition-levels-size
                           ^int definition-levels-size
                           ^int compressed-data-size
                           ^int uncompressed-data-size]
  IDataPageHeader
  (length [this]
    (reduce #(+ %1 (ByteArrayWriter/getNumUInt32Bytes %2)) 0 (vals this)))
  (body-length [_]
    (+ repetition-levels-size definition-levels-size compressed-data-size))
  (has-repetition-levels? [_]
    (pos? repetition-levels-size))
  (has-definition-levels? [_]
    (pos? definition-levels-size))
  (byte-offset-repetition-levels [_]
    0)
  (byte-offset-definition-levels [_]
    repetition-levels-size)
  (byte-offset-body [_]
    (+ repetition-levels-size definition-levels-size))
  ByteArrayWritable
  (writeTo [this baw]
    (reduce #(doto ^ByteArrayWriter %1 (.writeUInt32 %2)) baw (vals this))))

(defn read-data-page-header [^ByteArrayReader bar]
  (let [num-values (.readUInt32 bar)
        num-rows (.readUInt32 bar)
        repetition-levels-size (.readUInt32 bar)
        definition-levels-size (.readUInt32 bar)
        compressed-data-size (.readUInt32 bar)
        uncompressed-data-size (.readUInt32 bar)]
    (DataPageHeader. num-values num-rows repetition-levels-size definition-levels-size compressed-data-size
                     uncompressed-data-size)))

(def page-types [:data])

(def ^:private page-type-encodings
  (reduce-kv (fn [m data-page-idx data-page-kw] (assoc m data-page-kw data-page-idx)) {} page-types))

(defn encode-page-type [page-type] (get page-type-encodings page-type))

(defn decode-page-type [encoded-page-type] (get page-types encoded-page-type))

(defprotocol IDataPageWriter
  ^IDataPageWriter (write [this wrapped-value])
  ^IDataPageWriter (incr-num-rows [this])
  ^int (uncompressed-size-estimate [this])
  ^DataPageHeader (header [this]))

(defn write-wrapped-values [data-page-writer wrapped-values]
  (reduce #(write %1 %2) data-page-writer wrapped-values))

(defn write-wrapped-values-for-row [data-page-writer wrapped-values]
  (doto data-page-writer
    (write-wrapped-values wrapped-values)
    incr-num-rows))

(deftype DataPageWriter
    [^{:unsynchronized-mutable :int} num-values
     ^{:unsynchronized-mutable :int} num-rows
     size-estimator
     ^BufferedByteArrayWriter repetition-level-encoder
     ^BufferedByteArrayWriter definition-level-encoder
     ^BufferedByteArrayWriter data-encoder
     ^Compressor data-compressor]
  IDataPageWriter
  (write [this wrapped-value]
    (if-let [v (:value wrapped-value)]
      (encode data-encoder v))
    (when repetition-level-encoder
      (encode repetition-level-encoder (:repetition-level wrapped-value)))
    (when definition-level-encoder
      (encode definition-level-encoder (:definition-level wrapped-value)))
    (set! num-values (inc num-values))
    this)
  (incr-num-rows [this]
    (set! num-rows (inc num-rows))
    this)
  (uncompressed-size-estimate [_]
    (let [provisional-header
            (DataPageHeader. num-rows
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
                     (if data-compressor (.compressedSize data-compressor) 0)
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
    (let [h ^DataPageHeader (header this)]
      (+ (.size h) (body-length h))))
  (estimatedSize [this]
    (estimation/correct size-estimator (uncompressed-size-estimate this)))
  (writeTo [this byte-array-writer]
    (doto byte-array-writer
      (.writeUInt32 (encode-page-type :data))
      (.write (header this)))
    (when repetition-level-encoder
      (.write byte-array-writer repetition-level-encoder))
    (when definition-level-encoder
      (.write byte-array-writer definition-level-encoder))
    (.write byte-array-writer (if data-compressor data-compressor data-encoder))))

(defn- new-data-page-writer [repetition-level-encoder definition-level-encoder data-encoder data-compressor]
  (DataPageWriter. 0 0 (estimation/ratio-estimator)
                   repetition-level-encoder definition-level-encoder data-encoder data-compressor))

(defn data-page-writer [max-definition-level value-type encoding compression-type]
  (new-data-page-writer (levels-encoder max-definition-level)
                        (levels-encoder max-definition-level)
                        (encoder value-type encoding)
                        (compressor compression-type)))

(defn required-data-page-writer [max-definition-level value-type encoding compression-type]
  (new-data-page-writer (levels-encoder max-definition-level)
                        nil
                        (encoder value-type encoding)
                        (compressor compression-type)))

(defn top-level-data-page-writer [max-definition-level value-type encoding compression-type]
  (new-data-page-writer nil
                        (levels-encoder max-definition-level)
                        (encoder value-type encoding)
                        (compressor compression-type)))

(defn required-top-level-data-page-writer [max-definition-level value-type encoding compression-type]
  (new-data-page-writer nil nil (encoder value-type encoding) (compressor compression-type)))

(defmulti page-reader
  (fn [^ByteArrayReader bar max-definition-level value-type encoding compression-type]
    (-> bar .readUInt32 decode-page-type)))

(defprotocol IPageReader
  (next-page [_]))

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
  (next-page [_]
    (page-reader (.sliceAhead byte-array-reader (body-length header))))
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

(defmethod page-reader :data
  [^ByteArrayReader bar max-definition-level value-type encoding compression-type]
  (DataPageReader. bar max-definition-level (decoder-ctor value-type encoding)
                   (decompressor-ctor compression-type) (read-data-page-header bar)))

(defn read-values [^DataPageReader page-reader]
  (letfn [(lazy-read-values [repetition-levels-seq definition-levels-seq values-seq]
            (lazy-seq
             (let [nil-value? (< (first definition-levels-seq) (:max-definition-level page-reader))]
               (cons (wrap-value (first repetition-levels-seq)
                                 (first definition-levels-seq)
                                 (if nil-value? nil (first values-seq)))
                     (lazy-read-values (rest repetition-levels-seq)
                                       (rest definition-levels-seq)
                                       (if nil-value? values-seq (rest values-seq)))))))]
    (take (-> page-reader :header :num-values)
          (lazy-read-values (read-repetition-levels page-reader)
                            (read-definition-levels page-reader)
                            (read-data page-reader)))))
