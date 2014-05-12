(ns dendrite.encoding
  (:import [dendrite.java
            BooleanEncoder BooleanDecoder BooleanPackedEncoder BooleanPackedDecoder
            Int32Encoder Int32Decoder Int32PlainEncoder Int32PlainDecoder
            Int32PackedDeltaEncoder Int32PackedDeltaDecoder
            Int32FixedBitWidthPackedRunLengthEncoder Int32FixedBitWidthPackedRunLengthDecoder
            Int32PackedRunLengthEncoder Int32PackedRunLengthDecoder
            Int64Encoder Int64Decoder Int64PlainEncoder Int64PlainDecoder
            Int64PackedDeltaEncoder Int64PackedDeltaDecoder
            FloatEncoder FloatDecoder FloatPlainEncoder FloatPlainDecoder
            DoubleEncoder DoubleDecoder DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayEncoder FixedLengthByteArrayDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayEncoder ByteArrayDecoder ByteArrayPlainEncoder ByteArrayPlainDecoder
            ByteArrayIncrementalEncoder ByteArrayIncrementalDecoder
            ByteArrayDeltaLengthEncoder ByteArrayDeltaLengthDecoder
            ByteArrayReader BufferedByteArrayWriter]
           [java.nio.charset Charset]
           [java.math BigInteger]))

(set! *warn-on-reflection* true)

(defprotocol Decoder (decode [decoder]))

(extend-protocol Decoder
  BooleanDecoder
  (decode [boolean-decoder] (.decode boolean-decoder))
  Int32Decoder
  (decode [int32-decoder] (.decode int32-decoder))
  Int64Decoder
  (decode [int64-decoder] (.decode int64-decoder))
  FloatDecoder
  (decode [float-decoder] (.decode float-decoder))
  DoubleDecoder
  (decode [double-decoder] (.decode double-decoder))
  FixedLengthByteArrayDecoder
  (decode [fixed-length-byte-array-decoder] (.decode fixed-length-byte-array-decoder))
  ByteArrayDecoder
  (decode [byte-array-decoder] (.decode byte-array-decoder)))

(defn decode-values [decoder]
  (lazy-seq
   (cons (decode decoder) (decode-values decoder))))

(defprotocol Encoder (encode [encoder value]))

(extend-protocol Encoder
  BooleanEncoder
  (encode [boolean-encoder value] (doto boolean-encoder (.encode value)))
  Int32Encoder
  (encode [int32-encoder value] (doto int32-encoder (.encode value)))
  Int64Encoder
  (encode [int64-encoder value] (doto int64-encoder (.encode value)))
  FloatEncoder
  (encode [float-encoder value] (doto float-encoder (.encode value)))
  DoubleEncoder
  (encode [double-encoder value] (doto double-encoder (.encode value)))
  FixedLengthByteArrayEncoder
  (encode [fixed-length-byte-array-encoder value] (doto fixed-length-byte-array-encoder (.encode value)))
  ByteArrayEncoder
  (encode [byte-array-encoder value] (doto byte-array-encoder (.encode value))))

(defn encode-values [encoder values]
  (reduce encode encoder values))

(def ^:private valid-encodings-for-types
  {:boolean #{:plain}
   :int32 #{:plain :packed-run-length :delta}
   :int64 #{:plain :delta}
   :float #{:plain}
   :double #{:plain}
   :byte-array #{:plain :incremental :delta-length}
   :fixed-length-byte-array #{:plain}})

(defn- valid-base-value-type? [base-type]
  (-> valid-encodings-for-types keys set (contains? base-type)))

(defn- valid-encoding-for-base-type? [base-type encoding]
  (contains? (get valid-encodings-for-types base-type) encoding))

(defn- list-encodings-for-base-type [base-type] (get valid-encodings-for-types base-type))

(defn- base-decoder-ctor [base-type encoding]
  (case base-type
    :boolean #(BooleanPackedDecoder. %)
    :int32 (case encoding
             :plain #(Int32PlainDecoder. %)
             :packed-run-length #(Int32PackedRunLengthDecoder. %)
             :delta #(Int32PackedDeltaDecoder. %))
    :int64 (case encoding
             :plain #(Int64PlainDecoder. %)
             :delta #(Int64PackedDeltaDecoder. %))
    :float #(FloatPlainDecoder. %)
    :double #(DoublePlainDecoder. %)
    :byte-array (case encoding
                  :plain #(ByteArrayPlainDecoder. %)
                  :incremental #(ByteArrayIncrementalDecoder. %)
                  :delta-length #(ByteArrayDeltaLengthDecoder. %))
    :fixed-length-byte-array #(FixedLengthByteArrayPlainDecoder. %)))

(defn- base-encoder [base-type encoding]
  (case base-type
    :boolean (BooleanPackedEncoder.)
    :int32 (case encoding
             :plain (Int32PlainEncoder.)
             :packed-run-length (Int32PackedRunLengthEncoder.)
             :delta (Int32PackedDeltaEncoder.))
    :int64 (case encoding
             :plain (Int64PlainEncoder.)
             :delta (Int64PackedDeltaEncoder.))
    :float (FloatPlainEncoder.)
    :double (DoublePlainEncoder.)
    :byte-array (case encoding
                  :plain (ByteArrayPlainEncoder.)
                  :incremental (ByteArrayIncrementalEncoder.)
                  :delta-length (ByteArrayDeltaLengthEncoder.))
    :fixed-length-byte-array (FixedLengthByteArrayPlainEncoder.)))

(defn- packed-bit-width [n] (- 32 (Integer/numberOfLeadingZeros n)))

(defn levels-encoder [max-definition-level]
  (Int32FixedBitWidthPackedRunLengthEncoder. (packed-bit-width max-definition-level)))

(defn levels-decoder [^ByteArrayReader byte-array-reader max-definition-level]
  (Int32FixedBitWidthPackedRunLengthDecoder. byte-array-reader (packed-bit-width max-definition-level)))

(def ^{:private true :tag Charset} utf8-charset (Charset/forName "UTF-8"))

(defn str->utf8-bytes [^String s] (.getBytes s utf8-charset))

(defn utf8-bytes->str [^bytes bs] (String. bs utf8-charset))

(def ^:private derived-types
  {:string {:base-type :byte-array
            :to-base-type-fn str->utf8-bytes
            :from-base-type-fn utf8-bytes->str}
   :bigint {:base-type :byte-array
            :to-base-type-fn #(.toByteArray ^BigInteger %)
            :from-base-type-fn #(BigInteger. ^bytes %)}
   :keyword {:base-type :byte-array
             :to-base-type-fn (comp str->utf8-bytes name)
             :from-base-type-fn (comp keyword utf8-bytes->str)}
   :symbol {:base-type :byte-array
            :to-base-type-fn (comp str->utf8-bytes name)
            :from-base-type-fn (comp symbol utf8-bytes->str)}})

(defn derived-type? [t] (contains? derived-types t))

(defn- base-type [t]
  (get-in derived-types [t :base-type]))

(defn valid-value-type? [t]
  (valid-base-value-type? (if (derived-type? t) (base-type t) t)))

(defn valid-encoding-for-type? [t encoding]
  (valid-encoding-for-base-type? (if (derived-type? t) (base-type t) t) encoding))

(defn list-encodings-for-type [t]
  (list-encodings-for-base-type (if (derived-type? t) (base-type t) t)))

(defn- derived->base-type-fn [t]
  (get-in derived-types [t :to-base-type-fn]))

(defn- base->derived-type-fn [t]
  (get-in derived-types [t :from-base-type-fn]))

(defn encoder [t encoding]
  (if-not (derived-type? t)
    (base-encoder t encoding)
    (let [be (base-encoder (base-type t) encoding)
          derived->base-type (derived->base-type-fn t)]
      (reify
        Encoder
        (encode [this v] (encode be (derived->base-type v)) this)
        BufferedByteArrayWriter
        (reset [_] (.reset ^BufferedByteArrayWriter be))
        (finish [_] (.finish ^BufferedByteArrayWriter be))
        (size [_] (.size ^BufferedByteArrayWriter be))
        (estimatedSize [_] (.estimatedSize ^BufferedByteArrayWriter be))
        (writeTo [_ byte-array-writer] (.writeTo ^BufferedByteArrayWriter be byte-array-writer))))))

(defn decoder-ctor [t encoding]
  (if-not (derived-type? t)
    (base-decoder-ctor t encoding)
    (let [bdc (base-decoder-ctor (base-type t) encoding)
          base->derived-type (base->derived-type-fn t)]
      #(let [bd (bdc %)]
         (reify
           Decoder
           (decode [_] (-> (decode bd) base->derived-type)))))))

(defn coercion-fn [t]
  (let [coerce (case t
                 :boolean boolean
                 :int32 int
                 :int64 long
                 :float float
                 :double double
                 :byte-array bytes
                 :fixed-length-byte-array bytes
                 :string str
                 :bigint bigint
                 :keyword keyword
                 :symbol symbol)]
    (fn [v]
      (try
        (coerce v)
        (catch Exception e
          (throw (IllegalArgumentException. (format "Could not coerce '%s' into a %s" v (name t)) e)))))))
