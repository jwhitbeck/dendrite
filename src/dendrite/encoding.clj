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
            ByteArrayReader]))

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

(defn valid-value-type? [value-type]
  (-> valid-encodings-for-types keys set (contains? value-type)))

(defn valid-encoding-for-type? [value-type encoding]
  (contains? (get valid-encodings-for-types value-type) encoding))

(defn list-encodings-for-type [value-type] (get valid-encodings-for-types value-type))

(defn decoder-ctor [value-type encoding]
  (case value-type
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

(defn encoder [value-type encoding]
  (case value-type
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
