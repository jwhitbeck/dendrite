(ns dendrite.encodings
  (:import [dendrite.java
            BooleanEncoder BooleanDecoder
            Int32Encoder Int32Decoder
            Int64Encoder Int64Decoder
            FloatEncoder FloatDecoder
            DoubleEncoder DoubleDecoder
            FixedLengthByteArrayEncoder FixedLengthByteArrayDecoder
            ByteArrayEncoder ByteArrayDecoder
            BooleanPackedEncoder BooleanPackedDecoder]))

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
