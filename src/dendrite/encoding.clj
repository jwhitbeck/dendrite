(ns dendrite.encoding
  (:import [dendrite.java
            BooleanEncoder BooleanDecoder BooleanPackedEncoder BooleanPackedDecoder
            IntEncoder IntDecoder IntPlainEncoder IntPlainDecoder
            IntPackedDeltaEncoder IntPackedDeltaDecoder
            IntFixedBitWidthPackedRunLengthEncoder IntFixedBitWidthPackedRunLengthDecoder
            IntPackedRunLengthEncoder IntPackedRunLengthDecoder
            LongEncoder LongDecoder LongPlainEncoder LongPlainDecoder
            LongPackedDeltaEncoder LongPackedDeltaDecoder
            FloatEncoder FloatDecoder FloatPlainEncoder FloatPlainDecoder
            DoubleEncoder DoubleDecoder DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayEncoder FixedLengthByteArrayDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayEncoder ByteArrayDecoder ByteArrayPlainEncoder ByteArrayPlainDecoder
            ByteArrayIncrementalEncoder ByteArrayIncrementalDecoder
            ByteArrayDeltaLengthEncoder ByteArrayDeltaLengthDecoder
            ByteArrayReader BufferedByteArrayWriter]
           [java.nio.charset Charset]))

(set! *warn-on-reflection* true)

(defprotocol Decoder (decode-value [decoder]))

(extend-protocol Decoder
  BooleanDecoder
  (decode-value [boolean-decoder] (.decode boolean-decoder))
  IntDecoder
  (decode-value [int-decoder] (.decode int-decoder))
  LongDecoder
  (decode-value [long-decoder] (.decode long-decoder))
  FloatDecoder
  (decode-value [float-decoder] (.decode float-decoder))
  DoubleDecoder
  (decode-value [double-decoder] (.decode double-decoder))
  FixedLengthByteArrayDecoder
  (decode-value [fixed-length-byte-array-decoder] (.decode fixed-length-byte-array-decoder))
  ByteArrayDecoder
  (decode-value [byte-array-decoder] (.decode byte-array-decoder)))

(defn decode [decoder]
  (lazy-seq
   (cons (decode-value decoder) (decode decoder))))

(defprotocol Encoder (encode-value! [encoder value]))

(extend-protocol Encoder
  BooleanEncoder
  (encode-value! [boolean-encoder value] (doto boolean-encoder (.encode value)))
  IntEncoder
  (encode-value! [int-encoder value] (doto int-encoder (.encode value)))
  LongEncoder
  (encode-value! [long-encoder value] (doto long-encoder (.encode value)))
  FloatEncoder
  (encode-value! [float-encoder value] (doto float-encoder (.encode value)))
  DoubleEncoder
  (encode-value! [double-encoder value] (doto double-encoder (.encode value)))
  FixedLengthByteArrayEncoder
  (encode-value! [fixed-length-byte-array-encoder value]
    (doto fixed-length-byte-array-encoder (.encode value)))
  ByteArrayEncoder
  (encode-value! [byte-array-encoder value] (doto byte-array-encoder (.encode value))))

(def ^:private valid-encodings-for-types
  {:boolean #{:plain :dictionary}
   :int #{:plain :packed-run-length :delta :dictionary}
   :long #{:plain :delta :dictionary}
   :float #{:plain :dictionary}
   :double #{:plain :dictionary}
   :byte-array #{:plain :incremental :delta-length :dictionary}
   :fixed-length-byte-array #{:plain :dictionary}})

(defn- base-type? [t] (contains? valid-encodings-for-types t))

(defn- valid-base-value-type? [base-type]
  (-> valid-encodings-for-types keys set (contains? base-type)))

(defn- valid-encoding-for-base-type? [base-type encoding]
  (contains? (get valid-encodings-for-types base-type) encoding))

(defn- list-encodings-for-base-type [base-type] (get valid-encodings-for-types base-type))

(defn- base-decoder-ctor [base-type encoding]
  (case base-type
    :boolean #(BooleanPackedDecoder. %)
    :int (case encoding
             :plain #(IntPlainDecoder. %)
             :packed-run-length #(IntPackedRunLengthDecoder. %)
             :delta #(IntPackedDeltaDecoder. %))
    :long (case encoding
             :plain #(LongPlainDecoder. %)
             :delta #(LongPackedDeltaDecoder. %))
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
    :int (case encoding
             :plain (IntPlainEncoder.)
             :packed-run-length (IntPackedRunLengthEncoder.)
             :delta (IntPackedDeltaEncoder.))
    :long (case encoding
             :plain (LongPlainEncoder.)
             :delta (LongPackedDeltaEncoder.))
    :float (FloatPlainEncoder.)
    :double (DoublePlainEncoder.)
    :byte-array (case encoding
                  :plain (ByteArrayPlainEncoder.)
                  :incremental (ByteArrayIncrementalEncoder.)
                  :delta-length (ByteArrayDeltaLengthEncoder.))
    :fixed-length-byte-array (FixedLengthByteArrayPlainEncoder.)))

(defn- packed-bit-width [n] (- 32 (Integer/numberOfLeadingZeros n)))

(defn levels-encoder [max-level]
  (IntFixedBitWidthPackedRunLengthEncoder. (packed-bit-width max-level)))

(defn levels-decoder [^ByteArrayReader byte-array-reader max-level]
  (IntFixedBitWidthPackedRunLengthDecoder. byte-array-reader (packed-bit-width max-level)))

(def ^{:private true :tag Charset} utf8-charset (Charset/forName "UTF-8"))

(defn str->utf8-bytes [^String s] (.getBytes s utf8-charset))

(defn utf8-bytes->str [^bytes bs] (String. bs utf8-charset))

(defn bigint->bytes [^clojure.lang.BigInt bi] (-> bi .toBigInteger .toByteArray))

(defn bytes->bigint [^bytes bs] (bigint (BigInteger. bs)))

(defn bigdec->bytes [^BigDecimal bd]
  (let [unscaled-bigint-bytes (-> bd .unscaledValue .toByteArray)
        scale (.scale bd)
        ba (byte-array (+ (alength unscaled-bigint-bytes) 4))]
    (System/arraycopy unscaled-bigint-bytes 0 ba 4 (alength unscaled-bigint-bytes))
    (aset ba 3 (unchecked-byte scale))
    (aset ba 2 (unchecked-byte (bit-shift-right scale 8)))
    (aset ba 1 (unchecked-byte (bit-shift-right scale 16)))
    (aset ba 0 (unchecked-byte (bit-shift-right scale 24)))
    ba))

(defn bytes->bigdec [^bytes bs]
  (let [scale (bit-or (bit-shift-left (aget bs 0) 24)
                      (bit-shift-left (aget bs 1) 16)
                      (bit-shift-left (aget bs 2) 8)
                      (aget bs 3))
        unscaled-length (- (alength bs) 4)
        unscaled-bigint-bytes (byte-array unscaled-length)]
    (System/arraycopy bs 4 unscaled-bigint-bytes 0 unscaled-length)
    (BigDecimal. (BigInteger. unscaled-bigint-bytes) scale)))

(def ^:private derived-types
  {:string {:base-type :byte-array
            :coercion-fn str
            :to-base-type-fn str->utf8-bytes
            :from-base-type-fn utf8-bytes->str}
   :char {:base-type :int
          :coercion-fn char
          :to-base-type-fn int
          :from-base-type-fn char}
   :bigint {:base-type :byte-array
            :coercion-fn bigint
            :to-base-type-fn bigint->bytes
            :from-base-type-fn bytes->bigint}
   :bigdec {:base-type :byte-array
            :coercion-fn bigdec
            :to-base-type-fn bigdec->bytes
            :from-base-type-fn bytes->bigdec}
   :keyword {:base-type :byte-array
             :coercion-fn keyword
             :to-base-type-fn (comp str->utf8-bytes name)
             :from-base-type-fn (comp keyword utf8-bytes->str)}
   :symbol {:base-type :byte-array
            :coercion-fn symbol
            :to-base-type-fn (comp str->utf8-bytes name)
            :from-base-type-fn (comp symbol utf8-bytes->str)}})

(def ^:dynamic *custom-types* {})

(defn- all-derived-types [] (merge *custom-types* derived-types))

(defn- type-hierarchy [t]
  (lazy-seq
   (if (base-type? t)
     [t]
     (when-let [next-type (get-in (all-derived-types) [t :base-type])]
       (cons t (type-hierarchy next-type))))))

(defn- base-type [t] (last (type-hierarchy t)))

(defn valid-value-type? [t]
  (valid-base-value-type? (base-type t)))

(defn valid-encoding-for-type? [t encoding]
  (valid-encoding-for-base-type? (base-type t) encoding))

(defn list-encodings-for-type [t]
  (list-encodings-for-base-type (base-type t)))

(defn- derived->base-type-fn [t]
  (->> (map #(get-in (all-derived-types) [% :to-base-type-fn]) (type-hierarchy t))
       butlast
       reverse
       (apply comp)))

(defn- base->derived-type-fn [t]
  (->> (map #(get-in (all-derived-types) [% :from-base-type-fn]) (type-hierarchy t))
       butlast
       (apply comp)))

(defn encoder [t encoding]
  (if (base-type? t)
    (base-encoder t encoding)
    (let [be (base-encoder (base-type t) encoding)
          derived->base-type (derived->base-type-fn t)]
      (reify
        Encoder
        (encode-value! [this v] (encode-value! be (derived->base-type v)) this)
        BufferedByteArrayWriter
        (reset [_] (.reset ^BufferedByteArrayWriter be))
        (finish [_] (.finish ^BufferedByteArrayWriter be))
        (length [_] (.length ^BufferedByteArrayWriter be))
        (estimatedLength [_] (.estimatedLength ^BufferedByteArrayWriter be))
        (writeTo [_ byte-array-writer] (.writeTo ^BufferedByteArrayWriter be byte-array-writer))))))

(defn decoder-ctor [t encoding]
  (if (base-type? t)
    (base-decoder-ctor t encoding)
    (let [bdc (base-decoder-ctor (base-type t) encoding)
          base->derived-type (base->derived-type-fn t)]
      #(let [bd (bdc %)]
         (reify
           Decoder
           (decode-value [_] (-> (decode-value bd) base->derived-type)))))))

(defn coercion-fn [t]
  (let [coerce (if-not (base-type? t)
                 (get-in (all-derived-types) [t :coercion-fn] identity)
                 (case t
                   :boolean boolean
                   :int int
                   :long long
                   :float float
                   :double double
                   :byte-array byte-array
                   :fixed-length-byte-array byte-array))]
    (fn [v]
      (try
        (coerce v)
        (catch Exception e
          (throw (IllegalArgumentException. (format "Could not coerce '%s' into a %s" v (name t)) e)))))))
