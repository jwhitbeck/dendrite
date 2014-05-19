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
           [java.nio.charset Charset]
           [java.math BigInteger]))

(set! *warn-on-reflection* true)

(defprotocol Decoder (decode [decoder]))

(extend-protocol Decoder
  BooleanDecoder
  (decode [boolean-decoder] (.decode boolean-decoder))
  IntDecoder
  (decode [int-decoder] (.decode int-decoder))
  LongDecoder
  (decode [long-decoder] (.decode long-decoder))
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
  IntEncoder
  (encode [int-encoder value] (doto int-encoder (.encode value)))
  LongEncoder
  (encode [long-encoder value] (doto long-encoder (.encode value)))
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
   :int #{:plain :packed-run-length :delta}
   :long #{:plain :delta}
   :float #{:plain}
   :double #{:plain}
   :byte-array #{:plain :incremental :delta-length}
   :fixed-length-byte-array #{:plain}})

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

(defn levels-encoder [max-definition-level]
  (IntFixedBitWidthPackedRunLengthEncoder. (packed-bit-width max-definition-level)))

(defn levels-decoder [^ByteArrayReader byte-array-reader max-definition-level]
  (IntFixedBitWidthPackedRunLengthDecoder. byte-array-reader (packed-bit-width max-definition-level)))

(def ^{:private true :tag Charset} utf8-charset (Charset/forName "UTF-8"))

(defn str->utf8-bytes [^String s] (.getBytes s utf8-charset))

(defn utf8-bytes->str [^bytes bs] (String. bs utf8-charset))

(defn bigint->bytes [^BigInteger bi] (.toByteArray bi))

(defn bytes->bigint [^bytes bs] (BigInteger. bs))

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
        (encode [this v] (encode be (derived->base-type v)) this)
        BufferedByteArrayWriter
        (reset [_] (.reset ^BufferedByteArrayWriter be))
        (finish [_] (.finish ^BufferedByteArrayWriter be))
        (size [_] (.size ^BufferedByteArrayWriter be))
        (estimatedSize [_] (.estimatedSize ^BufferedByteArrayWriter be))
        (writeTo [_ byte-array-writer] (.writeTo ^BufferedByteArrayWriter be byte-array-writer))))))

(defn decoder-ctor [t encoding]
  (if (base-type? t)
    (base-decoder-ctor t encoding)
    (let [bdc (base-decoder-ctor (base-type t) encoding)
          base->derived-type (base->derived-type-fn t)]
      #(let [bd (bdc %)]
         (reify
           Decoder
           (decode [_] (-> (decode bd) base->derived-type)))))))

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
