(ns dendrite.encoding
  (:import [dendrite.java
            Encoder Decoder
            BooleanPackedEncoder BooleanPackedDecoder
            IntPlainEncoder IntPlainDecoder
            IntPackedDeltaEncoder IntPackedDeltaDecoder
            IntFixedBitWidthPackedRunLengthEncoder IntFixedBitWidthPackedRunLengthDecoder
            IntPackedRunLengthEncoder IntPackedRunLengthDecoder
            LongPlainEncoder LongPlainDecoder
            LongPackedDeltaEncoder LongPackedDeltaDecoder
            FloatPlainEncoder FloatPlainDecoder
            DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayPlainEncoder ByteArrayPlainDecoder
            ByteArrayIncrementalEncoder ByteArrayIncrementalDecoder
            ByteArrayDeltaLengthEncoder ByteArrayDeltaLengthDecoder
            ByteArrayReader BufferedByteArrayWriter]
           [java.nio.charset Charset]))

(set! *warn-on-reflection* true)

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

(defn- build-type-hierarchy [t all-derived-types]
  (if (base-type? t)
    [t]
    (when-let [next-type (get-in all-derived-types [t :base-type])]
      (cons t (build-type-hierarchy next-type all-derived-types)))))

(defn- build-type-hierarchy-map [all-types]
  (reduce-kv (fn [m t _] (assoc m t (build-type-hierarchy t all-types))) {} all-types))

(def ^:dynamic *derived-type-hierarchies* (build-type-hierarchy-map derived-types))
(def ^:dynamic *derived-types* derived-types)

(defmacro with-custom-types [custom-derived-types & body]
  `(let [all-derived-types# (merge ~custom-derived-types @#'derived-types)]
     (binding [*derived-types* all-derived-types#
               *derived-type-hierarchies* (#'build-type-hierarchy-map all-derived-types#)]
       ~@body)))

(defn- type-hierarchy [t]
  (if (base-type? t)
    [t]
    (get *derived-type-hierarchies* t)))

(defn base-type [t] (last (type-hierarchy t)))

(defn valid-value-type? [t]
  (valid-base-value-type? (base-type t)))

(defn valid-encoding-for-type? [t encoding]
  (valid-encoding-for-base-type? (base-type t) encoding))

(defn list-encodings-for-type [t]
  (list-encodings-for-base-type (base-type t)))

(defn- derived->base-type-fn [t]
  (let [f (->> (map #(get-in *derived-types* [% :to-base-type-fn]) (type-hierarchy t))
               butlast
               reverse
               (apply comp))]
    (fn [x] (try (f x)
                 (catch Exception e
                   (throw (IllegalArgumentException.
                           (format "Error while converting value '%s' from type '%s' to type '%s'"
                                   x (name t) (name (base-type t))) e)))))))

(defn- base->derived-type-fn [t]
  (let [f (->> (map #(get-in *derived-types* [% :from-base-type-fn]) (type-hierarchy t))
               butlast
               (apply comp))]
    (fn [x] (try (f x)
                 (catch Exception e
                   (throw (IllegalArgumentException.
                           (format "Error while converting value '%s' from type '%s' to type '%s'"
                                   x (name (base-type t)) (name t)) e)))))))

(defn encoder [t encoding]
  (if (base-type? t)
    (base-encoder t encoding)
    (let [^Encoder be (base-encoder (base-type t) encoding)
          derived->base-type (derived->base-type-fn t)]
      (reify
        Encoder
        (encode [_ v] (.encode be (derived->base-type v)))
        (numEncodedValues [_] (.numEncodedValues be))
        (reset [_] (.reset be))
        (finish [_] (.finish be))
        (length [_] (.length be))
        (estimatedLength [_] (.estimatedLength be))
        (flush [_ byte-array-writer] (.flush be byte-array-writer))))))

(defn decoder-ctor [t encoding]
  (if (base-type? t)
    (base-decoder-ctor t encoding)
    (let [bdc (base-decoder-ctor (base-type t) encoding)
          base->derived-type (base->derived-type-fn t)]
      #(let [^Decoder bd (bdc %)]
         (reify
           Decoder
           (decode [_] (base->derived-type (.decode bd)))
           (numEncodedValues [_] (.numEncodedValues bd))
           (iterator [_] (let [i (.iterator bd)]
                           (reify java.util.Iterator
                             (hasNext [_] (.hasNext i))
                             (next [_] (base->derived-type (.next i)))
                             (remove [_] (.remove i))))))))))

(defn coercion-fn [t]
  (let [coerce (if-not (base-type? t)
                 (get-in *derived-types* [t :coercion-fn] identity)
                 (case t
                   :boolean boolean
                   :int int
                   :long long
                   :float float
                   :double double
                   :byte-array byte-array
                   :fixed-length-byte-array byte-array))]
    (when-not coerce
      (throw
       (IllegalArgumentException.
        (format "Could not find coercion-fn for type '%s'. Perhaps :custom-types is misconfigured." t))))
    (fn [v]
      (try
        (coerce v)
        (catch Exception e
          (throw (IllegalArgumentException. (format "Could not coerce '%s' into a %s" v (name t)) e)))))))
