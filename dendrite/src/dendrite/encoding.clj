(ns dendrite.encoding
  (:require [dendrite.utils :as utils])
  (:import [clojure.lang Keyword Ratio]
           [dendrite.java
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
            ByteArrayReader ByteArrayWriter BufferedByteArrayWriter]
           [java.nio.charset Charset]
           [java.util Date UUID]))

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

(defn keyword->str [^Keyword k]
  (if-let [kw-namespace (namespace k)]
    (str kw-namespace "/" (name k))
    (name k)))

(defn bigint->bytes [^clojure.lang.BigInt bi] (.. bi toBigInteger toByteArray))

(defn bytes->bigint [^bytes bs] (bigint (BigInteger. bs)))

(defn- byte-array-set-int [^bytes bs i v]
  (aset bs (+ i 3) (unchecked-byte v))
  (aset bs (+ i 2) (unchecked-byte (bit-shift-right v 8)))
  (aset bs (inc i) (unchecked-byte (bit-shift-right v 16)))
  (aset bs i (unchecked-byte (bit-shift-right v 24))))

(defn- byte-array-get-int [^bytes bs i]
  (bit-or (bit-shift-left (aget bs i) 24)
          (bit-shift-left (aget bs (inc i)) 16)
          (bit-shift-left (aget bs (+ i 2)) 8)
          (aget bs (+ i 3))))

(defn bigdec->bytes [^BigDecimal bd]
  (let [unscaled-bigint-bytes (.. bd unscaledValue toByteArray)
        scale (.scale bd)
        ba (byte-array (+ (alength unscaled-bigint-bytes) 4))]
    (byte-array-set-int ba 0 scale)
    (System/arraycopy unscaled-bigint-bytes 0 ba 4 (alength unscaled-bigint-bytes))
    ba))

(defn bytes->bigdec [^bytes bs]
  (let [scale (int (byte-array-get-int bs 0))
        unscaled-length (- (alength bs) 4)
        unscaled-bigint-bytes (byte-array unscaled-length)]
    (System/arraycopy bs 4 unscaled-bigint-bytes 0 unscaled-length)
    (BigDecimal. (BigInteger. unscaled-bigint-bytes) scale)))

(defn ratio->bytes [^Ratio r]
  (let [numerator-bytes (.. r numerator toByteArray)
        denominator-bytes (.. r denominator toByteArray)
        numerator-length (alength numerator-bytes)
        denominator-length (alength denominator-bytes)
        ba (byte-array (+ numerator-length denominator-length 4))]
    (byte-array-set-int ba 0 numerator-length)
    (System/arraycopy numerator-bytes 0 ba 4 numerator-length)
    (System/arraycopy denominator-bytes 0 ba (+ 4 numerator-length) denominator-length)
    ba))

(defn bytes->ratio [^bytes bs]
  (let [numerator-length (int (byte-array-get-int bs 0))
        denominator-length (- (alength bs) numerator-length 4)
        numerator-bytes (byte-array numerator-length)
        denominator-bytes (byte-array denominator-length)]
    (System/arraycopy bs 4 numerator-bytes 0 numerator-length)
    (System/arraycopy bs (+ 4 numerator-length) denominator-bytes 0 denominator-length)
    (Ratio. (BigInteger. numerator-bytes) (BigInteger. denominator-bytes))))

(defn ratio [r] (if (ratio? r) r (Ratio. (bigint r) BigInteger/ONE)))

(defn date [d]
  (if-not (instance? Date d)
    (throw (IllegalArgumentException. (format "%s is not an instance of java.util.Date." d)))
    d))

(defn uuid->bytes [^UUID uuid]
  (let [baw (doto (ByteArrayWriter. 16)
              (.writeFixedLong (.getMostSignificantBits uuid))
              (.writeFixedLong (.getLeastSignificantBits uuid)))]
    (.buffer baw)))

(defn bytes->uuid [^bytes bs]
  (let [bar (ByteArrayReader. bs)]
    (UUID. (.readFixedLong bar) (.readFixedLong bar))))

(defn uuid [x]
  (if-not (instance? UUID x)
    (throw (IllegalArgumentException. (format "%s is not an instance of java.util.UUID" x)))
    x))

(defrecord DerivedType [base-type coercion-fn to-base-type-fn from-base-type-fn])

(defn- get-derived-type-fn [derived-type-name derived-type-map fn-keyword]
  (if-let [f (get derived-type-map fn-keyword)]
    (if-not (utils/callable? f)
      (throw (IllegalArgumentException.
              (format "%s expects a function for type '%s'." fn-keyword (name derived-type-name))))
      f)
    (do (utils/warn (format (str "%s is not defined for type '%s', defaulting to clojure.core/identity.")
                            fn-keyword (name derived-type-name)))
        identity)))

(defn derived-type [derived-type-name derived-type-map]
  (doseq [k (keys derived-type-map)]
    (when-not (#{:base-type :coercion-fn :to-base-type-fn :from-base-type-fn} k)
      (throw (IllegalArgumentException.
              (format (str "Key %s is not a valid derived-type key. Valid keys are "
                           ":base-type, :coercion-fn, :to-base-type-fn, and :from-base-type-fn") k)))))
  (map->DerivedType
   {:base-type (or (:base-type derived-type-map)
                   (throw (IllegalArgumentException.
                           (format "required field :base-type is missing for type '%s'."
                                   (name derived-type-name)))))
    :coercion-fn (get-derived-type-fn derived-type-name derived-type-map :coercion-fn)
    :to-base-type-fn (get-derived-type-fn derived-type-name derived-type-map :to-base-type-fn)
    :from-base-type-fn (get-derived-type-fn derived-type-name derived-type-map :from-base-type-fn)}))

(def ^:private derived-types
  {:string (map->DerivedType {:base-type :byte-array
                              :coercion-fn str
                              :to-base-type-fn str->utf8-bytes
                              :from-base-type-fn utf8-bytes->str})
   :date (map->DerivedType {:base-type :long
                            :coercion-fn date
                            :to-base-type-fn (fn [^Date d] (.getTime d))
                            :from-base-type-fn (fn [^long l] (Date. l))})
   :uuid (map->DerivedType {:base-type :fixed-length-byte-array
                            :coercion-fn uuid
                            :to-base-type-fn uuid->bytes
                            :from-base-type-fn bytes->uuid})
   :char (map->DerivedType {:base-type :int
                            :coercion-fn char
                            :to-base-type-fn int
                            :from-base-type-fn char})
   :bigint (map->DerivedType {:base-type :byte-array
                              :coercion-fn bigint
                              :to-base-type-fn bigint->bytes
                              :from-base-type-fn bytes->bigint})
   :bigdec (map->DerivedType {:base-type :byte-array
                              :coercion-fn bigdec
                              :to-base-type-fn bigdec->bytes
                              :from-base-type-fn bytes->bigdec})
   :ratio (map->DerivedType {:base-type :byte-array
                             :coercion-fn ratio
                             :to-base-type-fn ratio->bytes
                             :from-base-type-fn bytes->ratio})
   :keyword (map->DerivedType {:base-type :byte-array
                               :coercion-fn keyword
                               :to-base-type-fn (comp str->utf8-bytes keyword->str)
                               :from-base-type-fn (comp keyword utf8-bytes->str)})
   :symbol (map->DerivedType {:base-type :byte-array
                              :coercion-fn symbol
                              :to-base-type-fn (comp str->utf8-bytes name)
                              :from-base-type-fn (comp symbol utf8-bytes->str)})})

(defn- build-type-hierarchy [t all-derived-types]
  (if (base-type? t)
    [t]
    (when-let [next-type (get-in all-derived-types [t :base-type])]
      (cons t (build-type-hierarchy next-type all-derived-types)))))

(defn- build-type-hierarchy-map [all-types]
  (reduce-kv (fn [m t _] (assoc m t (vec (build-type-hierarchy t all-types)))) {} all-types))

(defrecord TypeStore [derived-types derived-type-hierarchies])

(defn parse-custom-derived-types [custom-derived-types]
  (reduce-kv (fn [m t ct] (assoc m t (derived-type t ct))) {} custom-derived-types))

(defn type-store [custom-derived-types]
  (let [all-derived-types (merge custom-derived-types derived-types)]
    (->TypeStore all-derived-types (build-type-hierarchy-map all-derived-types))))

(defn- type-hierarchy [ts t]
  (if (base-type? t)
    [t]
    (get-in ts [:derived-type-hierarchies t])))

(defn base-type [ts t] (last (type-hierarchy ts t)))

(defn valid-value-type? [ts t]
  (valid-base-value-type? (base-type ts t)))

(defn valid-encoding-for-type? [ts t encoding]
  (valid-encoding-for-base-type? (base-type ts t) encoding))

(defn list-encodings-for-type [ts t]
  (list-encodings-for-base-type (base-type ts t)))

(defn- derived->base-type-fn [ts t]
  (let [f (->> (map #(get-in ts [:derived-types % :to-base-type-fn]) (type-hierarchy ts t))
               butlast
               reverse
               (apply comp))]
    (fn [x] (try (f x)
                 (catch Exception e
                   (throw (IllegalArgumentException.
                           (format "Error while converting value '%s' from type '%s' to type '%s'"
                                   x (name t) (name (base-type ts t))) e)))))))

(defn- base->derived-type-fn [ts t]
  (let [f (->> (map #(get-in ts [:derived-types % :from-base-type-fn]) (type-hierarchy ts t))
               butlast
               (apply comp))]
    (fn [x] (try (f x)
                 (catch Exception e
                   (throw (IllegalArgumentException.
                           (format "Error while converting value '%s' from type '%s' to type '%s'"
                                   x (name (base-type ts t)) (name t)) e)))))))

(defn encoder [ts t encoding]
  (if (base-type? t)
    (base-encoder t encoding)
    (let [^Encoder be (base-encoder (base-type ts t) encoding)
          derived->base-type (derived->base-type-fn ts t)]
      (reify
        Encoder
        (encode [_ v] (.encode be (derived->base-type v)))
        (numEncodedValues [_] (.numEncodedValues be))
        (reset [_] (.reset be))
        (finish [_] (.finish be))
        (length [_] (.length be))
        (estimatedLength [_] (.estimatedLength be))
        (flush [_ byte-array-writer] (.flush be byte-array-writer))))))

(defn decoder-ctor [ts t encoding]
  (if (base-type? t)
    (base-decoder-ctor t encoding)
    (let [bdc (base-decoder-ctor (base-type ts t) encoding)
          base->derived-type (base->derived-type-fn ts t)]
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

(defn coercion-fn [ts t]
  (let [coerce (if-not (base-type? t)
                 (get-in ts [:derived-types t :coercion-fn] identity)
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
