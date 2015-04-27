;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.encoding
  (:require [dendrite.utils :as utils])
  (:import [clojure.lang Keyword Ratio]
           [dendrite.java
            IEncoder IDecoder
            BooleanPacked$Encoder BooleanPacked$Decoder
            IntPlain$Encoder IntPlain$Decoder
            IntVLQ$Encoder IntVLQ$Decoder
            IntZigZag$Encoder IntZigZag$Decoder
            IntPackedDelta$Encoder IntPackedDelta$Decoder
            IntFixedBitWidthPackedRunLength$Encoder IntFixedBitWidthPackedRunLength$Decoder
            IntPackedRunLength$Encoder IntPackedRunLength$Decoder
            LongPlain$Encoder LongPlain$Decoder
            LongVLQ$Encoder LongVLQ$Decoder
            LongZigZag$Encoder LongZigZag$Decoder
            LongPackedDelta$Encoder LongPackedDelta$Decoder
            FloatPlain$Encoder FloatPlain$Decoder
            DoublePlain$Encoder DoublePlain$Decoder
            FixedLengthByteArrayPlain$Encoder FixedLengthByteArrayPlain$Decoder
            ByteArrayPlain$Encoder ByteArrayPlain$Decoder
            ByteArrayIncremental$Encoder ByteArrayIncremental$Decoder
            ByteArrayDeltaLength$Encoder ByteArrayDeltaLength$Decoder
            Bytes]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]
           [java.util Date UUID]))

(set! *warn-on-reflection* true)

(def ^:private valid-encodings-for-types
  {:boolean #{:plain :dictionary :frequency}
   :int #{:plain :vlq :zig-zag :packed-run-length :delta :dictionary :frequency}
   :long #{:plain :vlq :zig-zag :delta :dictionary :frequency}
   :float #{:plain :dictionary :frequency}
   :double #{:plain :dictionary :frequency}
   :byte-array #{:plain :incremental :delta-length :dictionary :frequency}
   :fixed-length-byte-array #{:plain :dictionary :frequency}})

(defn- base-type? [t] (contains? valid-encodings-for-types t))

(defn- valid-base-value-type? [base-type]
  (-> valid-encodings-for-types keys set (contains? base-type)))

(defn- valid-encoding-for-base-type? [base-type encoding]
  (contains? (get valid-encodings-for-types base-type) encoding))

(defn- list-encodings-for-base-type [base-type] (get valid-encodings-for-types base-type))

(defn- base-decoder-ctor [base-type encoding]
  (case base-type
    :boolean #(BooleanPacked$Decoder. %)
    :int (case encoding
           :plain #(IntPlain$Decoder. %)
           :vlq #(IntVLQ$Decoder. %)
           :zig-zag #(IntZigZag$Decoder. %)
           :packed-run-length #(IntPackedRunLength$Decoder. %)
           :delta #(IntPackedDelta$Decoder. %))
    :long (case encoding
            :plain #(LongPlain$Decoder. %)
            :vlq #(LongVLQ$Decoder. %)
            :zig-zag #(LongZigZag$Decoder. %)
            :delta #(LongPackedDelta$Decoder. %))
    :float #(FloatPlain$Decoder. %)
    :double #(DoublePlain$Decoder. %)
    :byte-array (case encoding
                  :plain #(ByteArrayPlain$Decoder. %)
                  :incremental #(ByteArrayIncremental$Decoder. %)
                  :delta-length #(ByteArrayDeltaLength$Decoder. %))
    :fixed-length-byte-array #(FixedLengthByteArrayPlain$Decoder. %)))

(defn- base-encoder [base-type encoding]
  (case base-type
    :boolean (BooleanPacked$Encoder.)
    :int (case encoding
           :plain (IntPlain$Encoder.)
           :vlq (IntVLQ$Encoder.)
           :zig-zag (IntZigZag$Encoder.)
           :packed-run-length (IntPackedRunLength$Encoder.)
           :delta (IntPackedDelta$Encoder.))
    :long (case encoding
            :plain (LongPlain$Encoder.)
            :vlq (LongVLQ$Encoder.)
            :zig-zag (LongZigZag$Encoder.)
            :delta (LongPackedDelta$Encoder.))
    :float (FloatPlain$Encoder.)
    :double (DoublePlain$Encoder.)
    :byte-array (case encoding
                  :plain (ByteArrayPlain$Encoder.)
                  :incremental (ByteArrayIncremental$Encoder.)
                  :delta-length (ByteArrayDeltaLength$Encoder.))
    :fixed-length-byte-array (FixedLengthByteArrayPlain$Encoder.)))

(defn levels-encoder [max-level]
  (IntFixedBitWidthPackedRunLength$Encoder. (Bytes/getBitWidth (int max-level))))

(defn levels-decoder [^ByteBuffer byte-buffer max-level]
  (IntFixedBitWidthPackedRunLength$Decoder. byte-buffer (Bytes/getBitWidth (int max-level))))

(def ^{:private true :tag Charset} utf8-charset (Charset/forName "UTF-8"))

(defn str->utf8-bytes [^String s] (.getBytes s utf8-charset))

(defn utf8-bytes->str [^bytes bs] (String. bs utf8-charset))

(defn keyword->str [^Keyword k]
  (if-let [kw-namespace (namespace k)]
    (str kw-namespace "/" (name k))
    (name k)))

(defn bigint->bytes [^clojure.lang.BigInt bi] (.. bi toBigInteger toByteArray))

(defn bytes->bigint [^bytes bs] (bigint (BigInteger. bs)))

(defn bigdec->bytes [^BigDecimal bd]
  (let [unscaled-bigint-bytes (.. bd unscaledValue toByteArray)
        scale (.scale bd)
        bb (ByteBuffer/wrap (byte-array (+ (alength unscaled-bigint-bytes) 4)))]
    (doto bb
      (.putInt scale)
      (.put unscaled-bigint-bytes))
    (.array bb)))

(defn bytes->bigdec [^bytes bs]
  (let [bb (ByteBuffer/wrap bs)
        scale (.getInt bb)
        unscaled-length (- (alength bs) 4)
        unscaled-bigint-bytes (byte-array unscaled-length)]
    (.get bb unscaled-bigint-bytes)
    (BigDecimal. (BigInteger. unscaled-bigint-bytes) scale)))

(defn ratio->bytes [^Ratio r]
  (let [numerator-bytes (.. r numerator toByteArray)
        denominator-bytes (.. r denominator toByteArray)
        numerator-length (alength numerator-bytes)
        denominator-length (alength denominator-bytes)
        bb (ByteBuffer/wrap (byte-array (+ numerator-length denominator-length 4)))]
    (doto bb
      (.putInt numerator-length)
      (.put numerator-bytes)
      (.put denominator-bytes))
    (.array bb)))

(defn bytes->ratio [^bytes bs]
  (let [bb (ByteBuffer/wrap bs)
        numerator-length (.getInt bb)
        denominator-length (- (alength bs) numerator-length 4)
        numerator-bytes (byte-array numerator-length)
        denominator-bytes (byte-array denominator-length)]
    (.get bb numerator-bytes)
    (.get bb denominator-bytes)
    (Ratio. (BigInteger. numerator-bytes) (BigInteger. denominator-bytes))))

(defn ratio [r] (if (ratio? r) r (Ratio. (bigint r) BigInteger/ONE)))

(defn inst [d]
  (if-not (instance? Date d)
    (throw (IllegalArgumentException. (format "%s is not an instance of java.util.Date." d)))
    d))

(defn uuid->bytes [^UUID uuid]
  (let [bb (doto (ByteBuffer/wrap (byte-array 16))
             (.putLong (.getMostSignificantBits uuid))
             (.putLong (.getLeastSignificantBits uuid)))]
    (.array bb)))

(defn bytes->uuid [^bytes bs]
  (let [bb (ByteBuffer/wrap bs)]
    (UUID. (.getLong bb) (.getLong bb))))

(defn uuid [x]
  (if-not (instance? UUID x)
    (throw (IllegalArgumentException. (format "%s is not an instance of java.util.UUID" x)))
    x))

(defn byte-buffer->bytes [^ByteBuffer bb]
  (let [n (- (.limit bb) (.position bb))
        bs (byte-array n)]
    (.mark bb)
    (.get bb bs)
    (.reset bb)
    bs))

(defn bytes->byte-buffer [^bytes bs]
  (ByteBuffer/wrap bs))

(defn byte-buffer [x]
  (if-not (instance? ByteBuffer x)
    (throw (IllegalArgumentException. (format "%s is not an instance of java.nio.ByteBuffer" x)))
    x))

(defrecord DerivedType [base-type coercion-fn to-base-type-fn from-base-type-fn])

(defn- get-derived-type-fn [derived-type-name derived-type-map fn-keyword]
  (if-let [f (get derived-type-map fn-keyword)]
    (if-not (fn? f)
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
   :inst (map->DerivedType {:base-type :long
                            :coercion-fn inst
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
   :keyword (map->DerivedType {:base-type :string
                               :coercion-fn keyword
                               :to-base-type-fn keyword->str
                               :from-base-type-fn keyword})
   :symbol (map->DerivedType {:base-type :string
                              :coercion-fn symbol
                              :to-base-type-fn name
                              :from-base-type-fn symbol})
   :byte-buffer (map->DerivedType {:base-type :byte-array
                                   :coercion-fn byte-buffer
                                   :to-base-type-fn byte-buffer->bytes
                                   :from-base-type-fn bytes->byte-buffer})})

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
    (let [^IEncoder be (base-encoder (base-type ts t) encoding)
          derived->base-type (derived->base-type-fn ts t)]
      (reify
        IEncoder
        (encode [_ v] (.encode be (derived->base-type v)))
        (numEncodedValues [_] (.numEncodedValues be))
        (reset [_] (.reset be))
        (finish [_] (.finish be))
        (length [_] (.length be))
        (estimatedLength [_] (.estimatedLength be))
        (writeTo [_ memory-output-stream] (.writeTo be memory-output-stream))))))

(defn decoder-ctor [ts t encoding]
  (if (base-type? t)
    (base-decoder-ctor t encoding)
    (let [bdc (base-decoder-ctor (base-type ts t) encoding)
          base->derived-type (base->derived-type-fn ts t)]
      #(let [^IDecoder bd (bdc %)]
         (reify
           IDecoder
           (decode [_] (base->derived-type (.decode bd)))
           (numEncodedValues [_] (.numEncodedValues bd)))))))

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
