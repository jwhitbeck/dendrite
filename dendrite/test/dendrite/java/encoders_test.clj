;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.encoders-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java MemoryOutputStream Types
            IEncoder IDecoder
            BooleanPacked$Encoder BooleanPacked$Decoder
            IntPlain$Encoder IntPlain$Decoder
            IntVlq$Encoder IntVlq$Decoder
            IntZigZag$Encoder IntZigZag$Decoder
            IntFixedBitWidthPackedRunLength$Encoder IntFixedBitWidthPackedRunLength$Decoder
            IntPackedRunLength$Encoder IntPackedRunLength$Decoder
            IntPackedDelta$Encoder IntPackedDelta$Decoder
            LongPlain$Encoder LongPlain$Decoder
            LongVlq$Encoder LongVlq$Decoder
            LongZigZag$Encoder LongZigZag$Decoder
            LongPackedDelta$Encoder LongPackedDelta$Decoder
            FloatPlain$Encoder FloatPlain$Decoder DoublePlain$Encoder DoublePlain$Decoder
            FixedLengthByteArrayPlain$Encoder FixedLengthByteArrayPlain$Decoder
            ByteArrayPlain$Encoder ByteArrayPlain$Decoder
            ByteArrayDeltaLength$Encoder ByteArrayDeltaLength$Decoder
            ByteArrayIncremental$Encoder ByteArrayIncremental$Decoder
            Dictionary$Encoder Dictionary$Decoder]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn write-read
  ([encoder-constructor decoder-constructor input-seq]
     (write-read 1000 encoder-constructor decoder-constructor input-seq))
  ([n encoder-constructor decoder-constructor input-seq]
     (let [encoder (encoder-constructor)]
       (doseq [x (take n input-seq)]
         (.encode ^IEncoder encoder x))
       (let [decoder (->> encoder helpers/output-buffer->byte-buffer decoder-constructor)]
         (repeatedly n #(.decode ^IDecoder decoder))))))

(defn test-encode-n-values [n encoder-constructor decoder-constructor input-seq]
  (let [output-seq (write-read n encoder-constructor decoder-constructor input-seq)]
    (is (every? true? (map = output-seq input-seq)))))

(defn test-encoder [encoder-constructor decoder-constructor input-seq]
  (doseq [n [0 1 2 3 4 5 6 7 8 1000]]
    (test-encode-n-values n encoder-constructor decoder-constructor input-seq)))

(defn finish-repeatedly [n encoder-constructor input-seq]
  (let [^IEncoder encoder (encoder-constructor)
        mos (MemoryOutputStream.)]
    (doseq [x (take 10 input-seq)]
      (.encode encoder x))
    (dotimes [_ n] (.finish encoder))
    (.writeTo encoder mos)
    (-> mos .toByteBuffer .array seq)))

(defn test-finish-idempotence [encoder-constructor input-seq]
  (let [finish-fn (fn [n] (finish-repeatedly n encoder-constructor input-seq))]
    (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))

(deftest boolean-encoders
  (testing "packed encoder/decoder"
    (test-encoder #(BooleanPacked$Encoder.) #(BooleanPacked$Decoder. %) (repeatedly helpers/rand-bool)))
  (testing "packed encoder's finish method is idempotent"
    (test-finish-idempotence #(BooleanPacked$Encoder.) (repeatedly helpers/rand-bool))))

(deftest int-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(IntPlain$Encoder.) #(IntPlain$Decoder. %) (repeatedly helpers/rand-int)))
  (testing "vlq encoder/decoder"
    (test-encoder #(IntVlq$Encoder.) #(IntVlq$Decoder. %) (repeatedly helpers/rand-int)))
  (testing "zigzag encoder/decoder"
    (test-encoder #(IntZigZag$Encoder.) #(IntZigZag$Decoder. %) (repeatedly helpers/rand-int)))
  (testing "fixed-bit-width packed run-length encoder/decoder"
    (testing "sparse input"
      (let [rand-ints (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (int (rand-int 8)) (int 0))))]
        (test-encoder #(IntFixedBitWidthPackedRunLength$Encoder. 3)
                      #(IntFixedBitWidthPackedRunLength$Decoder. % 3)
                      rand-ints)))
    (testing "random input"
      (test-encoder #(IntFixedBitWidthPackedRunLength$Encoder. 32)
                    #(IntFixedBitWidthPackedRunLength$Decoder. % 32)
                    (repeatedly helpers/rand-int))))
  (testing "fixed-bit-width packed run-length encoder's finish method is idempotent"
    (test-finish-idempotence #(IntFixedBitWidthPackedRunLength$Encoder. 32) (repeatedly helpers/rand-int)))
  (testing "packed run-length encoder"
    (testing "sparse input"
      (test-encoder #(IntPackedRunLength$Encoder.)
                    #(IntPackedRunLength$Decoder. %)
                    (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (int (rand-int 8)) (int 0))))))
    (testing "random input"
      (test-encoder #(IntPackedRunLength$Encoder.)
                    #(IntPackedRunLength$Decoder. %)
                    (repeatedly helpers/rand-int))))
  (testing "packed run-length encoder's finish method is idempotent"
    (test-finish-idempotence #(IntPackedRunLength$Encoder.) (repeatedly helpers/rand-int)))
  (testing "packed delta encoder/decoder"
    (test-encoder #(IntPackedDelta$Encoder.)
                  #(IntPackedDelta$Decoder. %)
                  (repeatedly helpers/rand-int)))
  (testing "packed delta encoder's finish method is idempotent"
    (test-finish-idempotence #(IntPackedDelta$Encoder.) (repeatedly helpers/rand-int))))

(deftest long-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(LongPlain$Encoder.) #(LongPlain$Decoder. %) (repeatedly helpers/rand-long)))
  (testing "vlq encoder/decoder"
    (test-encoder #(LongVlq$Encoder.) #(LongVlq$Decoder. %) (repeatedly helpers/rand-long)))
  (testing "zigzag encoder/decoder"
    (test-encoder #(LongZigZag$Encoder.) #(LongZigZag$Decoder. %) (repeatedly helpers/rand-long)))
  (testing "packed delta encoder/decoder"
    (test-encoder #(LongPackedDelta$Encoder.) #(LongPackedDelta$Decoder. %) (repeatedly helpers/rand-long)))
  (testing "packed delta encoder's finish method is idempotent"
    (let [rand-longs (repeatedly helpers/rand-long)
          finish-fn (fn [n] (finish-repeatedly n #(LongPackedDelta$Encoder.) rand-longs))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest float-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(FloatPlain$Encoder.) #(FloatPlain$Decoder. %) (repeatedly helpers/rand-float))))

(deftest double-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(DoublePlain$Encoder.) #(DoublePlain$Decoder. %) (repeatedly helpers/rand-double))))

(deftest fixed-length-byte-array-encoders
  (testing "length byte array plain encoder/decoder"
    (let [length 10
          rand-byte-arrays (repeatedly #(helpers/rand-byte-array length))
          read-byte-arrays (write-read #(FixedLengthByteArrayPlain$Encoder.)
                                       #(FixedLengthByteArrayPlain$Decoder. %)
                                       rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays)))))))

(deftest byte-array-encoders
  (testing "plain encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayPlain$Encoder.) #(ByteArrayPlain$Decoder. %) rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays))))))
  (testing "delta-length encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayDeltaLength$Encoder.)
                                       #(ByteArrayDeltaLength$Decoder. %) rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays))))))
  (testing "delta-length encoder's finish method is idempotent"
    (test-finish-idempotence #(ByteArrayDeltaLength$Encoder.) (repeatedly helpers/rand-byte-array)))
  (testing "incremental encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayIncremental$Encoder.)
                                       #(ByteArrayIncremental$Decoder. %) rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays))))))
  (testing "incremental encoder's finish method is idempotent"
    (test-finish-idempotence #(ByteArrayIncremental$Encoder.) (repeatedly helpers/rand-byte-array))))

(deftest dictionary-encoders
  (testing "small set of integers"
    (let [rand-int-set (repeatedly 10 helpers/rand-int)
          n 1000
          rand-ints (repeatedly n #(rand-nth rand-int-set))
          enc (Dictionary$Encoder/create Types/INT Types/VLQ)
          mos (MemoryOutputStream.)]
      (doseq [i rand-ints]
        (.encode enc i))
      (.write mos enc)
      (let [dictionary (.getDictionary enc)
            dec (Dictionary$Decoder. (IntVlq$Decoder. (.toByteBuffer mos)) dictionary)
            read-ints (repeatedly n #(.decode dec))]
        (is (= read-ints rand-ints)))))
  (testing "byte-array encoding"
    (let [byte-arrays (repeatedly 10 helpers/rand-byte-array)
          n 1000
          rand-byte-arrays (repeatedly n #(rand-nth byte-arrays))
          enc (Dictionary$Encoder/create Types/BYTE_ARRAY Types/VLQ)
          mos (MemoryOutputStream.)]
      (doseq [ba rand-byte-arrays]
        (.encode enc ba))
      (.write mos enc)
      (let [dictionary (into-array (.getDictionary enc))
            dec (Dictionary$Decoder. (IntVlq$Decoder. (.toByteBuffer mos)) dictionary)
            read-byte-arrays (repeatedly n #(.decode dec))]
        (is (= (map seq read-byte-arrays) (map seq rand-byte-arrays)))))))
