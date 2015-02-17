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
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java MemoryOutputStream
            Encoder Decoder
            BooleanPackedEncoder BooleanPackedDecoder
            IntPlainEncoder IntPlainDecoder
            IntVLQEncoder IntVLQDecoder
            IntZigZagEncoder IntZigZagDecoder
            IntFixedBitWidthPackedRunLengthEncoder IntFixedBitWidthPackedRunLengthDecoder
            IntPackedRunLengthEncoder IntPackedRunLengthDecoder
            IntPackedDeltaEncoder IntPackedDeltaDecoder
            LongPlainEncoder LongPlainDecoder
            LongVLQEncoder LongVLQDecoder
            LongZigZagEncoder LongZigZagDecoder
            LongPackedDeltaEncoder LongPackedDeltaDecoder
            FloatPlainEncoder FloatPlainDecoder DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayPlainEncoder ByteArrayPlainDecoder
            ByteArrayDeltaLengthEncoder ByteArrayDeltaLengthDecoder
            ByteArrayIncrementalEncoder ByteArrayIncrementalDecoder]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn write-read
  ([encoder-constructor decoder-constructor input-seq]
     (write-read 1000 encoder-constructor decoder-constructor input-seq))
  ([n encoder-constructor decoder-constructor input-seq]
     (let [encoder (encoder-constructor)]
       (doseq [x (take n input-seq)]
         (.encode ^Encoder encoder x))
       (let [decoder (->> encoder helpers/output-buffer->byte-buffer decoder-constructor)]
         (repeatedly n #(.decode ^Decoder decoder))))))

(defn test-encode-n-values [n encoder-constructor decoder-constructor input-seq]
  (let [output-seq (write-read n encoder-constructor decoder-constructor input-seq)]
    (is (every? true? (map = output-seq input-seq)))))

(defn test-encoder [encoder-constructor decoder-constructor input-seq]
  (doseq [n [0 1 2 3 4 5 6 7 8 1000]]
    (test-encode-n-values n encoder-constructor decoder-constructor input-seq)))

(defn finish-repeatedly [n encoder-constructor input-seq]
  (let [^Encoder encoder (encoder-constructor)
        mos (MemoryOutputStream.)]
    (doseq [x (take 10 input-seq)]
      (.encode encoder x))
    (dotimes [_ n] (.finish encoder))
    (.writeTo encoder mos)
    (-> mos .byteBuffer .array seq)))

(defn test-finish-idempotence [encoder-constructor input-seq]
  (let [finish-fn (fn [n] (finish-repeatedly n encoder-constructor input-seq))]
    (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))

(deftest boolean-encoders
  (testing "packed encoder/decoder"
    (test-encoder #(BooleanPackedEncoder.) #(BooleanPackedDecoder. %) (repeatedly helpers/rand-bool)))
  (testing "packed encoder's finish method is idempotent"
    (test-finish-idempotence #(BooleanPackedEncoder.) (repeatedly helpers/rand-bool))))

(deftest int-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(IntPlainEncoder.) #(IntPlainDecoder. %) (repeatedly helpers/rand-int)))
  (testing "vlq encoder/decoder"
    (test-encoder #(IntVLQEncoder.) #(IntVLQDecoder. %) (repeatedly helpers/rand-int)))
  (testing "zigzag encoder/decoder"
    (test-encoder #(IntZigZagEncoder.) #(IntZigZagDecoder. %) (repeatedly helpers/rand-int)))
  (testing "fixed-bit-width packed run-length encoder/decoder"
    (testing "sparse input"
      (let [rand-ints (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (int (rand-int 8)) (int 0))))]
        (test-encoder #(IntFixedBitWidthPackedRunLengthEncoder. 3)
                      #(IntFixedBitWidthPackedRunLengthDecoder. % 3)
                      rand-ints)))
    (testing "random input"
      (test-encoder #(IntFixedBitWidthPackedRunLengthEncoder. 32)
                    #(IntFixedBitWidthPackedRunLengthDecoder. % 32)
                    (repeatedly helpers/rand-int))))
  (testing "fixed-bit-width packed run-length encoder's finish method is idempotent"
    (test-finish-idempotence #(IntFixedBitWidthPackedRunLengthEncoder. 32) (repeatedly helpers/rand-int)))
  (testing "packed run-length encoder"
    (testing "sparse input"
      (test-encoder #(IntPackedRunLengthEncoder.)
                    #(IntPackedRunLengthDecoder. %)
                    (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (int (rand-int 8)) (int 0))))))
    (testing "random input"
      (test-encoder #(IntPackedRunLengthEncoder.)
                    #(IntPackedRunLengthDecoder. %)
                    (repeatedly helpers/rand-int))))
  (testing "packed run-length encoder's finish method is idempotent"
    (test-finish-idempotence #(IntPackedRunLengthEncoder.) (repeatedly helpers/rand-int)))
  (testing "packed delta encoder/decoder"
    (test-encoder #(IntPackedDeltaEncoder.)
                  #(IntPackedDeltaDecoder. %)
                  (repeatedly helpers/rand-int)))
  (testing "packed delta encoder's finish method is idempotent"
    (test-finish-idempotence #(IntPackedDeltaEncoder.) (repeatedly helpers/rand-int))))

(deftest long-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(LongPlainEncoder.) #(LongPlainDecoder. %) (repeatedly helpers/rand-long)))
  (testing "vlq encoder/decoder"
    (test-encoder #(LongVLQEncoder.) #(LongVLQDecoder. %) (repeatedly helpers/rand-long)))
  (testing "zigzag encoder/decoder"
    (test-encoder #(LongZigZagEncoder.) #(LongZigZagDecoder. %) (repeatedly helpers/rand-long)))
  (testing "packed delta encoder/decoder"
    (test-encoder #(LongPackedDeltaEncoder.) #(LongPackedDeltaDecoder. %) (repeatedly helpers/rand-long)))
  (testing "packed delta encoder's finish method is idempotent"
    (let [rand-longs (repeatedly helpers/rand-long)
          finish-fn (fn [n] (finish-repeatedly n #(LongPackedDeltaEncoder.) rand-longs))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest float-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(FloatPlainEncoder.) #(FloatPlainDecoder. %) (repeatedly helpers/rand-float))))

(deftest double-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(DoublePlainEncoder.) #(DoublePlainDecoder. %) (repeatedly helpers/rand-double))))

(deftest fixed-length-byte-array-encoders
  (testing "length byte array plain encoder/decoder"
    (let [length 10
          rand-byte-arrays (repeatedly #(helpers/rand-byte-array length))
          read-byte-arrays (write-read #(FixedLengthByteArrayPlainEncoder.)
                                       #(FixedLengthByteArrayPlainDecoder. %)
                                       rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays)))))))

(deftest byte-array-encoders
  (testing "plain encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayPlainEncoder.) #(ByteArrayPlainDecoder. %) rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays))))))
  (testing "delta-length encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayDeltaLengthEncoder.)
                                       #(ByteArrayDeltaLengthDecoder. %) rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays))))))
  (testing "delta-length encoder's finish method is idempotent"
    (test-finish-idempotence #(ByteArrayDeltaLengthEncoder.) (repeatedly helpers/rand-byte-array)))
  (testing "incremental encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayIncrementalEncoder.)
                                       #(ByteArrayIncrementalDecoder. %) rand-byte-arrays)]
      (is (every? true? (map = (map seq read-byte-arrays) (map seq rand-byte-arrays))))))
  (testing "incremental encoder's finish method is idempotent"
    (test-finish-idempotence #(ByteArrayIncrementalEncoder.) (repeatedly helpers/rand-byte-array))))
