(ns dendrite.java.encoders-test
  (:require [clojure.test :refer :all]
            [dendrite.java.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayReader ByteArrayWriter BooleanPackedEncoder BooleanPackedDecoder
            Int32PlainEncoder Int32PlainDecoder
            Int32FixedBitWidthPackedRunLengthEncoder Int32FixedBitWidthPackedRunLengthDecoder
            Int32PackedRunLengthEncoder Int32PackedRunLengthDecoder
            Int32PackedDeltaEncoder Int32PackedDeltaDecoder
            Int64PlainEncoder Int64PlainDecoder
            Int64PackedDeltaEncoder Int64PackedDeltaDecoder
            FloatPlainEncoder FloatPlainDecoder DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayPlainEncoder ByteArrayPlainDecoder
            ByteArrayDeltaLengthEncoder ByteArrayDeltaLengthDecoder
            ByteArrayIncrementalEncoder ByteArrayIncrementalDecoder]))

(defn write-read [encoder-constructor decoder-constructor input-seq]
  (let [n 1000
        encoder (encoder-constructor)
        baw (ByteArrayWriter.)]
    (doseq [x (take n input-seq)]
      (.encode encoder x))
    (.writeTo encoder baw)
    (let [decoder (->> (.buffer baw) ByteArrayReader. decoder-constructor)]
      (->> (repeatedly #(.decode decoder))
           (take n)))))

(defn finish-repeatedly [n encoder-constructor input-seq]
  (let [encoder (encoder-constructor)
        baw (ByteArrayWriter.)]
    (doseq [x (take 10 input-seq)]
      (.encode encoder x))
    (dotimes [_ n] (.finish encoder))
    (.writeTo encoder baw)
    (seq (.buffer baw))))

(deftest boolean-encoders
  (testing "packed encoder/decoder works"
    (let [rand-bools (repeatedly helpers/rand-bool)
          read-bools (write-read #(BooleanPackedEncoder.) #(BooleanPackedDecoder. %) rand-bools)]
      (is (every? true? (map = read-bools rand-bools)))))
  (testing "packed encoder's finish method is idempotent"
    (let [rand-bools (repeatedly helpers/rand-bool)
          finish-fn (fn [n] (finish-repeatedly n #(BooleanPackedEncoder.) rand-bools))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest int32-encoders
  (testing "plain encoder/decoder works"
    (let [rand-ints (repeatedly helpers/rand-int32)
          read-ints (write-read #(Int32PlainEncoder.) #(Int32PlainDecoder. %) rand-ints)]
      (is (every? true? (map = read-ints rand-ints)))))
  (testing "fixed-bit-width packed run-length encoder/decoder works"
    (testing "sparse input"
      (let [rand-ints (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (rand-int 8) 0)))
            read-ints (write-read #(Int32FixedBitWidthPackedRunLengthEncoder. 3)
                                  #(Int32FixedBitWidthPackedRunLengthDecoder. % 3) rand-ints)]
        (is (every? true? (map = read-ints rand-ints)))))
    (testing "random input"
      (let [rand-ints (repeatedly helpers/rand-int32)
            read-ints (write-read #(Int32FixedBitWidthPackedRunLengthEncoder. 32)
                                  #(Int32FixedBitWidthPackedRunLengthDecoder. % 32) rand-ints)]
        (is (every? true? (map = read-ints rand-ints))))))
  (testing "fixed-bit-width packed run-length encoder's finish method is idempotent"
    (let [rand-ints (repeatedly helpers/rand-int32)
          finish-fn (fn [n] (finish-repeatedly n #(Int32FixedBitWidthPackedRunLengthEncoder. 32) rand-ints))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))
  (testing "packed run-length encoder works"
    (testing "sparse input"
      (let [rand-ints (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (rand-int 8) 0)))
            read-ints (write-read #(Int32PackedRunLengthEncoder.)
                                  #(Int32PackedRunLengthDecoder. %) rand-ints)]
        (is (every? true? (map = read-ints rand-ints)))))
    (testing "random input"
      (let [rand-ints (repeatedly helpers/rand-int32)
            read-ints (write-read #(Int32PackedRunLengthEncoder.)
                                  #(Int32PackedRunLengthDecoder. %) rand-ints)]
        (is (every? true? (map = read-ints rand-ints))))))
  (testing "packed run-length encoder's finish method is idempotent"
    (let [rand-ints (repeatedly helpers/rand-int32)
          finish-fn (fn [n] (finish-repeatedly n #(Int32PackedRunLengthEncoder.) rand-ints))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))
  (testing "packed delta encoder/decoder works"
    (let [rand-ints (repeatedly helpers/rand-int32)
          read-ints (write-read #(Int32PackedDeltaEncoder.)
                                #(Int32PackedDeltaDecoder. %) rand-ints)]
      (is (every? true? (map = read-ints rand-ints)))))
  (testing "packed delta encoder's finish method is idempotent"
    (let [rand-ints (repeatedly helpers/rand-int32)
          finish-fn (fn [n] (finish-repeatedly n #(Int32PackedDeltaEncoder.) rand-ints))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest int64-encoders
  (testing "plain encoder/decoder works"
    (let [rand-longs (repeatedly helpers/rand-int64)
          read-longs (write-read #(Int64PlainEncoder.) #(Int64PlainDecoder. %) rand-longs)]
      (is (every? true? (map = read-longs rand-longs)))))
  (testing "packed delta encoder/decoder works"
    (let [rand-longs (repeatedly helpers/rand-int64)
          read-longs (write-read #(Int64PackedDeltaEncoder.) #(Int64PackedDeltaDecoder. %) rand-longs)]
      (is (every? true? (map = read-longs rand-longs)))))
  (testing "packed delta encoder's finish method is idempotent"
    (let [rand-longs (repeatedly helpers/rand-int64)
          finish-fn (fn [n] (finish-repeatedly n #(Int64PackedDeltaEncoder.) rand-longs))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest float-encoders
  (testing "plain encoder/decoder works"
    (let [rand-floats (repeatedly helpers/rand-float)
          read-floats (write-read #(FloatPlainEncoder.) #(FloatPlainDecoder. %) rand-floats)]
      (is (every? true? (map = read-floats rand-floats))))))

(deftest double-encoders
  (testing "plain encoder/decoder works"
    (let [rand-doubles (repeatedly helpers/rand-double)
          read-doubles (write-read #(DoublePlainEncoder.) #(DoublePlainDecoder. %) rand-doubles)]
      (is (every? true? (map = read-doubles rand-doubles))))))

(deftest fixed-length-byte-array-encoders
  (testing "length byte array plain encoder/decoder works"
    (let [length 10
          rand-byte-arrays (repeatedly #(helpers/rand-byte-array length))
          read-byte-arrays (write-read #(FixedLengthByteArrayPlainEncoder.)
                                       #(FixedLengthByteArrayPlainDecoder. %)
                                       rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays))))))

(deftest byte-array-encoders
  (testing "plain encoder/decoder works"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayPlainEncoder.) #(ByteArrayPlainDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays)))))
  (testing "delta-length encoder/decoder works"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayDeltaLengthEncoder.)
                                       #(ByteArrayDeltaLengthDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays)))))
  (testing "delta-length encoder's finish method is idempotent"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          finish-fn (fn [n] (finish-repeatedly n #(ByteArrayDeltaLengthEncoder.) rand-byte-arrays))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))
  (testing "incremental encoder/decoder works"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayIncrementalEncoder.)
                                       #(ByteArrayIncrementalDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays)))))
  (testing "incremental encoder's finish method is idempotent"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          finish-fn (fn [n] (finish-repeatedly n #(ByteArrayIncrementalEncoder.) rand-byte-arrays))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))
