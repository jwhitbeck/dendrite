(ns dendrite.java.encoders-test
  (:require [clojure.test :refer :all]
            [dendrite.java.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayReader ByteArrayWriter BooleanPackedEncoder BooleanPackedDecoder
            Int32PlainEncoder Int32PlainDecoder Int64PlainEncoder Int64PlainDecoder
            FloatPlainEncoder FloatPlainDecoder DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayPlainEncoder ByteArrayPlainDecoder]))

(defn write-read [encoder-constructor decoder-constructor input-seq]
  (let [n 1000
        encoder (encoder-constructor)
        baw (ByteArrayWriter.)]
    (doseq [x (->> input-seq (take n))]
      (.append encoder x))
    (.flush encoder baw)
    (let [decoder (->> (.buffer baw) ByteArrayReader. decoder-constructor)]
      (->> (repeatedly #(.next decoder))
           (take n)))))

(deftest boolean-encoders
  (testing "Boolean packed encoder/decoder works"
    (let [rand-bools (repeatedly helpers/rand-bool)
          read-bools (write-read #(BooleanPackedEncoder.) #(BooleanPackedDecoder. %) rand-bools)]
      (is (every? true? (map = read-bools rand-bools))))))

(deftest int32-encoders
  (testing "Int32 plain encoder/decoder works"
    (let [rand-ints (repeatedly helpers/rand-int)
          read-ints (write-read #(Int32PlainEncoder.) #(Int32PlainDecoder. %) rand-ints)]
      (is (every? true? (map = read-ints rand-ints))))))

(deftest int64-encoders
  (testing "Int64 plain encoder/decoder works"
    (let [rand-longs (repeatedly helpers/rand-int)
          read-longs (write-read #(Int64PlainEncoder.) #(Int64PlainDecoder. %) rand-longs)]
      (is (every? true? (map = read-longs rand-longs))))))

(deftest float-encoders
  (testing "Float plain encoder/decoder works"
    (let [rand-floats (repeatedly helpers/rand-float)
          read-floats (write-read #(FloatPlainEncoder.) #(FloatPlainDecoder. %) rand-floats)]
      (is (every? true? (map = read-floats rand-floats))))))

(deftest double-encoders
  (testing "Double plain encoder/decoder works"
    (let [rand-doubles (repeatedly helpers/rand-double)
          read-doubles (write-read #(DoublePlainEncoder.) #(DoublePlainDecoder. %) rand-doubles)]
      (is (every? true? (map = read-doubles rand-doubles))))))

(deftest fixed-length-byte-array-encoders
  (testing "Fixed length byte array plain encoder/decoder works"
    (let [length 10
          rand-byte-arrays (->> (repeatedly helpers/rand-byte) (partition length) (map byte-array))
          read-byte-arrays (write-read #(FixedLengthByteArrayPlainEncoder. length)
                                       #(FixedLengthByteArrayPlainDecoder. % length)
                                       rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays))))))

(deftest byte-array-encoders
  (testing "Byte array plain encoder/decoder works"
    (let [rand-byte-arrays (->> (repeatedly #(take (rand-int 24) (repeatedly helpers/rand-byte)))
                                (map byte-array))
          read-byte-arrays (write-read #(ByteArrayPlainEncoder.) #(ByteArrayPlainDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays))))))
