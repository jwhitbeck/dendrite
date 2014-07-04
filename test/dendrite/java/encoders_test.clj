(ns dendrite.java.encoders-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayReader ByteArrayWriter BooleanPackedEncoder BooleanPackedDecoder
            IntPlainEncoder IntPlainDecoder
            IntFixedBitWidthPackedRunLengthEncoder IntFixedBitWidthPackedRunLengthDecoder
            IntPackedRunLengthEncoder IntPackedRunLengthDecoder
            IntPackedDeltaEncoder IntPackedDeltaDecoder
            LongPlainEncoder LongPlainDecoder
            LongPackedDeltaEncoder LongPackedDeltaDecoder
            FloatPlainEncoder FloatPlainDecoder DoublePlainEncoder DoublePlainDecoder
            FixedLengthByteArrayPlainEncoder FixedLengthByteArrayPlainDecoder
            ByteArrayPlainEncoder ByteArrayPlainDecoder
            ByteArrayDeltaLengthEncoder ByteArrayDeltaLengthDecoder
            ByteArrayIncrementalEncoder ByteArrayIncrementalDecoder]))

(defn write-read
  ([encoder-constructor decoder-constructor input-seq]
     (write-read 1000 encoder-constructor decoder-constructor input-seq))
  ([n encoder-constructor decoder-constructor input-seq]
     (let [encoder (encoder-constructor)]
       (doseq [x (take n input-seq)]
         (.encode encoder x))
       (let [decoder (-> encoder helpers/get-byte-array-reader decoder-constructor)]
         (->> (repeatedly #(.decode decoder))
              (take n))))))

(defn test-encode-n-values [n encoder-constructor decoder-constructor input-seq]
  (let [output-seq (write-read n encoder-constructor decoder-constructor input-seq)]
    (is (every? true? (map = output-seq input-seq)))))

(defn test-encoder [encoder-constructor decoder-constructor input-seq]
  (doseq [n [0 1 2 3 4 5 6 7 8 1000]]
    (test-encode-n-values n encoder-constructor decoder-constructor input-seq)))

(defn finish-repeatedly [n encoder-constructor input-seq]
  (let [encoder (encoder-constructor)
        baw (ByteArrayWriter.)]
    (doseq [x (take 10 input-seq)]
      (.encode encoder x))
    (dotimes [_ n] (.finish encoder))
    (.writeTo encoder baw)
    (seq (.buffer baw))))

(deftest boolean-encoders
  (testing "packed encoder/decoder"
    (test-encoder #(BooleanPackedEncoder.) #(BooleanPackedDecoder. %) (repeatedly helpers/rand-bool)))
  (testing "packed encoder's finish method is idempotent"
    (let [rand-bools (repeatedly helpers/rand-bool)
          finish-fn (fn [n] (finish-repeatedly n #(BooleanPackedEncoder.) rand-bools))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest int-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(IntPlainEncoder.) #(IntPlainDecoder. %) (repeatedly helpers/rand-int)))
  (testing "fixed-bit-width packed run-length encoder/decoder"
    (testing "sparse input"
      (let [rand-ints (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (rand-int 8) 0)))]
        (test-encoder #(IntFixedBitWidthPackedRunLengthEncoder. 3)
                      #(IntFixedBitWidthPackedRunLengthDecoder. % 3)
                      rand-ints)))
    (testing "random input"
      (test-encoder #(IntFixedBitWidthPackedRunLengthEncoder. 32)
                    #(IntFixedBitWidthPackedRunLengthDecoder. % 32)
                    (repeatedly helpers/rand-int))))
  (testing "fixed-bit-width packed run-length encoder's finish method is idempotent"
    (let [rand-ints (repeatedly helpers/rand-int)
          finish-fn (fn [n] (finish-repeatedly n #(IntFixedBitWidthPackedRunLengthEncoder. 32) rand-ints))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))
  (testing "packed run-length encoder"
    (testing "sparse input"
      (test-encoder #(IntPackedRunLengthEncoder.)
                    #(IntPackedRunLengthDecoder. %)
                    (->> (repeatedly #(rand-int 8)) (map #(if (= 7 %) (rand-int 8) 0)))))
    (testing "random input"
      (test-encoder #(IntPackedRunLengthEncoder.)
                    #(IntPackedRunLengthDecoder. %)
                    (repeatedly helpers/rand-int))))
  (testing "packed run-length encoder's finish method is idempotent"
    (let [rand-ints (repeatedly helpers/rand-int)
          finish-fn (fn [n] (finish-repeatedly n #(IntPackedRunLengthEncoder.) rand-ints))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))
  (testing "packed delta encoder/decoder"
    (test-encoder #(IntPackedDeltaEncoder.)
                  #(IntPackedDeltaDecoder. %)
                  (repeatedly helpers/rand-int)))
  (testing "packed delta encoder's finish method is idempotent"
    (let [rand-ints (repeatedly helpers/rand-int)
          finish-fn (fn [n] (finish-repeatedly n #(IntPackedDeltaEncoder.) rand-ints))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))

(deftest long-encoders
  (testing "plain encoder/decoder"
    (test-encoder #(LongPlainEncoder.) #(LongPlainDecoder. %) (repeatedly helpers/rand-long)))
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
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays))))))

(deftest byte-array-encoders
  (testing "plain encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayPlainEncoder.) #(ByteArrayPlainDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays)))))
  (testing "delta-length encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayDeltaLengthEncoder.)
                                       #(ByteArrayDeltaLengthDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays)))))
  (testing "delta-length encoder's finish method is idempotent"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          finish-fn (fn [n] (finish-repeatedly n #(ByteArrayDeltaLengthEncoder.) rand-byte-arrays))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3))))))
  (testing "incremental encoder/decoder"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          read-byte-arrays (write-read #(ByteArrayIncrementalEncoder.)
                                       #(ByteArrayIncrementalDecoder. %) rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays)))))
  (testing "incremental encoder's finish method is idempotent"
    (let [rand-byte-arrays (repeatedly helpers/rand-byte-array)
          finish-fn (fn [n] (finish-repeatedly n #(ByteArrayIncrementalEncoder.) rand-byte-arrays))]
      (is (every? true? (map = (finish-fn 0) (finish-fn 3)))))))
