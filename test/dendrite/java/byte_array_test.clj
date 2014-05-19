(ns dendrite.java.byte-array-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayReader ByteArrayWriter]))

(deftest writer-growth
  (testing "ByteArrayWriter grows properly"
    (let [baw (ByteArrayWriter. 10)]
      (is (= (alength (.buffer baw)) 10))
      (dotimes [_ 10] (.writeByte baw (byte 1)))
      (is (= (alength (.buffer baw)) 10))
      (.writeByte baw (byte 1))
      (is (= (alength (.buffer baw)) 20)))))

(defn write-read [write-fn read-fn input-seq]
  (let [n 1000
        baw (ByteArrayWriter.)]
    (doseq [x (take n input-seq)]
      (write-fn baw x))
    (let [bar (ByteArrayReader. (.buffer baw))]
      (repeatedly n #(read-fn bar)))))

(deftest read-write-bytes
  (testing "writeByte/readByte"
    (let [rand-bytes (repeatedly helpers/rand-byte)
          read-bytes (write-read #(.writeByte %1 %2) #(.readByte %) rand-bytes)]
      (is (every? true? (map = read-bytes rand-bytes))))))

(deftest read-write-fixed-int
  (testing "writeFixedInt/readFixedInt"
    (let [rand-ints (repeatedly helpers/rand-int)
          read-ints (write-read #(.writeFixedInt %1 %2) #(.readFixedInt %) rand-ints)]
      (is (every? true? (map = read-ints rand-ints))))))

(deftest read-write-uint
  (testing "writeUInt/readUInt"
    (let [rand-ints (repeatedly helpers/rand-int)
          read-ints (write-read #(.writeUInt %1 %2) #(.readUInt %) rand-ints)]
      (is (every? true? (map = read-ints rand-ints)))))
  (testing "read invalid uint throws exception"
    (let [bar (->> [-1 -1 -1 -1 -1] (map unchecked-byte) byte-array ByteArrayReader.)]
      (is (thrown? IllegalStateException (.readUInt bar))))))

(deftest read-write-packed-int
  (testing "writePackedInt/readPackedInt"
    (let [width 12
          rand-ints (repeatedly #(helpers/rand-int-bits width))
          read-ints (write-read #(.writePackedInt %1 %2 width) #(.readPackedInt % width) rand-ints)]
      (is (every? true? (map = read-ints rand-ints))))))

(deftest read-write-sint
  (testing "writeSInt/readSInt"
    (let [rand-ints (repeatedly helpers/rand-int)
          read-ints (write-read #(.writeSInt %1 %2) #(.readSInt %) rand-ints)]
      (is (every? true? (map = read-ints rand-ints))))))

(deftest read-write-fixed-long
  (testing "writeFixedLong/readFixedLong"
    (let [rand-longs (repeatedly helpers/rand-long)
          read-longs (write-read #(.writeFixedLong %1 %2) #(.readFixedLong %) rand-longs)]
      (is (every? true? (map = read-longs rand-longs))))))

(deftest read-write-ulong
  (testing "writeULong/readULong"
    (let [rand-longs (repeatedly helpers/rand-long)
          read-longs (write-read #(.writeULong %1 %2) #(.readULong %) rand-longs)]
      (is (every? true? (map = read-longs rand-longs)))))
  (testing "read invalid ulong throws exception"
    (let [bar (->> [-1 -1 -1 -1 -1 -1 -1 -1 -1 -1] (map unchecked-byte) byte-array ByteArrayReader.)]
      (is (thrown? IllegalStateException (.readULong bar))))))

(deftest read-write-slong
  (testing "writeSLong/readSLong"
    (let [rand-longs (repeatedly helpers/rand-long)
          read-longs (write-read #(.writeSLong %1 %2) #(.readSLong %) rand-longs)]
      (is (every? true? (map = read-longs rand-longs))))))

(deftest read-write-big-int
  (testing "writeBigInt/readBigInt"
    (let [rand-big-ints (repeatedly #(helpers/rand-big-int 72))
          read-big-ints (write-read #(.writeBigInt %1 %2) #(.readBigInt %) rand-big-ints)]
      (is (every? true? (map = read-big-ints rand-big-ints))))))

(deftest read-write-uint-vlq
  (testing "writeUIntVLQ/readUIntVLQ"
    (let [rand-big-ints (repeatedly #(helpers/rand-big-int 72))
          read-big-ints (write-read #(.writeUIntVLQ %1 %2) #(.readUIntVLQ %) rand-big-ints)]
      (is (every? true? (map = read-big-ints rand-big-ints))))))

(deftest read-write-sint-vlq
  (testing "writeSIntVLQ/readSIntVLQ"
    (let [rand-big-ints (repeatedly #(helpers/rand-big-int-signed 72))
          read-big-ints (write-read #(.writeSIntVLQ %1 %2) #(.readSIntVLQ %) rand-big-ints)]
      (is (every? true? (map = read-big-ints rand-big-ints))))))

(deftest read-write-packed-boolean
  (testing "writePackedBooleans/readPackedBooleans"
    (let [rand-bool-octoplets (->> (repeatedly helpers/rand-bool) (partition 8) (map boolean-array))
          read-bool-octoplets (write-read #(.writePackedBooleans %1 %2)
                                          #(let [octopulet (boolean-array 8)]
                                             (.readPackedBooleans % octopulet)
                                             octopulet)
                                          rand-bool-octoplets)]
      (is (every? true? (map helpers/array= read-bool-octoplets rand-bool-octoplets))))))

(deftest read-write-float
  (testing "writeFloat/readFloat"
    (let [rand-floats (repeatedly helpers/rand-float)
          read-floats (write-read #(.writeFloat %1 %2) #(.readFloat %) rand-floats)]
      (is (every? true? (map = read-floats rand-floats))))))

(deftest read-write-double
  (testing "writeDouble/readDouble"
    (let [rand-doubles (repeatedly helpers/rand-double)
          read-doubles (write-read #(.writeDouble %1 %2) #(.readDouble %) rand-doubles)]
      (is (every? true? (map = read-doubles rand-doubles))))))

(deftest read-write-byte-arrays
  (testing "writeByteArray/readByteArray"
    (let [rand-byte-arrays (->> (repeatedly helpers/rand-byte) (partition 10) (map byte-array))
          read-byte-arrays (write-read #(.writeByteArray %1 %2)
                                       #(let [ba (byte-array 10)]
                                          (.readByteArray % ba)
                                          ba)
                                       rand-byte-arrays)]
      (is (every? true? (map helpers/array= read-byte-arrays rand-byte-arrays))))))

(deftest read-write-packed-ints32
  (testing "packing numbers 0 through 7"
    (let [baw (ByteArrayWriter. 10)
          ints (->> (range 8) (map int) int-array)]
      (.writePackedInts32 baw ints 3 8)
      (is (->> (seq (.buffer baw))
               (map = (->> ["10001000" "11000110" "11111010"]
                           (map (comp unchecked-byte #(Integer/parseInt % 2)))))
               (every? true?)))))

  (testing "writePackedInt/readPackedInt"
    (let [rand-ints (repeatedly #(rand-int 8))
          read-ints (write-read #(.writePackedInt %1 %2 3) #(.readPackedInt % 3) rand-ints)]
      (is (every? true? (map = read-ints rand-ints)))))

  (testing "writePackedInts32/readPackedInts32"
    (testing "width under 24 bits"
      (let [rand-int-arrays (->> (repeatedly #(helpers/rand-int-bits 3)) (partition 8) (map int-array))
            read-int-arrays (write-read #(.writePackedInts32 %1 %2 3 8)
                                        #(let [ints (int-array 8)]
                                           (.readPackedInts32 % ints 3 8)
                                           ints)
                                        rand-int-arrays)]
        (is (every? true? (map helpers/array= read-int-arrays rand-int-arrays)))))
    (testing "width over 24 bits"
      (let [rand-int-arrays (->> (repeatedly #(helpers/rand-int-bits 28)) (partition 8) (map int-array))
            read-int-arrays (write-read #(.writePackedInts32 %1 %2 28 8)
                                        #(let [ints (int-array 8)]
                                           (.readPackedInts32 % ints 28 8)
                                           ints)
                                        rand-int-arrays)]
        (is (every? true? (map helpers/array= read-int-arrays rand-int-arrays)))))
    (testing "width equal to 32 bits"
      (let [rand-int-arrays (->> (repeatedly #(helpers/rand-int-bits 32)) (partition 8) (map int-array))
            read-int-arrays (write-read #(.writePackedInts32 %1 %2 32 8)
                                        #(let [ints (int-array 8)]
                                           (.readPackedInts32 % ints 32 8)
                                           ints)
                                        rand-int-arrays)]
        (is (every? true? (map helpers/array= read-int-arrays rand-int-arrays)))))))

(deftest read-write-packed-ints64
  (testing "packing numbers 0 through 7"
    (let [baw (ByteArrayWriter. 10)
          longs (->> (range 8) long-array)]
      (.writePackedInts64 baw longs 3 8)
      (is (->> (seq (.buffer baw))
               (map = (->> ["10001000" "11000110" "11111010"]
                           (map (comp unchecked-byte #(Integer/parseInt % 2)))))
               (every? true?)))))

  (testing "writePackedInts64/readPackedInts64"
    (testing "width under 56 bits"
      (let [rand-long-arrays (->> (repeatedly #(helpers/rand-long-bits 3)) (partition 8) (map long-array))
            read-long-arrays (write-read #(.writePackedInts64 %1 %2 3 8)
                                         #(let [longs (long-array 8)]
                                            (.readPackedInts64 % longs 3 8)
                                            longs)
                                         rand-long-arrays)]
        (is (every? true? (map helpers/array= read-long-arrays rand-long-arrays)))))
    (testing "width over 56 bits"
      (let [rand-long-arrays (->> (repeatedly #(helpers/rand-long-bits 60)) (partition 8) (map long-array))
            read-long-arrays (write-read #(.writePackedInts64 %1 %2 60 8)
                                         #(let [longs (long-array 8)]
                                            (.readPackedInts64 % longs 60 8)
                                            longs)
                                         rand-long-arrays)]
        (is (every? true? (map helpers/array= read-long-arrays rand-long-arrays)))))
    (testing "width equals 64 bits"
      (let [rand-long-arrays (->> (repeatedly helpers/rand-long) (partition 8) (map long-array))
            read-long-arrays (write-read #(.writePackedInts64 %1 %2 64 8)
                                         #(let [longs (long-array 8)]
                                            (.readPackedInts64 % longs 64 8)
                                            longs)
                                         rand-long-arrays)]
        (is (every? true? (map helpers/array= read-long-arrays rand-long-arrays)))))))
