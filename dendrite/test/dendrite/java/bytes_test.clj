;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.bytes-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Bytes MemoryOutputStream]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn write-read [write-fn read-fn input-seq]
  (let [mos (MemoryOutputStream. 10)]
    (doseq [x input-seq]
      (write-fn mos x))
    (let [n (count input-seq)
          bb (.byteBuffer mos)]
      (repeatedly n #(read-fn bb)))))

(deftest read-write-bytes
  (testing "writeByte/readByte"
    (let [rand-bytes (repeatedly 100 helpers/rand-byte)
          read-bytes (write-read #(.write ^MemoryOutputStream %1 (int %2)) #(.get ^ByteBuffer %) rand-bytes)]
      (is (= read-bytes rand-bytes)))))

(deftest read-write-fixed-int
  (testing "writeFixedInt/readFixedInt"
    (let [rand-ints (repeatedly 100 helpers/rand-int)
          read-ints (write-read #(Bytes/writeFixedInt %1 %2) #(Bytes/readFixedInt %) rand-ints)]
      (is (= read-ints rand-ints)))))

(deftest read-write-uint
  (testing "writeUInt/readUInt"
    (let [rand-ints (repeatedly 100 helpers/rand-int)
          read-ints (write-read #(Bytes/writeUInt %1 %2) #(Bytes/readUInt %) rand-ints)]
      (is (= read-ints rand-ints))))
  (testing "read invalid uint throws exception"
    (let [bb (->> [-1 -1 -1 -1 -1] (map unchecked-byte) byte-array ByteBuffer/wrap)]
      (is (thrown-with-msg? IllegalStateException #"Failed to parse UInt" (Bytes/readUInt bb))))))

(deftest read-write-packed-int
  (testing "writePackedInt/readPackedInt"
    (doseq [width (range 33)]
      (let [rand-ints (repeatedly 100 #(helpers/rand-int-bits width))
            read-ints (write-read #(Bytes/writePackedInt %1 %2 width)
                                  #(Bytes/readPackedInt % width)
                                  rand-ints)]
        (is (= read-ints rand-ints))))))

(deftest read-write-sint
  (testing "writeSInt/readSInt"
    (let [rand-ints (repeatedly 100 helpers/rand-int)
          read-ints (write-read #(Bytes/writeSInt %1 %2) #(Bytes/readSInt %) rand-ints)]
      (is (= read-ints rand-ints)))))

(deftest read-write-fixed-long
  (testing "writeFixedLong/readFixedLong"
    (let [rand-longs (repeatedly 100 helpers/rand-long)
          read-longs (write-read #(Bytes/writeFixedLong %1 %2) #(Bytes/readFixedLong %) rand-longs)]
      (is (= read-longs rand-longs)))))

(deftest read-write-ulong
  (testing "writeULong/readULong"
    (let [rand-longs (repeatedly 100 helpers/rand-long)
          read-longs (write-read #(Bytes/writeULong %1 %2) #(Bytes/readULong %) rand-longs)]
      (is (= read-longs rand-longs))))
  (testing "read invalid ulong throws exception"
    (let [bar (->> [-1 -1 -1 -1 -1 -1 -1 -1 -1 -1] (map unchecked-byte) byte-array ByteBuffer/wrap)]
      (is (thrown-with-msg? IllegalStateException #"Failed to parse ULong" (Bytes/readULong bar))))))

(deftest read-write-slong
  (testing "writeSLong/readSLong"
    (let [rand-longs (repeatedly 100 helpers/rand-long)
          read-longs (write-read #(Bytes/writeSLong %1 %2) #(Bytes/readSLong %) rand-longs)]
      (is (= read-longs rand-longs)))))

(deftest read-write-biginteger
  (testing "writeBigInt/readBigInt"
    (let [rand-bigintegers (repeatedly 100 #(helpers/rand-biginteger 72))
          read-bigintegers (write-read #(Bytes/writeBigInt %1 %2) #(Bytes/readBigInt %) rand-bigintegers)]
      (is (= read-bigintegers rand-bigintegers)))))

(deftest read-write-uint-vlq
  (testing "writeUIntVLQ/readUIntVLQ"
    (let [rand-bigintegers (repeatedly 100 #(helpers/rand-biginteger 72))
          read-bigintegers (write-read #(Bytes/writeUIntVLQ %1 %2) #(Bytes/readUIntVLQ %) rand-bigintegers)]
      (is (= read-bigintegers rand-bigintegers)))))

(deftest read-write-sint-vlq
  (testing "writeSIntVLQ/readSIntVLQ"
    (let [rand-bigintegers (repeatedly 100 #(helpers/rand-biginteger-signed 72))
          read-bigintegers (write-read #(Bytes/writeSIntVLQ %1 %2) #(Bytes/readSIntVLQ %) rand-bigintegers)]
      (is (= read-bigintegers rand-bigintegers)))))

(deftest read-write-packed-boolean
  (testing "writePackedBooleans/readPackedBooleans"
    (let [rand-bool-octoplets (->> (repeatedly helpers/rand-bool) (partition 8) (map boolean-array) (take 100))
          read-bool-octoplets (write-read #(Bytes/writePackedBooleans %1 %2)
                                          #(let [octopulet (boolean-array 8)]
                                             (Bytes/readPackedBooleans % octopulet)
                                             octopulet)
                                          rand-bool-octoplets)]
      (is (= (map seq read-bool-octoplets) (map seq rand-bool-octoplets))))))

(deftest read-write-float
  (testing "writeFloat/readFloat"
    (let [rand-floats (repeatedly 100 helpers/rand-float)
          read-floats (write-read #(Bytes/writeFloat %1 %2) #(Bytes/readFloat %) rand-floats)]
      (is (= read-floats rand-floats)))))

(deftest read-write-double
  (testing "writeDouble/readDouble"
    (let [rand-doubles (repeatedly 100 helpers/rand-double)
          read-doubles (write-read #(Bytes/writeDouble %1 %2) #(Bytes/readDouble %) rand-doubles)]
      (is (= read-doubles rand-doubles)))))

(deftest read-write-byte-arrays
  (testing "write/read byte arrays"
    (let [rand-byte-arrays (->> (repeatedly helpers/rand-byte) (partition 10) (map byte-array) (take 100))
          read-byte-arrays (write-read #(.write ^MemoryOutputStream %1 ^bytes %2)
                                       #(let [ba (byte-array 10)]
                                          (.get ^ByteBuffer % ba)
                                          ba)
                                       rand-byte-arrays)]
      (is (= (map seq read-byte-arrays) (map seq rand-byte-arrays))))))

(deftest read-write-packed-ints32
  (testing "packing numbers 0 through 7"
    (let [mos (MemoryOutputStream. 10)
          ints (->> (range 8) (map int) int-array)]
      (Bytes/writePackedInts32 mos ints 3 8)
      (let [bb (.byteBuffer mos)]
        (is (= [(.get bb) (.get bb) (.get bb)]
               (->> ["10001000" "11000110" "11111010"]
                    (map (comp unchecked-byte #(Integer/parseInt % 2)))))))))

  (testing "writePackedInt/readPackedInt"
    (let [rand-ints (repeatedly 100 #(rand-int 8))
          read-ints (write-read #(Bytes/writePackedInt %1 %2 3) #(Bytes/readPackedInt % 3) rand-ints)]
      (is (= read-ints rand-ints))))

  (testing "writePackedInts32/readPackedInts32"
    (doseq [width (range 33)]
      (let [rand-int-arrays (->> (repeatedly #(helpers/rand-int-bits width))
                                 (partition 8)
                                 (map int-array)
                                 (take 100))
            read-int-arrays (write-read #(Bytes/writePackedInts32 %1 %2 width 8)
                                        #(let [ints (int-array 8)]
                                           (Bytes/readPackedInts32 % ints width 8)
                                           ints)
                                        rand-int-arrays)]
        (is (= (map seq read-int-arrays) (map seq rand-int-arrays)))))))

(deftest read-write-packed-ints64
  (testing "packing numbers 0 through 7"
    (let [mos (MemoryOutputStream. 10)
          longs (long-array (range 8))]
      (Bytes/writePackedInts64 mos longs 3 8)
      (let [bb (.byteBuffer mos)]
        (is (= [(.get bb) (.get bb) (.get bb)]
               (->> ["10001000" "11000110" "11111010"]
                    (map (comp unchecked-byte #(Integer/parseInt % 2)))))))))

  (testing "writePackedInts64/readPackedInts64"
    (doseq [width (range 65)]
      (let [rand-long-arrays (->> (repeatedly #(helpers/rand-long-bits width))
                                  (partition 8)
                                  (map long-array)
                                  (take 100))
            read-long-arrays (write-read #(Bytes/writePackedInts64 %1 %2 width 8)
                                         #(let [longs (long-array 8)]
                                            (Bytes/readPackedInts64 % longs width 8)
                                            longs)
                                         rand-long-arrays)]
        (is (= (map seq read-long-arrays) (map seq rand-long-arrays)))))))
