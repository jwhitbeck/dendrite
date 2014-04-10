(ns dendrite.pages-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.pages :refer :all])
  (:import [dendrite.java Int32PlainEncoder Int32PlainDecoder ByteArrayWriter ByteArrayReader
            DeflateCompressor DeflateDecompressor]))

(defn- rand-wrapped-value [schema-depth]
  (let [definition-level (rand-int (inc schema-depth))
        repetition-level (rand-int (inc schema-depth))
        v (if (= definition-level schema-depth) (rand-int 1024) nil)]
    (wrap-value repetition-level definition-level v)))

(defn- rand-required-wrapped-value [schema-depth]
  (let [definition-level schema-depth
        repetition-level (rand-int (inc schema-depth))
        v (rand-int 1024)]
    (wrap-value repetition-level definition-level v)))

(defn- rand-top-level-wrapped-value []
  (let [repetition-level 0
        definition-level (rand-int 2)
        v (if (= definition-level 1) (rand-int 1024) nil)]
    (wrap-value repetition-level definition-level v)))

(defn rand-required-top-level-wrapped-value []
  (wrap-value 0 1 (rand-int 1024)))

(defn- write-page-to-buffer [data-page-writer byte-array-writer values]
  (doto data-page-writer
    (write-wrapped-values values)
    .finish
    (.writeTo byte-array-writer)))

(defn- write-read-single-page
  [page-writer page-reader-ctor input-values]
  (let [baw (ByteArrayWriter.)]
    (write-page-to-buffer page-writer baw input-values)
    (-> baw .buffer ByteArrayReader. page-reader-ctor read-values)))

(deftest write-read-page
  (testing "Write/read a data page works"
    (testing "uncompressed"
      (let [schema-depth 3
            input-values (repeatedly 1000 #(rand-wrapped-value schema-depth))
            page-writer (data-page-writer schema-depth (Int32PlainEncoder.) nil)
            page-reader-ctor (fn [byte-array-reader]
                               (get-page-reader byte-array-reader schema-depth #(Int32PlainDecoder. %) nil))]
        (is (= (write-read-single-page page-writer page-reader-ctor input-values) input-values))))
    (testing "compressed"
      (let [schema-depth 3
            input-values (repeatedly 1000 #(rand-wrapped-value schema-depth))
            page-writer (data-page-writer schema-depth (Int32PlainEncoder.) (DeflateCompressor.))
            page-reader-ctor (fn [byte-array-reader]
                               (get-page-reader byte-array-reader schema-depth #(Int32PlainDecoder. %)
                                                #(DeflateDecompressor.)))]
        (is (= (write-read-single-page page-writer page-reader-ctor input-values) input-values))))
    (testing "required"
      (let [schema-depth 3
            input-values (repeatedly 1000 #(rand-required-wrapped-value schema-depth))
            page-writer (required-data-page-writer schema-depth (Int32PlainEncoder.) nil)
            page-reader-ctor (fn [byte-array-reader]
                               (get-page-reader byte-array-reader schema-depth #(Int32PlainDecoder. %) nil))]
        (is (= (write-read-single-page page-writer page-reader-ctor input-values) input-values))))
    (testing "top-level"
      (let [schema-depth 1
            input-values (repeatedly 1000 #(rand-top-level-wrapped-value))
            page-writer (top-level-data-page-writer schema-depth (Int32PlainEncoder.) nil)
            page-reader-ctor (fn [byte-array-reader]
                               (get-page-reader byte-array-reader schema-depth #(Int32PlainDecoder. %) nil))]
        (is (= (write-read-single-page page-writer page-reader-ctor input-values) input-values))))
    (testing "required top-level"
      (let [schema-depth 1
            input-values (repeatedly 1000 #(rand-required-top-level-wrapped-value))
            page-writer (required-top-level-data-page-writer schema-depth (Int32PlainEncoder.) nil)
            page-reader-ctor (fn [byte-array-reader]
                               (get-page-reader byte-array-reader schema-depth #(Int32PlainDecoder. %) nil))]
        (is (= (write-read-single-page page-writer page-reader-ctor input-values) input-values))))))
