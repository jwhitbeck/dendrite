(ns dendrite.page-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.page :refer :all])
  (:import [dendrite.java ByteArrayWriter ByteArrayReader]))

(defn- rand-wrapped-value [schema-depth]
  (let [definition-level (rand-int (inc schema-depth))
        repetition-level (rand-int (inc schema-depth))
        v (if (= definition-level schema-depth) (rand-int 1024) nil)]
    (wrap-value repetition-level definition-level v)))

(defn- rand-wrapped-nil-value [schema-depth]
  (let [definition-level (rand-int schema-depth)
        repetition-level (rand-int (inc schema-depth))]
    (wrap-value repetition-level definition-level nil)))

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
    (write-all values)
    (.writeTo byte-array-writer)))

(defn- write-read-single-data-page
  [max-definition-level required? value-type encoding compression-type input-values]
  (let [baw (ByteArrayWriter.)
        page-writer (data-page-writer max-definition-level required? value-type encoding compression-type)
        page-reader-ctor #(data-page-reader % max-definition-level value-type encoding compression-type)]
    (write-page-to-buffer page-writer baw input-values)
    (-> baw .buffer ByteArrayReader. (.sliceAhead 1) page-reader-ctor read-page)))

(defn- write-read-single-dictionnary-page
  [value-type encoding compression-type input-values]
  (let [baw (ByteArrayWriter.)
        page-writer (dictionnary-page-writer value-type encoding compression-type)
        page-reader-ctor #(dictionnary-page-reader % value-type encoding compression-type)]
    (write-page-to-buffer page-writer baw input-values)
    (-> baw .buffer ByteArrayReader. (.sliceAhead 1) page-reader-ctor read-page)))

(deftest write-read-page
  (testing "Write/read a data page works"
    (testing "uncompressed"
      (let [max-definition-level 3
            input-values (repeatedly 1000 #(rand-wrapped-value max-definition-level))
            output-values (write-read-single-data-page max-definition-level false
                                                       :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "all nils"
      (let [max-definition-level 3
            input-values (repeatedly 1000 #(rand-wrapped-nil-value max-definition-level))
            output-values (write-read-single-data-page max-definition-level false
                                                       :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [max-definition-level 3
            input-values (repeatedly 1000 #(rand-wrapped-value max-definition-level))
            output-values (write-read-single-data-page max-definition-level false
                                                       :int32 :plain :deflate input-values)]
        (is (= output-values input-values))))
    (testing "required"
      (let [max-definition-level 3
            input-values (repeatedly 1000 #(rand-required-wrapped-value max-definition-level))
            output-values (write-read-single-data-page max-definition-level true
                                                       :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "top-level"
      (let [max-definition-level 1
            input-values (repeatedly 1000 #(rand-top-level-wrapped-value))
            output-values (write-read-single-data-page max-definition-level false
                                                       :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "required top-level"
      (let [max-definition-level 1
            input-values (repeatedly 1000 #(rand-required-top-level-wrapped-value))
            output-values (write-read-single-data-page max-definition-level true
                                                       :int32 :plain :none input-values)]
        (is (= output-values input-values)))))
  (testing "Write/read a dictionnary page works"
    (testing "uncompressed"
      (let [input-values (repeatedly 1000 #(rand-int 10000))
            output-values (write-read-single-dictionnary-page :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [input-values (repeatedly 1000 #(rand-int 10000))
            output-values (write-read-single-dictionnary-page :int32 :plain :lz4 input-values)]
        (is (= output-values input-values))))))
