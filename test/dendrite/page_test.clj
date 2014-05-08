(ns dendrite.page-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.page :refer :all]
            [dendrite.test-helpers :refer [get-byte-array-reader]])
  (:import [dendrite.java ByteArrayWriter]))

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

(defn- write-read-single-data-page
  [max-definition-level required? value-type encoding compression-type input-values]
  (let [page-writer (data-page-writer max-definition-level required? value-type encoding compression-type)
        page-reader-ctor #(data-page-reader % max-definition-level value-type encoding compression-type)]
    (-> page-writer (write-all input-values) get-byte-array-reader page-reader-ctor read-page)))

(defn- write-read-single-dictionary-page
  [value-type encoding compression-type input-values]
  (let [page-writer (dictionary-page-writer value-type encoding compression-type)
        page-reader-ctor #(dictionary-page-reader % value-type encoding compression-type)]
    (-> page-writer (write-all input-values) get-byte-array-reader page-reader-ctor read-page)))

(deftest data-page
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
        (is (= output-values input-values))))
    (testing "empty page"
      (let [max-definition-level 1
            input-values []
            output-values (write-read-single-data-page max-definition-level true
                                                       :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [max-definition-level 3
            input-values (repeatedly 1000 #(rand-wrapped-value max-definition-level))
            page-writer (-> (data-page-writer max-definition-level false :int32 :plain :none)
                            (write-all input-values))
            baw1 (doto (ByteArrayWriter. 10) (.write page-writer))
            baw2 (doto (ByteArrayWriter. 10) (.write page-writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatable reads"
      (let [max-definition-level 3
            input-values (repeatedly 1000 #(rand-wrapped-value max-definition-level))
            page-writer (-> (data-page-writer max-definition-level false :int32 :plain :none)
                            (write-all input-values))
            page-reader (-> page-writer
                            get-byte-array-reader
                            (data-page-reader max-definition-level :int32 :plain :none))]
        (is (= (read-page page-reader) (read-page page-reader)))))))

(deftest dictionary-page
  (testing "Write/read a dictionary page works"
    (testing "uncompressed"
      (let [input-values (repeatedly 1000 #(rand-int 10000))
            output-values (write-read-single-dictionary-page :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [input-values (repeatedly 1000 #(rand-int 10000))
            output-values (write-read-single-dictionary-page :int32 :plain :lz4 input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [input-values []
            output-values (write-read-single-dictionary-page :int32 :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [input-values (repeatedly 1000 #(rand-int 10000))
            page-writer (-> (dictionary-page-writer :int32 :plain :none)
                            (write-all input-values))
            baw1 (doto (ByteArrayWriter. 10) (.write page-writer))
            baw2 (doto (ByteArrayWriter. 10) (.write page-writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatable reads"
      (let [input-values (repeatedly 1000 #(rand-int 10000))
            page-writer (-> (dictionary-page-writer :int32 :plain :none)
                            (write-all input-values))
            page-reader (-> page-writer
                            get-byte-array-reader
                            (dictionary-page-reader :int32 :plain :none))]
        (is (= (read-page page-reader) (read-page page-reader)))))))

(deftest incompatible-pages
  (testing "Read incompatible page types throws an exception"
    (let [data-bar (-> (data-page-writer 1 false :int32 :plain :none)
                       (write-all (repeatedly 100 #(rand-wrapped-value 1)))
                       get-byte-array-reader)
          dict-bar (-> (dictionary-page-writer :int32 :plain :none)
                       (write-all (range 100))
                       get-byte-array-reader)]
      (is (data-page-reader data-bar 1 :int32 :plain :none))
      (is (thrown? IllegalArgumentException (data-page-reader dict-bar 1 :int32 :plain :none)))
      (is (dictionary-page-reader dict-bar :int32 :plain :none))
      (is (thrown? IllegalArgumentException (dictionary-page-reader data-bar :int32 :plain :none))))))
