(ns dendrite.page-test
  (:require [clojure.test :refer :all]
            [dendrite.page :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayWriter])
  (:refer-clojure :exclude [read type]))

(defn- write-read-single-data-page
  [{:keys [max-definition-level max-repetition-level]} value-type encoding compression input-values]
  (let [page-writer (data-page-writer max-repetition-level max-definition-level
                                      value-type encoding compression)
        page-reader-ctor #(data-page-reader % max-repetition-level max-definition-level value-type
                                            encoding compression)]
    (-> page-writer (write! input-values) helpers/get-byte-array-reader page-reader-ctor read)))

(defn- write-read-single-dictionary-page
  [value-type encoding compression input-values]
  (let [page-writer (dictionary-page-writer value-type encoding compression)
        page-reader-ctor #(dictionary-page-reader % value-type encoding compression)]
    (-> page-writer (write! input-values) helpers/get-byte-array-reader page-reader-ctor read)))

(deftest data-page
  (testing "write/read a data page"
    (testing "uncompressed"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "all nils"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeat nil) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :deflate input-values)]
        (is (= output-values input-values))))
    (testing "required"
      (let [spec {:max-definition-level 0 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "non-repeated"
      (let [spec {:max-definition-level 2 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values []
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            page-writer (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
                                              :int :plain :none)
                            (write! input-values))
            baw1 (doto (ByteArrayWriter. 10) (.write page-writer))
            baw2 (doto (ByteArrayWriter. 10) (.write page-writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatable reads"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            page-writer (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
                                              :int :plain :none)
                            (write! input-values))
            page-reader (-> page-writer
                            helpers/get-byte-array-reader
                            (data-page-reader (:max-repetition-level spec) (:max-definition-level spec)
                                              :int :plain :none))]
        (is (= (read page-reader) (read page-reader)))))))

(deftest dictionary-page
  (testing "write/read a dictionary page"
    (testing "uncompressed"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-single-dictionary-page :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-single-dictionary-page :int :plain :lz4 input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [input-values []
            output-values (write-read-single-dictionary-page :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            page-writer (-> (dictionary-page-writer :int :plain :none)
                            (write! input-values))
            baw1 (doto (ByteArrayWriter. 10) (.write page-writer))
            baw2 (doto (ByteArrayWriter. 10) (.write page-writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatable reads"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            page-writer (-> (dictionary-page-writer :int :plain :none)
                            (write! input-values))
            page-reader (-> page-writer
                            helpers/get-byte-array-reader
                            (dictionary-page-reader :int :plain :none))]
        (is (= (read page-reader) (read page-reader)))))))

(deftest incompatible-pages
  (testing "read incompatible page types throws an exception"
    (let [spec {:max-definition-level 1 :max-repetition-level 1}
          data-bar (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
                                         :int :plain :none)
                       (write! (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 100)))
                       helpers/get-byte-array-reader)
          dict-bar (-> (dictionary-page-writer :int :plain :none)
                       (write! (range 100))
                       helpers/get-byte-array-reader)]
      (is (data-page-reader data-bar 1 1 :int :plain :none))
      (is (thrown? IllegalArgumentException (data-page-reader dict-bar (:max-definition-level spec)
                                                              :int :plain :none)))
      (is (dictionary-page-reader dict-bar :int :plain :none))
      (is (thrown? IllegalArgumentException (dictionary-page-reader data-bar :int :plain :none))))))
