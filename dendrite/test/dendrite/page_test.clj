(ns dendrite.page-test
  (:require [clojure.test :refer :all]
            [dendrite.page :refer :all]
            [dendrite.leveled-value :as lv]
            [dendrite.test-helpers :as helpers]
            [dendrite.utils :as utils])
  (:import [dendrite.java ByteArrayWriter])
  (:refer-clojure :exclude [read type]))

(set! *warn-on-reflection* true)

(defn- write-read-single-data-page
  [{:keys [max-definition-level max-repetition-level]} value-type encoding compression
   input-values & {:keys [map-fn]}]
  (let [page-writer (data-page-writer max-repetition-level max-definition-level
                                      value-type encoding compression)
        page-reader-ctor #(data-page-reader % max-repetition-level max-definition-level value-type
                                            encoding compression)]
    (-> page-writer
        (write! input-values)
        helpers/get-byte-array-reader
        page-reader-ctor
        (read map-fn)
        utils/flatten-1)))

(defn- write-read-single-dictionary-page
  [value-type encoding compression input-values & {:keys [map-fn]}]
  (let [page-writer (dictionary-page-writer value-type encoding compression)
        page-reader-ctor #(dictionary-page-reader % value-type encoding compression)]
    (-> page-writer
        (write! input-values)
        helpers/get-byte-array-reader
        page-reader-ctor
        (read-array map-fn)
        seq)))

(deftest data-page
  (testing "write/read a data page"
    (testing "uncompressed"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "with a mapping function"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            map-fn (partial * 2)
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values
                                                       :map-fn map-fn)]
        (is (= output-values (map #(lv/apply-fn % map-fn) input-values)))))
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
            ^ByteArrayWriter page-writer (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
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
        (is (= (read page-reader) (read page-reader)))))
    (testing "read seq is chunked"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (chunked-seq? (seq output-values)))))))

(deftest dictionary-page
  (testing "write/read a dictionary page"
    (testing "uncompressed"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-single-dictionary-page :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "with a mapping function"
      (let [map-fn (partial * 3)
            input-values (repeatedly 10 helpers/rand-int)
            output-values (write-read-single-dictionary-page :int :plain :none input-values :map-fn map-fn)]
        (is (= output-values (map map-fn input-values)))))
    (testing "compressed"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-single-dictionary-page :int :plain :lz4 input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [input-values []
            output-values (write-read-single-dictionary-page :int :plain :none input-values)]
        (is (nil? output-values))))
    (testing "repeatable writes"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            ^ByteArrayWriter page-writer (-> (dictionary-page-writer :int :plain :none)
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
        (is (every? true? (map = (read-array page-reader) (read-array page-reader))))))))

(deftest incompatible-pages
  (testing "read incompatible page types throws an exception"
    (let [spec {:max-definition-level 1 :max-repetition-level 1}
          data-bar (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
                                         :int :plain :none)
                       (write! (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 100)))
                       helpers/get-byte-array-reader)
          dict-bar (-> (dictionary-page-writer :int :plain :none)
                       (write! (map int (range 100)))
                       helpers/get-byte-array-reader)]
      (is (data-page-reader data-bar 1 1 :int :plain :none))
      (is (thrown-with-msg?
           IllegalArgumentException #":dictionary is not a supported data page type"
           (data-page-reader dict-bar (:max-repetition-level spec) (:max-definition-level spec)
                             :int :plain :none)))
      (is (dictionary-page-reader dict-bar :int :plain :none))
      (is (thrown-with-msg?
           IllegalArgumentException #":data is not a supported dictionary page type"
           (dictionary-page-reader data-bar :int :plain :none))))))
