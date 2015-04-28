;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.page-test
  (:require [clojure.test :refer :all]
            [dendrite.page :refer :all]
            [dendrite.leveled-value :as lv]
            [dendrite.test-helpers :as helpers]
            [dendrite.utils :as utils])
  (:import [dendrite.java MemoryOutputStream])
  (:refer-clojure :exclude [read type]))

(set! *warn-on-reflection* true)

(defn- write-read-single-data-page
  [{:keys [max-definition-level max-repetition-level]} value-type encoding compression
   input-values & {:keys [map-fn]}]
  (let [page-writer (data-page-writer max-repetition-level max-definition-level
                                      helpers/default-type-store value-type encoding compression)
        bb (helpers/output-buffer->byte-buffer (.write page-writer [input-values]))
        page-reader (data-page-reader bb max-repetition-level max-definition-level
                                      helpers/default-type-store value-type encoding compression)]
    (utils/flatten-1 (.read page-reader map-fn))))

(defn- write-read-single-non-repeated-data-page
  [{:keys [max-definition-level max-repetition-level]} value-type encoding compression
   input-values & {:keys [map-fn]}]
  (let [page-writer (data-page-writer max-repetition-level max-definition-level
                                      helpers/default-type-store value-type encoding compression)
        bb (helpers/output-buffer->byte-buffer (.write page-writer input-values))
        page-reader (data-page-reader bb max-repetition-level max-definition-level
                                      helpers/default-type-store value-type encoding compression)]
    (.read page-reader map-fn)))

(defn- write-read-single-dictionary-page
  [value-type encoding compression input-values & {:keys [map-fn]}]
  (let [page-writer (dictionary-page-writer helpers/default-type-store value-type encoding compression)]
    (doseq [entry input-values]
      (.writeEntry page-writer entry))
    (let [bb (helpers/output-buffer->byte-buffer page-writer)
          page-reader (dictionary-page-reader bb helpers/default-type-store
                                              value-type encoding compression)]
      (seq (.readArray page-reader map-fn)))))

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
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 10))
            output-values (write-read-single-data-page spec :int :plain :deflate input-values)]
        (is (= output-values input-values))))
    (testing "required"
      (let [spec {:max-definition-level 0 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int) (take 1000))
            output-values (write-read-single-non-repeated-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "non-repeated"
      (let [spec {:max-definition-level 2 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int)
                              (helpers/rand-map 0.2 (constantly nil))
                              (take 1000))
            output-values (write-read-single-non-repeated-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values []
            output-values (write-read-single-data-page spec :int :plain :none input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-buffer (doto (data-page-writer (:max-repetition-level spec)
                                                  (:max-definition-level spec)
                                                  helpers/default-type-store
                                                  :int :plain :none)
                            (.write [input-values]))
            mos1 (MemoryOutputStream. 10)
            mos2 (MemoryOutputStream. 10)]
        (.writeTo output-buffer mos1)
        (.writeTo output-buffer mos2)
        (is (= (-> mos1 helpers/output-buffer->byte-buffer .array seq)
               (-> mos2 helpers/output-buffer->byte-buffer .array seq)))))
    (testing "repeatable reads"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            page-writer (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
                                              helpers/default-type-store :int :plain :none)
                            (.write [input-values]))
            page-reader (-> page-writer
                            helpers/output-buffer->byte-buffer
                            (data-page-reader (:max-repetition-level spec) (:max-definition-level spec)
                                              helpers/default-type-store
                                              :int :plain :none))]
        (is (= (.read page-reader) (.read page-reader)))))
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
            output-values (write-read-single-dictionary-page :int :plain :deflate input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [input-values []
            output-values (write-read-single-dictionary-page :int :plain :none input-values)]
        (is (nil? output-values))))
    (testing "repeatable writes"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            page-writer (dictionary-page-writer helpers/default-type-store :int :plain :none)]
        (doseq [entry input-values]
          (.writeEntry page-writer entry))
        (let [mos1 (MemoryOutputStream. 10)
              mos2 (MemoryOutputStream. 10)]
          (.writeTo page-writer mos1)
          (.writeTo page-writer mos2)
          (is (= (-> mos1 helpers/output-buffer->byte-buffer .array seq)
                 (-> mos2 helpers/output-buffer->byte-buffer .array seq))))))
    (testing "repeatable reads"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            page-writer (dictionary-page-writer helpers/default-type-store :int :plain :none)]
        (doseq [entry input-values]
          (.writeEntry page-writer entry))
        (let [page-reader (-> page-writer
                              helpers/output-buffer->byte-buffer
                              (dictionary-page-reader helpers/default-type-store :int :plain :none))]
          (is (every? true? (map = (.readArray page-reader) (.readArray page-reader)))))))))

(deftest incompatible-pages
  (testing "read incompatible page types throws an exception"
    (let [spec {:max-definition-level 1 :max-repetition-level 1}
          data-buffer (-> (data-page-writer (:max-repetition-level spec) (:max-definition-level spec)
                                                     helpers/default-type-store :int :plain :none)
                          (.write (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 100) vector))
                          helpers/output-buffer->byte-buffer)
          dict-writer (dictionary-page-writer helpers/default-type-store :int :plain :none)]
      (doseq [entry (map int (range 100))]
        (.writeEntry dict-writer entry))
      (let [dict-buffer (helpers/output-buffer->byte-buffer dict-writer)]
        (is (data-page-reader data-buffer 1 1 helpers/default-type-store :int :plain :none))
        (is (thrown-with-msg?
             IllegalArgumentException #":dictionary is not a supported data page type"
             (data-page-reader dict-buffer (:max-repetition-level spec) (:max-definition-level spec)
                               helpers/default-type-store :int :plain :none)))
        (is (dictionary-page-reader dict-buffer helpers/default-type-store :int :plain :none))
        (is (thrown-with-msg?
             IllegalArgumentException #":data is not a supported dictionary page type"
             (dictionary-page-reader data-buffer helpers/default-type-store :int :plain :none)))))))
