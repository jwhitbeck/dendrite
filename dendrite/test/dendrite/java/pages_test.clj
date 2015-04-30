;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.pages-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers]
            [dendrite.utils :as utils])
  (:import [dendrite.java DataPage$Reader DataPage$Writer DictionaryPage$Reader DictionaryPage$Writer
            LeveledValue MemoryOutputStream Pages Types])
  (:refer-clojure :exclude [read type]))

(set! *warn-on-reflection* true)

(def ^Types types (Types/create nil nil))

(defn- write-read-data-page
  [{:keys [max-repetition-level max-definition-level type encoding compression f]
    :or {type Types/INT encoding Types/PLAIN compression Types/NONE}}
   input-values]
  (let [writer (doto (DataPage$Writer/create max-repetition-level max-definition-level
                                             (.getEncoder types type encoding)
                                             (.getDecoderFactory types type encoding)
                                             (.getCompressor types compression))
                 (.write input-values))
        bb (helpers/output-buffer->byte-buffer writer)
        reader (DataPage$Reader/create bb max-repetition-level max-definition-level
                                       (.getDecoderFactory types type encoding)
                                       (.getDecompressorFactory types compression))]
    (cond->> (if f
               (.readWith reader f)
               (.read reader))
      (pos? max-repetition-level) utils/flatten-1)))

(deftest data-page
  (testing "write/read a data page"
    (testing "default"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (= output-values input-values))))
    (testing "with a function"
      (let [f (fnil (partial * 2) 1)
            spec {:max-definition-level 3 :max-repetition-level 2 :f f}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (= output-values (map (fn [^LeveledValue lv] (.apply lv f)) input-values)))))
    (testing "all nils"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeat nil) (helpers/leveled spec) (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [spec {:max-definition-level 3 :max-repetition-level 2 :compression Types/DEFLATE}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (= output-values input-values))))
    (testing "required"
      (let [spec {:max-definition-level 0 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int) (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (= output-values input-values))))
    (testing "non-repeated"
      (let [spec {:max-definition-level 2 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int)
                              (helpers/rand-map 0.2 (constantly nil))
                              (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values []
            output-values (write-read-data-page spec input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            writer (doto (DataPage$Writer/create (:max-repetition-level spec)
                                                 (:max-definition-level spec)
                                                 (.getEncoder types Types/INT Types/PLAIN)
                                                 (.getDecoderFactory types Types/INT Types/PLAIN)
                                                 (.getCompressor types Types/DEFLATE))
                     (.write input-values))
            mos1 (doto (MemoryOutputStream. 10)
                   (.write writer))
            mos2 (doto (MemoryOutputStream. 10)
                   (.write writer))]
        (is (= (-> mos1 helpers/output-buffer->byte-buffer .array seq)
               (-> mos2 helpers/output-buffer->byte-buffer .array seq)))))
    (testing "repeatable reads"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            writer (doto (DataPage$Writer/create (:max-repetition-level spec)
                                                 (:max-definition-level spec)
                                                 (.getEncoder types Types/INT Types/PLAIN)
                                                 (.getDecoderFactory types Types/INT Types/PLAIN)
                                                 (.getCompressor types Types/DEFLATE))
                     (.write input-values))
            reader (DataPage$Reader/create (helpers/output-buffer->byte-buffer writer)
                                           (:max-repetition-level spec) (:max-definition-level spec)
                                           (.getDecoderFactory types Types/INT Types/PLAIN)
                                           (.getDecompressorFactory types Types/NONE))]
        (is (= (.read reader) (.read reader)))))
    (testing "read seq is chunked"
      (let [spec {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
            output-values (write-read-data-page spec input-values)]
        (is (chunked-seq? (seq output-values)))))))

(defn- write-read-dictionary-page
  [{:keys [type encoding compression f] :or {type Types/INT encoding Types/PLAIN compression Types/NONE}}
   input-values]
  (let [writer (doto (DictionaryPage$Writer/create (.getEncoder types type encoding)
                                                   (.getCompressor types compression))
                 (.write input-values))
        bb (helpers/output-buffer->byte-buffer writer)
        reader (DictionaryPage$Reader/create bb
                                             (.getDecoderFactory types type encoding)
                                             (.getDecompressorFactory types compression))]
    (seq (if f (.readWith reader f) (.read reader)))))

(deftest dictionary-page
  (testing "write/read a dictionary page"
    (testing "uncompressed"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-dictionary-page {} input-values)]
        (is (= output-values input-values))))
    (testing "with a function"
      (let [f (partial * 3)
            input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-dictionary-page {:f f} input-values)]
        (is (= output-values (map f input-values)))))
    (testing "compressed"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            output-values (write-read-dictionary-page {:compression Types/DEFLATE} input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [input-values []
            output-values (write-read-dictionary-page {}  input-values)]
        (is (empty? output-values))))
    (testing "repeatable writes"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            writer (doto (DictionaryPage$Writer/create (.getEncoder types Types/INT Types/PLAIN)
                                                       (.getCompressor types Types/NONE))
                          (.write input-values))
            mos1 (doto (MemoryOutputStream. 10)
                   (.write writer))
            mos2 (doto (MemoryOutputStream. 10)
                   (.write writer))]
        (is (= (-> mos1 helpers/output-buffer->byte-buffer .array seq)
               (-> mos2 helpers/output-buffer->byte-buffer .array seq)))))
    (testing "repeatable reads"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            writer (doto (DictionaryPage$Writer/create (.getEncoder types Types/INT Types/PLAIN)
                                                       (.getCompressor types Types/NONE))
                     (.write input-values))
            reader (DictionaryPage$Reader/create (helpers/output-buffer->byte-buffer writer)
                                                 (.getDecoderFactory types Types/INT Types/PLAIN)
                                                 (.getDecompressorFactory types Types/NONE))]
        (is (= (seq (.read reader)) (seq (.read reader))))))))

(deftest multiple-pages
  (testing "regular data pages"
    (let [spec {:max-definition-level 3 :max-repetition-level 2}
          input-values (->> (repeatedly helpers/rand-int) (helpers/leveled spec) (take 1000))
          writer (doto (DataPage$Writer/create (:max-repetition-level spec)
                         (:max-definition-level spec)
                         (.getEncoder types Types/INT Types/PLAIN)
                         (.getDecoderFactory types Types/INT Types/PLAIN)
                         (.getCompressor types Types/NONE))
                   (.write input-values))
          mos (MemoryOutputStream. 10)
          num-pages 10]
      (dotimes [i num-pages]
        (Pages/writeTo mos writer))
      (testing "read page-by-page"
        (let [data-page-readers (Pages/getDataPageReaders (.byteBuffer mos)
                                                          num-pages
                                                          (:max-repetition-level spec)
                                                          (:max-definition-level spec)
                                                          (.getDecoderFactory types Types/INT Types/PLAIN)
                                                          (.getDecompressorFactory types Types/NONE))]
          (is (= (repeat num-pages input-values)
                 (for [^DataPage$Reader rdr data-page-readers]
                   (utils/flatten-1 (.read rdr)))))))
      (testing "read partitionned"
        (let [partition-length 31
              partitioned-values (Pages/readDataPagesPartitioned (.byteBuffer mos)
                                                                 num-pages
                                                                 partition-length
                                                                 (:max-repetition-level spec)
                                                                 (:max-definition-level spec)
                                                                 (.getDecoderFactory types Types/INT Types/PLAIN)
                                                                 (.getDecompressorFactory types Types/NONE)
                                                                 nil)]
          (is (->> partitioned-values butlast (map count) (every? (partial = partition-length))))
          (is (= (utils/flatten-1 (repeat num-pages input-values))
                 (mapcat utils/flatten-1 partitioned-values)))))
      (testing "trying to read dictionary throws exception"
        (is (thrown-with-msg? IllegalStateException #"is not a dictionary page type"
                              (first (Pages/readDataPagesWithDictionaryPartitioned
                                      (.byteBuffer mos)
                                      num-pages
                                      31
                                      (:max-repetition-level spec)
                                      (:max-definition-level spec)
                                      (.getDecoderFactory types Types/INT Types/PLAIN)
                                      (.getDecoderFactory types Types/INT Types/VLQ)
                                      (.getDecompressorFactory types Types/NONE)
                                      nil))))))))
