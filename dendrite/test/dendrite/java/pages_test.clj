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
            [dendrite.test-helpers :as helpers :refer [flatten-1]])
  (:import [dendrite.java DataPage$Reader DataPage$Writer Dictionary$DecoderFactory DictionaryPage$Reader
            DictionaryPage$Writer LeveledValue MemoryOutputStream Pages Types]))

(set! *warn-on-reflection* true)

(def ^Types types (Types/create))

(defn- write-read-data-page
  [{:keys [max-repetition-level max-definition-level type encoding compression f]
    :or {type Types/INT encoding Types/PLAIN compression Types/NONE}}
   input-values]
  (let [writer (DataPage$Writer/create max-repetition-level max-definition-level
                                       (.getEncoder types type encoding)
                                       (.getCompressor types compression))]
    (doseq [v (cond->> input-values (pos? max-repetition-level) helpers/partition-by-record)]
      (.write writer v))
    (let [bb (helpers/output-buffer->byte-buffer writer)
          reader (DataPage$Reader/create bb max-repetition-level max-definition-level
                                         (.getDecoderFactory types type encoding f)
                                         (.getDecompressorFactory types compression))]
      (cond->> (seq reader)
        (pos? max-repetition-level) (mapcat seq)))))

(deftest data-page
  (testing "write/read a data page"
    (testing "default"
      (let [levels {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled levels) (take 1000))
            output-values (write-read-data-page levels input-values)]
        (is (= output-values input-values))))
    (testing "with a function"
      (let [f (fnil (partial * 2) 1)
            levels {:max-definition-level 3 :max-repetition-level 2 :f f}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled levels) (take 1000))
            output-values (write-read-data-page levels input-values)]
        (is (= output-values (helpers/map-leveled f input-values)))))
    (testing "all nils"
      (let [levels {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeat nil) (helpers/leveled levels) (take 1000))
            output-values (write-read-data-page levels input-values)]
        (is (= output-values input-values))))
    (testing "compressed"
      (let [levels {:max-definition-level 3 :max-repetition-level 2 :compression Types/DEFLATE}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled levels) (take 1000))
            output-values (write-read-data-page levels input-values)]
        (is (= output-values input-values))))
    (testing "required"
      (let [levels {:max-definition-level 0 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int) (take 1000))
            output-values (write-read-data-page levels input-values)]
        (is (= output-values input-values))))
    (testing "non-repeated"
      (let [levels {:max-definition-level 2 :max-repetition-level 0}
            input-values (->> (repeatedly helpers/rand-int)
                              (helpers/rand-map 0.2 (constantly nil))
                              (take 1000))
            output-values (write-read-data-page levels input-values)]
        (is (= output-values input-values))))
    (testing "empty page"
      (let [levels {:max-definition-level 3 :max-repetition-level 2}
            input-values []
            output-values (write-read-data-page levels input-values)]
        (is (= output-values input-values))))
    (testing "repeatable writes"
      (let [levels {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled levels) (take 1000))
            writer (doto (DataPage$Writer/create (:max-repetition-level levels)
                                                 (:max-definition-level levels)
                                                 (.getEncoder types Types/INT Types/PLAIN)
                                                 (.getCompressor types Types/DEFLATE))
                     (.write input-values))
            mos1 (doto (MemoryOutputStream. 10)
                   (.write writer))
            mos2 (doto (MemoryOutputStream. 10)
                   (.write writer))]
        (is (= (-> mos1 helpers/output-buffer->byte-buffer .array seq)
               (-> mos2 helpers/output-buffer->byte-buffer .array seq)))))
    (testing "repeatable reads"
      (let [levels {:max-definition-level 3 :max-repetition-level 2}
            input-values (->> (repeatedly helpers/rand-int) (helpers/leveled levels) (take 1000))
            writer (doto (DataPage$Writer/create (:max-repetition-level levels)
                                                 (:max-definition-level levels)
                                                 (.getEncoder types Types/INT Types/PLAIN)
                                                 (.getCompressor types Types/DEFLATE))
                     (.write input-values))
            reader (DataPage$Reader/create (helpers/output-buffer->byte-buffer writer)
                                           (:max-repetition-level levels) (:max-definition-level levels)
                                           (.getDecoderFactory types Types/INT Types/PLAIN)
                                           (.getDecompressorFactory types Types/NONE))]
        (is (= (seq reader) (seq reader)))))))

(defn- write-read-dictionary-page
  [{:keys [type encoding compression f] :or {type Types/INT encoding Types/PLAIN compression Types/NONE}}
   input-values]
  (let [writer (DictionaryPage$Writer/create (.getEncoder types type encoding)
                 (.getCompressor types compression))]
    (doseq [v input-values]
      (.write writer v))
    (let [bb (helpers/output-buffer->byte-buffer writer)
          reader (DictionaryPage$Reader/create bb
                   (.getDecoderFactory types type encoding f)
                   (.getDecompressorFactory types compression))]
      (seq (.read reader)))))

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
            writer (DictionaryPage$Writer/create (.getEncoder types Types/INT Types/PLAIN)
                     (.getCompressor types Types/NONE))]
        (doseq [v input-values]
          (.write writer v))
        (let [mos1 (doto (MemoryOutputStream. 10)
                     (.write writer))
              mos2 (doto (MemoryOutputStream. 10)
                     (.write writer))]
          (is (= (-> mos1 helpers/output-buffer->byte-buffer .array seq)
                 (-> mos2 helpers/output-buffer->byte-buffer .array seq))))))
    (testing "repeatable reads"
      (let [input-values (repeatedly 1000 helpers/rand-int)
            writer (DictionaryPage$Writer/create (.getEncoder types Types/INT Types/PLAIN)
                     (.getCompressor types Types/NONE))]
        (doseq [v input-values]
          (.write writer v))
        (let [reader (DictionaryPage$Reader/create (helpers/output-buffer->byte-buffer writer)
                       (.getDecoderFactory types Types/INT Types/PLAIN)
                       (.getDecompressorFactory types Types/NONE))]
          (is (= (seq (.read reader)) (seq (.read reader)))))))))

(deftest multiple-data-pages
  (let [levels {:max-definition-level 3 :max-repetition-level 2}
        input-values (->> (repeatedly helpers/rand-int) (helpers/leveled levels) (take 1000))
        writer (DataPage$Writer/create (:max-repetition-level levels)
                 (:max-definition-level levels)
                 (.getEncoder types Types/INT Types/PLAIN)
                 (.getCompressor types Types/NONE))
        mos (MemoryOutputStream. 10)
        num-pages 10]
    (doseq [v (helpers/partition-by-record input-values)]
      (.write writer v))
    (dotimes [i num-pages]
      (Pages/writeTo mos writer))
    (testing "read page-by-page"
      (let [data-page-readers (Pages/getDataPageReaders (.toByteBuffer mos)
                                                        num-pages
                                                        (:max-repetition-level levels)
                                                        (:max-definition-level levels)
                                                        (.getDecoderFactory types Types/INT Types/PLAIN)
                                                        (.getDecompressorFactory types Types/NONE))]
        (is (= (repeat num-pages input-values)
               (for [^DataPage$Reader rdr data-page-readers]
                 (flatten-1 rdr))))))
    (testing "read partitionned"
      (let [partition-length 31
            partitioned-values (Pages/readAndPartitionDataPages
                                (.toByteBuffer mos)
                                num-pages
                                partition-length
                                (:max-repetition-level levels)
                                (:max-definition-level levels)
                                (.getDecoderFactory types Types/INT Types/PLAIN)
                                (.getDecompressorFactory types Types/NONE))]
        (is (->> partitioned-values butlast (map count) (every? (partial = partition-length))))
        (is (= (flatten-1 (repeat num-pages input-values))
               (mapcat flatten-1 partitioned-values)))))
    (testing "read partitionned with fn"
      (let [partition-length 31
            f #(if % (* 2 %) ::null)
            partitioned-values (Pages/readAndPartitionDataPages
                                (.toByteBuffer mos)
                                num-pages
                                partition-length
                                (:max-repetition-level levels)
                                (:max-definition-level levels)
                                (.getDecoderFactory types Types/INT Types/PLAIN f)
                                (.getDecompressorFactory types Types/NONE))]
        (is (->> partitioned-values butlast (map count) (every? (partial = partition-length))))
        (is (= (flatten-1 (repeat num-pages (helpers/map-leveled f input-values)))
               (mapcat flatten-1 partitioned-values)))))
    (testing "trying to read dictionary throws exception"
      (is (thrown-with-msg? IllegalStateException #"is not a dictionary page type"
                            (first (Pages/readAndPartitionDataPagesWithDictionary
                                    (.toByteBuffer mos)
                                    num-pages
                                    31
                                    (:max-repetition-level levels)
                                    (:max-definition-level levels)
                                    (.getDecoderFactory types Types/INT Types/PLAIN)
                                    (.getDecoderFactory types Types/INT Types/VLQ)
                                    (.getDecompressorFactory types Types/NONE))))))))

(deftest multiple-data-pages-with-dictionary
  (let [levels {:max-definition-level 3 :max-repetition-level 2}
        ^objects dictionary (into-array ["foo" "bar" "baz" "foobar"])
        dictionary-writer (DictionaryPage$Writer/create (.getEncoder types Types/STRING Types/PLAIN)
                            (.getCompressor types Types/NONE))
        input-indices (->> (repeatedly #(helpers/rand-int-bits 2)) (helpers/leveled levels) (take 1000))
        writer (DataPage$Writer/create (:max-repetition-level levels)
                 (:max-definition-level levels)
                 (.getEncoder types Types/INT Types/PLAIN)
                 (.getCompressor types Types/NONE))
        mos (MemoryOutputStream. 10)
        num-pages 10]
    (doseq [entry (seq dictionary)]
      (.write dictionary-writer entry))
    (doseq [v (helpers/partition-by-record input-indices)]
      (.write writer v))
    (Pages/writeTo mos dictionary-writer)
    (dotimes [i num-pages]
      (Pages/writeTo mos writer))
    (testing "read page-by-page"
      (let [dictionary-decoder-factory (.getDecoderFactory types Types/STRING Types/PLAIN)
            indices-decoder-factory (.getDecoderFactory types Types/INT Types/PLAIN)
            dictionary-reader (Pages/getDictionaryPageReader
                               (.toByteBuffer mos)
                               dictionary-decoder-factory
                               (.getDecompressorFactory types Types/NONE))
            dictionary (.read dictionary-reader)
            data-page-readers (Pages/getDataPageReaders
                               (.getNextBuffer dictionary-reader)
                               num-pages
                               (:max-repetition-level levels)
                               (:max-definition-level levels)
                               (Dictionary$DecoderFactory. dictionary indices-decoder-factory
                                                           dictionary-decoder-factory)
                               (.getDecompressorFactory types Types/NONE))]
        (is (= (->> input-indices
                    (helpers/map-leveled #(when % (aget dictionary (int %))))
                    (repeat num-pages))
               (for [^DataPage$Reader rdr data-page-readers]
                 (flatten-1 rdr))))))
    (testing "read partitionned"
      (let [partition-length 31
            partitioned-values (Pages/readAndPartitionDataPagesWithDictionary
                                (.toByteBuffer mos)
                                num-pages
                                partition-length
                                (:max-repetition-level levels)
                                (:max-definition-level levels)
                                (.getDecoderFactory types Types/STRING Types/PLAIN)
                                (.getDecoderFactory types Types/INT Types/PLAIN)
                                (.getDecompressorFactory types Types/NONE))]
        (is (->> partitioned-values butlast (map count) (every? (partial = partition-length))))
        (is (= (->> input-indices
                    (helpers/map-leveled #(when % (aget dictionary (int %))))
                    (repeat num-pages)
                    flatten-1)
               (mapcat flatten-1 partitioned-values)))))
    (testing "read partitionned with fn"
      (let [partition-length 31
            f #(if % (str "foo" %) ::null)
            partitioned-values (Pages/readAndPartitionDataPagesWithDictionary
                                (.toByteBuffer mos)
                                num-pages
                                partition-length
                                (:max-repetition-level levels)
                                (:max-definition-level levels)
                                (.getDecoderFactory types Types/STRING Types/PLAIN f)
                                (.getDecoderFactory types Types/INT Types/PLAIN)
                                (.getDecompressorFactory types Types/NONE))]
        (is (->> partitioned-values butlast (map count) (every? (partial = partition-length))))
        (is (= (->> input-indices
                    (helpers/map-leveled #(f (when % (aget dictionary (int %)))))
                    (repeat num-pages)
                    flatten-1)
               (mapcat flatten-1 partitioned-values)))))
    (testing "trying to read without dictionary throws exception"
      (is (thrown-with-msg? IllegalStateException #"is not a data page type"
                            (first (Pages/readAndPartitionDataPages
                                    (.toByteBuffer mos)
                                    num-pages
                                    31
                                    (:max-repetition-level levels)
                                    (:max-definition-level levels)
                                    (.getDecoderFactory types Types/INT Types/PLAIN)
                                    (.getDecompressorFactory types Types/NONE))))))))
