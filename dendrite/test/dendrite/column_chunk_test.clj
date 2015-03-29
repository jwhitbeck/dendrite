;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.column-chunk-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.column-chunk :refer :all]
            [dendrite.encoding :as encoding]
            [dendrite.page :as page]
            [dendrite.metadata :as metadata]
            [dendrite.stats :as stats]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java LeveledValue OutputBuffer]
           [java.util Date Calendar]
           [java.text SimpleDateFormat])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(def test-target-data-page-length 1000)

(defn write-blocks! [column-chunk-writer blocks]
  (doseq [block blocks]
    (write! column-chunk-writer block)))

(defn write-column-chunk-and-get-reader
  ([column-spec input-blocks]
     (write-column-chunk-and-get-reader column-spec test-target-data-page-length
                                        helpers/default-type-store input-blocks))
  ([column-spec target-data-page-length type-store input-blocks]
   (let [^OutputBuffer w (writer target-data-page-length type-store column-spec)]
     (write-blocks! w input-blocks)
     (.finish w)
     (-> (helpers/output-buffer->byte-buffer w)
         (reader (metadata w) type-store column-spec)))))

(def ^SimpleDateFormat simple-date-format (SimpleDateFormat. "yyyy-MM-dd"))

(defn- iterate-calendar-by-day [^Calendar calendar]
  (lazy-seq (cons (.getTime calendar)
                  (iterate-calendar-by-day (doto calendar (.add Calendar/DATE 1))))))

(defn- days-seq [start-date-str]
  (-> (doto (Calendar/getInstance)
        (.setTime (.parse ^SimpleDateFormat simple-date-format start-date-str)))
      iterate-calendar-by-day))

(defn- column-spec [value-type encoding compression]
  (metadata/map->ColumnSpec
   {:type value-type
    :encoding encoding
    :compression compression
    :max-definition-level 3
    :max-repetition-level 2}))

(defn- column-spec-non-repeated [value-type encoding compression]
  (metadata/map->ColumnSpec
   {:type value-type
    :encoding encoding
    :compression compression
    :max-definition-level 3
    :max-repetition-level 0}))

(defn- column-spec-required [value-type encoding compression]
  (metadata/map->ColumnSpec
   {:type value-type
    :encoding encoding
    :compression compression
    :max-definition-level 0
    :max-repetition-level 0}))

(defn partition-by-record [leveled-values]
  (lazy-seq
   (when-let [coll (seq leveled-values)]
     (let [fst (first coll)
           [keep remaining] (split-with #(-> ^LeveledValue % .repetitionLevel pos?) (next coll))]
       (cons (cons fst keep) (partition-by-record remaining))))))

(defn- rand-blocks [column-spec coll]
  (->> coll (helpers/leveled column-spec) partition-by-record))

(deftest data-column-chunk
  (let [cs (column-spec :int :plain :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 1100))
        reader (write-column-chunk-and-get-reader cs input-blocks)
        num-pages (-> reader :column-chunk-metadata :num-data-pages)
        output-blocks (read reader)]
    (testing "write/read a data colum-chunk"
      (is (helpers/roughly num-pages 4))
      (is (= input-blocks output-blocks)))
    (testing "value mapping"
        (let [map-fn (partial * 2)
              mapped-reader (write-column-chunk-and-get-reader (assoc cs :map-fn map-fn) input-blocks)]
          (is (= (->> input-blocks flatten (map #(some-> (.value ^LeveledValue %) map-fn)))
                 (->> mapped-reader read flatten (map #(.value ^LeveledValue %)))))))
    (testing "repeatable writes"
      (let [^OutputBuffer w (writer test-target-data-page-length helpers/default-type-store cs)]
        (write-blocks! w input-blocks)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (read reader) (read reader))))
    (testing "Page length estimation converges"
      (letfn [(avg-page-length [target-length]
                (let [reader (write-column-chunk-and-get-reader cs target-length
                                                                helpers/default-type-store input-blocks)
                      num-data-pages (-> reader :column-chunk-metadata :num-data-pages)]
                  (->> (page/read-data-page-headers (:byte-buffer reader) num-data-pages)
                       rest                      ; the first page is always inaccurate
                       butlast                   ; the last page can have any length
                       (map (comp :length page/stats))
                       helpers/avg)))]
        (is (helpers/roughly 1024 (avg-page-length 1024)))
        (is (helpers/roughly 256 (avg-page-length 256)))))
    (testing "read seq is chunked"
      (is (chunked-seq? (seq (read reader)))))))

(deftest dictionary-column-chunk
  (let [cs (column-spec :int :dictionary :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 1000))
        reader (write-column-chunk-and-get-reader cs input-blocks)
        output-blocks (read reader)]
    (testing "write/read a dictionary colum-chunk"
      (is (= input-blocks output-blocks)))
    (testing "value mapping"
      (let [map-fn (partial * 2)
            mapped-reader (write-column-chunk-and-get-reader (assoc cs :map-fn map-fn) input-blocks)]
        (is (= (->> input-blocks flatten (map #(some-> (.value ^LeveledValue %) map-fn)))
               (->> mapped-reader read flatten (map #(.value ^LeveledValue %)))))))
    (testing "repeatable writes"
      (let [^OutputBuffer w (writer test-target-data-page-length helpers/default-type-store cs)]
        (write-blocks! w input-blocks)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (read reader) (read reader))))))

(deftest frequency-column-chunk
  (let [cs (column-spec :int :frequency :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 1000))
        reader (write-column-chunk-and-get-reader cs input-blocks)
        output-blocks (read reader)]
    (testing "write/read a frequency colum-chunk"
      (is (= input-blocks output-blocks)))
    (testing "value mapping"
      (let [map-fn (partial * 2)
            mapped-reader (write-column-chunk-and-get-reader (assoc cs :map-fn map-fn) input-blocks)]
        (is (= (->> input-blocks flatten (map #(some-> (.value ^LeveledValue %) map-fn)))
               (->> mapped-reader read flatten (map #(.value ^LeveledValue %)))))))
    (testing "repeatable writes"
      (let [^OutputBuffer w (writer test-target-data-page-length helpers/default-type-store cs)]
        (write-blocks! w input-blocks)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (read reader) (read reader))))))

(defn- find-best-encoding* [reader] (find-best-encoding reader test-target-data-page-length))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [cs (column-spec-required :boolean :plain :none)
          input-blocks (->> helpers/rand-bool repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "mostly true booleans"
    (let [cs (column-spec-required :boolean :plain :none)
          input-blocks (->> #(helpers/rand-biased-bool 0.99) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-int-encodings
  (testing "random ints"
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (->> helpers/rand-int repeatedly (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "random ints (non-repeated)"
    (let [cs (column-spec-non-repeated :int :plain :none)
          input-blocks (->> helpers/rand-int repeatedly (helpers/rand-map 0.2 (constantly nil)) (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "random small ints"
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :packed-run-length (find-best-encoding* reader)))))
  (testing "increasing ints"
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (->> (range) (map int) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding* reader)))))
  (testing "small selection of random ints"
    (let [cs (column-spec-required :int :plain :none)
          random-ints (repeatedly 100 helpers/rand-int)
          input-blocks (->> #(rand-nth random-ints) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader)))))
  (testing "skewed selection of random ints"
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (concat (->> helpers/rand-int repeatedly (take 255))
                               (apply interleave (repeatedly 10 #(take 100 (repeat (helpers/rand-int))))))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :frequency (find-best-encoding reader (* 100 1024))))))
  (testing "small random signed ints with an occasional large one."
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 7)
                            repeatedly
                            (helpers/rand-map 0.1 (constantly (helpers/rand-int-bits 24)))
                            (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :vlq (find-best-encoding* reader)))))
  (testing "small random signed ints with an occasional large one."
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 7)
                            repeatedly
                            (helpers/rand-map 0.1 (constantly (helpers/rand-int-bits 24)))
                            (map * (repeatedly helpers/rand-sign))
                            (map unchecked-int)
                            (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :zig-zag (find-best-encoding* reader)))))
  (testing "small selection of chars"
    (let [cs (column-spec-required :char :plain :none)
          input-blocks (->> #(rand-nth [\c \return \u1111]) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-long-encodings
  (testing "random longs"
    (let [cs (column-spec-required :long :plain :none)
          input-blocks (->> helpers/rand-long repeatedly (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "random small longs"
    (let [cs (column-spec-required :long :plain :none)
          input-blocks (->> #(helpers/rand-long-bits 10) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding* reader)))))
  (testing "small random longs with an occasional large one."
    (let [cs (column-spec-required :long :plain :none)
          input-blocks (->> #(helpers/rand-long-bits 7)
                            repeatedly
                            (helpers/rand-map 0.1 (constantly (helpers/rand-long-bits 24)))
                            (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :vlq (find-best-encoding* reader)))))
  (testing "small random signed longs with an occasional large one."
    (let [cs (column-spec-required :long :plain :none)
          input-blocks (->> #(helpers/rand-long-bits 7)
                            repeatedly
                            (helpers/rand-map 0.1 (constantly (helpers/rand-long-bits 24)))
                            (map * (repeatedly helpers/rand-sign))
                            (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :zig-zag (find-best-encoding* reader)))))
  (testing "increasing longs"
    (let [cs (column-spec-required :long :plain :none)
          input-blocks (->> (range) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding* reader)))))
  (testing "increasing timestamps"
    (let [cs (column-spec-required :long :plain :none)
          input-blocks (->> #(System/nanoTime) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding* reader)))))
  (testing "incrementing dates as a custom-type"
    (let [cs (column-spec-required :inst :plain :none)
          input-blocks (->> (days-seq "2014-01-01") (take 1000))]
      (let [reader (write-column-chunk-and-get-reader cs input-blocks)]
        (is (= (read reader) input-blocks))
        (is (= :delta (find-best-encoding* reader))))))
  (testing "small selection of random longs"
    (let [cs (column-spec-required :long :plain :none)
          random-ints (repeatedly 100 helpers/rand-long)
          input-blocks (->> #(rand-nth random-ints) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-float-encodings
  (testing "random floats"
    (let [cs (column-spec-required :float :plain :none)
          input-blocks (->> helpers/rand-float repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "small selection of random floats"
    (let [cs (column-spec-required :float :plain :none)
          random-floats (repeatedly 100 helpers/rand-float)
          input-blocks (->> #(rand-nth random-floats) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-double-encodings
  (testing "random doubles"
    (let [cs (column-spec-required :double :plain :none)
          input-blocks (->> helpers/rand-double repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "small selection of random doubles"
    (let [cs (column-spec-required :double :plain :none)
          random-doubles (repeatedly 100 helpers/rand-double)
          input-blocks (->> #(rand-nth random-doubles) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-byte-array-encodings
  (testing "random byte arrays"
    (let [cs (column-spec-required :byte-array :plain :none)
          input-blocks (->> #(helpers/rand-byte-array) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (map seq (read reader) (map seq input-blocks))))
      (is (= :delta-length (find-best-encoding* reader)))))
  (testing "random big ints"
    (let [cs (column-spec-required :bigint :plain :none)
          input-blocks (->> #(helpers/rand-bigint 100) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta-length (find-best-encoding* reader)))))
  (testing "random big decimals"
    (let [cs (column-spec-required :bigdec :plain :none)
          input-blocks (->> #(helpers/rand-bigdec 40) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :incremental (find-best-encoding* reader)))))
  (testing "random ratios"
    (let [cs (column-spec-required :ratio :plain :none)
          input-blocks (->> #(helpers/rand-ratio 40) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :incremental (find-best-encoding* reader)))))
  (testing "incrementing dates"
    (let [cs (column-spec-required :date-str :plain :none)
          input-blocks (->> (days-seq "2014-01-01") (take 1000))
          type-store (encoding/type-store {:date-str {:base-type :string
                                                      :to-base-type-fn #(locking simple-date-format
                                                                          (.format simple-date-format %))
                                                      :from-base-type-fn #(locking simple-date-format
                                                                            (.parse simple-date-format %))}})]
      (let [reader (write-column-chunk-and-get-reader cs test-target-data-page-length type-store input-blocks)]
        (is (= (read reader) input-blocks))
        (is (= :incremental (find-best-encoding* reader))))))
  (testing "small set of keywords"
    (let [cs (column-spec-required :keyword :plain :none)
          input-blocks (->> #(rand-nth [:foo ::bar :baz]) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader)))))
  (testing "small set of symbols"
    (let [cs (column-spec-required :symbol :plain :none)
          input-blocks (->> #(rand-nth ['foo 'bar 'baz]) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-fixed-length-byte-array-encodings
  (testing "random byte arrays"
    (let [cs (column-spec-required :fixed-length-byte-array :plain :none)
          input-blocks (->> #(helpers/rand-byte-array 16) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (map seq (read reader)) (map seq input-blocks)))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "UUIDs"
    (let [cs (column-spec-required :uuid :plain :none)
          input-blocks (->> helpers/rand-uuid repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding* reader)))))
  (testing "small selection of random byte arrays"
    (let [cs (column-spec-required :fixed-length-byte-array :plain :none)
          rand-byte-arrays (repeatedly 10 #(helpers/rand-byte-array 16))
          input-blocks (->> #(rand-nth rand-byte-arrays) repeatedly (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (map seq (read reader)) (map seq input-blocks)))
      (is (= :dictionary (find-best-encoding* reader))))))

(deftest find-best-compression-types
  (testing "plain encoded random ints"
    (let [cs (column-spec-required :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (take 5000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (testing "different compression thresholds"
        (are [cmp cmp-thresholds-map] (= cmp (find-best-compression reader
                                                                    test-target-data-page-length
                                                                    cmp-thresholds-map))
             :none {}
             :deflate {:lz4 1.0 :deflate 1.5}
             :lz4 {:lz4 1.0 :deflate 5}
             :none {:lz4 2 :deflate 5}))
      (testing "unsupported compression types throw proper exceptions"
        (is (thrown-with-msg? IllegalArgumentException #"is not a valid compression-type"
                              (find-best-compression reader test-target-data-page-length {:lzo 0.8})))))))

(deftest find-best-column-types
  (testing "lorem ispum permutations"
    (let [cs (column-spec-required :string :plain :none)
          lorem-ipsum-words (-> helpers/lorem-ipsum (string/split #" ") set)
          lorem-ipsum-shuffles (repeatedly 100 #(->> lorem-ipsum-words shuffle (apply str)))
          input-blocks (->> #(rand-nth lorem-ipsum-shuffles)
                            repeatedly
                            (take 5000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (assoc cs
               :encoding :delta-length
               :compression :deflate)
             (find-best-column-spec reader test-target-data-page-length {:lz4 0.8 :deflate 0.5}))))))

(deftest optimization
  (let [cs (column-spec-required :string :plain :none)
        input-blocks (->> #(rand-nth ["foo" "bar" "baz"]) repeatedly (take 1000))
        w (reduce write! (writer test-target-data-page-length helpers/default-type-store cs) input-blocks)
        optimized-w (optimize! w helpers/default-type-store {:lz4 1.2 :deflate 2})]
    (is (= {:type :string :encoding :dictionary :compression :none}
           (-> optimized-w :column-spec (select-keys [:type :encoding :compression]))))
    (is (= input-blocks (read (writer->reader! optimized-w helpers/default-type-store))))))
