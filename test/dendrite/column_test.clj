(ns dendrite.column-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.core :refer [leveled-value]]
            [dendrite.column :refer :all]
            [dendrite.encoding :refer [str->utf8-bytes]]
            [dendrite.page :as page]
            [dendrite.schema :refer [column-type]]
            [dendrite.test-helpers :refer [get-byte-array-reader] :as helpers])
  (:import [dendrite.java ByteArrayWriter]
           [java.util Date Calendar]
           [java.text SimpleDateFormat])
  (:refer-clojure :exclude [read]))

(def test-schema-path [:foo :bar])

(defn- rand-leveled-values [coll]
  (let [schema-depth (count test-schema-path)]
    (lazy-seq
     (let [definition-level (rand-int (inc schema-depth))
           repetition-level (rand-int (inc schema-depth))]
       (if (= definition-level schema-depth)
         (cons (leveled-value repetition-level definition-level (first coll))
               (rand-leveled-values (rest coll)))
         (cons (leveled-value repetition-level definition-level nil)
               (rand-leveled-values coll)))))))

(defn- rand-partition [n coll]
  (lazy-seq
   (when-not (empty? coll)
     (let [k (inc (rand-int n))]
       (cons (take k coll) (rand-partition n (drop k coll)))))))

(defn- rand-blocks [rand-fn]
  (->> (repeatedly rand-fn) rand-leveled-values (rand-partition 3)))

(def target-data-page-size 1000)

(defn- avg [coll] (/ (reduce + coll) (count coll)))

(defn- abs [x] (if (pos? x) x (- x)))

(defn- roughly=
  ([a b] (roughly= a b 0.1))
  ([a b r] (< (abs (- a b)) (* a r))))

(defn write-blocks [column-writer blocks]
  (reduce write column-writer blocks))

(defn write-column-and-get-reader
  ([column-type input-blocks] (write-column-and-get-reader column-type test-schema-path input-blocks))
  ([column-type schema-path input-blocks]
     (let [writer (doto (column-writer target-data-page-size schema-path column-type)
                    (write-blocks input-blocks)
                    .finish)
           column-chunk-metadata (metadata writer)]
       (-> writer
           get-byte-array-reader
           (column-reader column-chunk-metadata schema-path column-type)))))

(deftest data-column
  (let [ct (column-type :int32 :plain :deflate false)
        input-blocks (->> #(helpers/rand-int-bits 10) rand-blocks (take 5000))
        reader (write-column-and-get-reader ct input-blocks)
        num-pages (-> reader :column-chunk-metadata :num-data-pages)
        output-values (read reader)]
    (testing "Write/read a colum works"
      (is (roughly= num-pages 13))
      (is (= (flatten input-blocks) output-values)))
    (testing "value mapping works"
      (is (= (->> input-blocks flatten (map #(some-> % :value (* 2))))
             (map :value (read reader (partial * 2))))))
    (testing "repeatable writes"
      (let [writer (doto (column-writer target-data-page-size test-schema-path ct)
                     (write-blocks input-blocks))
            baw1 (doto (ByteArrayWriter. 10) (.write writer))
            baw2 (doto (ByteArrayWriter. 10) (.write writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatble reads"
      (is (= (read reader) (read reader))))
    (testing "Page size estimation converges"
      (->> (page/read-data-page-headers (:byte-array-reader reader) num-pages)
           rest                         ; the first page is always inaccurate
           butlast                      ; the last page can have any size
           (map data-page-header->partial-column-stats)
           (map #(+ (:header-bytes %) (:repetition-level-bytes %) (:definition-level-bytes %)
                    (:data-bytes %)))
           avg
           (roughly= target-data-page-size)))))

(deftest dictionary-column
  (let [ct (column-type :int32 :dictionary :deflate false)
        input-blocks (->> #(helpers/rand-int-bits 10) rand-blocks (take 5000))
        reader (write-column-and-get-reader ct input-blocks)
        output-values (read reader)]
    (testing "Write/read a dictionary colum works"
      (is (= (flatten input-blocks) output-values)))
    (testing "value mapping works"
      (is (= (->> input-blocks flatten (map #(some-> % :value (* 2))))
             (map :value (read reader (partial * 2))))))
    (testing "repeatable writes"
      (let [writer (doto (column-writer target-data-page-size test-schema-path ct)
                     (write-blocks input-blocks))
            baw1 (doto (ByteArrayWriter. 10) (.write writer))
            baw2 (doto (ByteArrayWriter. 10) (.write writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatble reads"
      (is (= (read reader) (read reader))))))

(defn- wrap-top-level-required [v] (leveled-value 0 1 v))

(defn- rand-top-level-required-blocks [s]
  (->> s (map wrap-top-level-required) (rand-partition 3)))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [ct (column-type :boolean :plain :none true)
          input-blocks (->> (repeatedly helpers/rand-bool) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "mostly true booleans"
    (let [ct (column-type :boolean :plain :none true)
          input-blocks (->> (repeatedly #(helpers/rand-biased-bool 0.99))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-int32-encodings
  (testing "random int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-blocks (->> (repeatedly helpers/rand-int32) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "random small int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-blocks (->> (repeatedly #(helpers/rand-int-bits 10))
                            rand-top-level-required-blocks
                            (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :packed-run-length (find-best-encoding reader target-data-page-size)))))
  (testing "increasing int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-blocks (->> (range) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random int32s"
    (let [ct (column-type :int32 :plain :none true)
          random-ints (repeatedly 100 helpers/rand-int32)
          input-blocks (->> (repeatedly #(helpers/rand-member random-ints))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-int64-encodings
  (testing "random int64s"
    (let [ct (column-type :int64 :plain :none true)
          input-blocks (->> (repeatedly helpers/rand-int64) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "random small int64s"
    (let [ct (column-type :int64 :plain :none true)
          input-blocks (->> (repeatedly #(helpers/rand-int-bits 10))
                            rand-top-level-required-blocks
                            (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "increasing int64s"
    (let [ct (column-type :int64 :plain :none true)
          input-blocks (->> (range) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "increasing timestamps"
    (let [ct (column-type :int64 :plain :none true)
          input-blocks (->> (repeatedly #(System/nanoTime)) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random int64s"
    (let [ct (column-type :int64 :plain :none true)
          random-ints (repeatedly 100 helpers/rand-int64)
          input-blocks (->> (repeatedly #(helpers/rand-member random-ints))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-float-encodings
  (testing "random floats"
    (let [ct (column-type :float :plain :none true)
          input-blocks (->> (repeatedly helpers/rand-float) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random floats"
    (let [ct (column-type :float :plain :none true)
          random-floats (repeatedly 100 helpers/rand-float)
          input-blocks (->> (repeatedly #(helpers/rand-member random-floats))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-double-encodings
  (testing "random doubles"
    (let [ct (column-type :double :plain :none true)
          input-blocks (->> (repeatedly helpers/rand-double) rand-top-level-required-blocks (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random doubles"
    (let [ct (column-type :double :plain :none true)
          random-doubles (repeatedly 100 helpers/rand-double)
          input-blocks (->> (repeatedly #(helpers/rand-member random-doubles))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(def simple-date-format (SimpleDateFormat. "dd/MM/yyyy"))

(defn- iterate-calendar-by-day [calendar]
  (lazy-seq (cons (.getTime calendar)
                  (iterate-calendar-by-day (doto calendar (.add Calendar/DATE 1))))))

(deftest find-best-byte-array-encodings
  (testing "random byte arrays"
    (let [ct (column-type :byte-array :plain :none true)
          input-blocks (->> (repeatedly #(helpers/rand-byte-array))
                            rand-top-level-required-blocks
                            (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :delta-length (find-best-encoding reader target-data-page-size)))))
  (testing "incrementing dates"
    (let [ct (column-type :byte-array :plain :none true)
          calendar (doto (Calendar/getInstance)
                     (.setTime (.parse simple-date-format "01/01/2014")))
          input-blocks (->> (iterate-calendar-by-day calendar)
                          (map (comp str->utf8-bytes #(.format simple-date-format %)))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :incremental (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random byte arrays"
    (let [ct (column-type :byte-array :plain :none true)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array))
          input-blocks (->> (repeatedly #(helpers/rand-member rand-byte-arrays))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-fixed-length-byte-array-encodings
  (testing "random byte arrays"
    (let [ct (column-type :fixed-length-byte-array :plain :none true)
          input-blocks (->> (repeatedly #(helpers/rand-byte-array 16))
                            rand-top-level-required-blocks
                            (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random byte arrays"
    (let [ct (column-type :fixed-length-byte-array :plain :none true)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          input-blocks (->> (repeatedly #(helpers/rand-member rand-byte-arrays))
                          rand-top-level-required-blocks
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-compression-types
  (testing "plain encoded random int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-blocks (->> (repeatedly #(helpers/rand-int-bits 10))
                            rand-top-level-required-blocks
                            (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= :none (find-best-compression-type reader target-data-page-size {})))
      (is (= :deflate (find-best-compression-type reader target-data-page-size {:lz4 0.9 :deflate 0.5})))
      (is (= :lz4 (find-best-compression-type reader target-data-page-size {:lz4 0.9 :deflate 0.2})))
      (is (= :none (find-best-compression-type reader target-data-page-size {:lz4 0.5 :deflate 0.2}))))))

(deftest find-best-column-types
  (testing "lorem ispum permutations"
    (let [ct (column-type :byte-array :plain :none true)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          lorem-ipsum-words (-> helpers/lorem-ipsum (string/split #" ") set)
          lorem-ipsum-shuffles (repeatedly 100 #(->> lorem-ipsum-words shuffle (apply str)))
          input-blocks (->>  (repeatedly #(-> lorem-ipsum-shuffles helpers/rand-member str->utf8-bytes))
                           rand-top-level-required-blocks
                           (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-blocks)]
      (is (= (assoc ct
               :encoding :dictionary
               :compression-type :deflate)
             (find-best-column-type reader target-data-page-size {:lz4 0.8 :deflate 0.5}))))))
