(ns dendrite.column-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.column :refer :all]
            [dendrite.encoding :refer [str->utf8-bytes]]
            [dendrite.page :as page]
            [dendrite.schema :refer [column-type]]
            [dendrite.test-helpers :refer [get-byte-array-reader] :as helpers])
  (:import [dendrite.java ByteArrayWriter]
           [java.util Date Calendar]
           [java.text SimpleDateFormat]))

(def test-schema-path [:foo :bar])

(defn- rand-wrap-values [coll]
  (let [schema-depth (count test-schema-path)]
    (lazy-seq
     (let [definition-level (rand-int (inc schema-depth))
           repetition-level (rand-int (inc schema-depth))]
       (if (= definition-level schema-depth)
         (cons (wrap-value repetition-level definition-level (first coll)) (rand-wrap-values (rest coll)))
         (cons (wrap-value repetition-level definition-level nil) (rand-wrap-values coll)))))))

(defn- rand-partition [n coll]
  (lazy-seq
   (when-not (empty? coll)
     (let [k (inc (rand-int n))]
       (cons (take k coll) (rand-partition n (drop k coll)))))))

(defn- rand-rows [rand-fn]
  (->> (repeatedly rand-fn) rand-wrap-values (rand-partition 3)))

(def target-data-page-size 1000)

(defn- avg [coll] (/ (reduce + coll) (count coll)))

(defn- abs [x] (if (pos? x) x (- x)))

(defn- roughly=
  ([a b] (roughly= a b 0.1))
  ([a b r] (< (abs (- a b)) (* a r))))

(defn write-column-and-get-reader
  ([column-type input-rows] (write-column-and-get-reader column-type test-schema-path input-rows))
  ([column-type schema-path input-rows]
     (let [writer (doto (column-writer target-data-page-size schema-path column-type)
                    (write-rows input-rows)
                    .finish)
           column-chunk-metadata (metadata writer)]
       (-> writer
           get-byte-array-reader
           (column-reader column-chunk-metadata schema-path column-type)))))

(deftest data-column
  (let [ct (column-type :int32 :plain :deflate false)
        input-rows (->> #(helpers/rand-int-bits 10) rand-rows (take 5000))
        reader (write-column-and-get-reader ct input-rows)
        num-pages (-> reader :column-chunk-metadata :num-data-pages)
        output-values (read-column reader)]
    (testing "Write/read a colum works"
      (is (roughly= num-pages 13))
      (is (= (flatten input-rows) output-values)))
    (testing "value mapping works"
      (is (= (->> input-rows flatten (map #(some-> % :value (* 2))))
             (map :value (read-column reader (partial * 2))))))
    (testing "repeatable writes"
      (let [writer (doto (column-writer target-data-page-size test-schema-path ct)
                     (write-rows input-rows))
            baw1 (doto (ByteArrayWriter. 10) (.write writer))
            baw2 (doto (ByteArrayWriter. 10) (.write writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatble reads"
      (is (= (read-column reader) (read-column reader))))
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
        input-rows (->> #(helpers/rand-int-bits 10) rand-rows (take 5000))
        reader (write-column-and-get-reader ct input-rows)
        output-values (read-column reader)]
    (testing "Write/read a dictionary colum works"
      (is (= (flatten input-rows) output-values)))
    (testing "value mapping works"
      (is (= (->> input-rows flatten (map #(some-> % :value (* 2))))
             (map :value (read-column reader (partial * 2))))))
    (testing "repeatable writes"
      (let [writer (doto (column-writer target-data-page-size test-schema-path ct)
                     (write-rows input-rows))
            baw1 (doto (ByteArrayWriter. 10) (.write writer))
            baw2 (doto (ByteArrayWriter. 10) (.write writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatble reads"
      (is (= (read-column reader) (read-column reader))))))

(defn- wrap-top-level-required [v] (wrap-value 0 1 v))

(defn- rand-top-level-required-rows [s]
  (->> s (map wrap-top-level-required) (rand-partition 3)))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [ct (column-type :boolean :plain :none true)
          input-rows (->> (repeatedly helpers/rand-bool) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "mostly true booleans"
    (let [ct (column-type :boolean :plain :none true)
          input-rows (->> (repeatedly #(helpers/rand-biased-bool 0.99))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-int32-encodings
  (testing "random int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-rows (->> (repeatedly helpers/rand-int32) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "random small int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-rows (->> (repeatedly #(helpers/rand-int-bits 10)) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :packed-run-length (find-best-encoding reader target-data-page-size)))))
  (testing "increasing int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-rows (->> (range) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random int32s"
    (let [ct (column-type :int32 :plain :none true)
          random-ints (repeatedly 100 helpers/rand-int32)
          input-rows (->> (repeatedly #(helpers/rand-member random-ints))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-int64-encodings
  (testing "random int64s"
    (let [ct (column-type :int64 :plain :none true)
          input-rows (->> (repeatedly helpers/rand-int64) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "random small int64s"
    (let [ct (column-type :int64 :plain :none true)
          input-rows (->> (repeatedly #(helpers/rand-int-bits 10)) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "increasing int64s"
    (let [ct (column-type :int64 :plain :none true)
          input-rows (->> (range) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "increasing timestamps"
    (let [ct (column-type :int64 :plain :none true)
          input-rows (->> (repeatedly #(System/nanoTime)) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random int64s"
    (let [ct (column-type :int64 :plain :none true)
          random-ints (repeatedly 100 helpers/rand-int64)
          input-rows (->> (repeatedly #(helpers/rand-member random-ints))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-float-encodings
  (testing "random floats"
    (let [ct (column-type :float :plain :none true)
          input-rows (->> (repeatedly helpers/rand-float) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random floats"
    (let [ct (column-type :float :plain :none true)
          random-floats (repeatedly 100 helpers/rand-float)
          input-rows (->> (repeatedly #(helpers/rand-member random-floats))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-double-encodings
  (testing "random doubles"
    (let [ct (column-type :double :plain :none true)
          input-rows (->> (repeatedly helpers/rand-double) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random doubles"
    (let [ct (column-type :double :plain :none true)
          random-doubles (repeatedly 100 helpers/rand-double)
          input-rows (->> (repeatedly #(helpers/rand-member random-doubles))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (read-column reader) (flatten input-rows)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(def simple-date-format (SimpleDateFormat. "dd/MM/yyyy"))

(defn- iterate-calendar-by-day [calendar]
  (lazy-seq (cons (.getTime calendar)
                  (iterate-calendar-by-day (doto calendar (.add Calendar/DATE 1))))))

(deftest find-best-byte-array-encodings
  (testing "random byte arrays"
    (let [ct (column-type :byte-array :plain :none true)
          input-rows (->> (repeatedly #(helpers/rand-byte-array)) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (every? true? (map helpers/array=
                             (->> reader read-column (map :value))
                             (->> input-rows flatten (map :value)))))
      (is (= :delta-length (find-best-encoding reader target-data-page-size)))))
  (testing "incrementing dates"
    (let [ct (column-type :byte-array :plain :none true)
          calendar (doto (Calendar/getInstance)
                     (.setTime (.parse simple-date-format "01/01/2014")))
          input-rows (->> (iterate-calendar-by-day calendar)
                          (map (comp str->utf8-bytes #(.format simple-date-format %)))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (every? true? (map helpers/array=
                             (->> reader read-column (map :value))
                             (->> input-rows flatten (map :value)))))
      (is (= :incremental (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random byte arrays"
    (let [ct (column-type :byte-array :plain :none true)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array))
          input-rows (->> (repeatedly #(helpers/rand-member rand-byte-arrays))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (every? true? (map helpers/array=
                             (->> reader read-column (map :value))
                             (->> input-rows flatten (map :value)))))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-fixed-length-byte-array-encodings
  (testing "random byte arrays"
    (let [ct (column-type :fixed-length-byte-array :plain :none true)
          input-rows (->> (repeatedly #(helpers/rand-byte-array 16)) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (every? true? (map helpers/array=
                             (->> reader read-column (map :value))
                             (->> input-rows flatten (map :value)))))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random byte arrays"
    (let [ct (column-type :fixed-length-byte-array :plain :none true)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          input-rows (->> (repeatedly #(helpers/rand-member rand-byte-arrays))
                          rand-top-level-required-rows
                          (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (every? true? (map helpers/array=
                             (->> reader read-column (map :value))
                             (->> input-rows flatten (map :value)))))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-compression-types
  (testing "plain encoded random int32s"
    (let [ct (column-type :int32 :plain :none true)
          input-rows (->> (repeatedly #(helpers/rand-int-bits 10)) rand-top-level-required-rows (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
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
          input-rows (->>  (repeatedly #(-> lorem-ipsum-shuffles helpers/rand-member str->utf8-bytes))
                           rand-top-level-required-rows
                           (take 5000))
          reader (write-column-and-get-reader ct [:foo] input-rows)]
      (is (= (assoc ct
               :encoding :dictionary
               :compression-type :deflate)
             (find-best-column-type reader target-data-page-size {:lz4 0.8 :deflate 0.5}))))))
