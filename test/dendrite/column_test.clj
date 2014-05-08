(ns dendrite.column-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.column :refer :all]
            [dendrite.page :as page]
            [dendrite.schema :refer [column-type]]
            [dendrite.test-helpers :refer [get-byte-array-reader] :as helpers])
  (:import [dendrite.java ByteArrayWriter]))

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

(defn write-column-and-get-reader [column-type input-rows]
  (let [writer (doto (column-writer target-data-page-size test-schema-path column-type)
                 (write-rows input-rows)
                 .finish)
        column-chunk-metadata (metadata writer)]
    (-> writer
        get-byte-array-reader
        (column-reader column-chunk-metadata test-schema-path column-type))))

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
