(ns dendrite.column-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.column :refer :all]
            [dendrite.page :as page]
            [dendrite.schema :refer [column-type]]
            [dendrite.test-helpers :refer [get-byte-array-reader] :as helpers]))

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

(deftest write-read-column
  (let [ct (column-type :int32 :plain :deflate false)
        input-rows (->> (rand-rows #(helpers/rand-int-bits 10)) (take 5000))
        writer (doto (column-writer target-data-page-size test-schema-path ct)
                 (write-rows input-rows)
                 .finish)
        column-chunk-metadata (metadata writer)
        input-values (flatten input-rows)
        output-values (-> writer
                          get-byte-array-reader
                          (column-reader column-chunk-metadata test-schema-path ct)
                          read-column)]
    (testing "Write/read a colum works"
      (is (roughly= (:num-data-pages column-chunk-metadata) 13))
      (is (= input-values output-values)))
    (testing "Page size estimation converges"
      (->> (page/read-data-page-headers (get-byte-array-reader writer) (:num-data-pages column-chunk-metadata))
           rest                         ; the first page is always inaccurate
           butlast                      ; the last page can have any size
           (map data-page-header->partial-column-stats)
           (map #(+ (:header-bytes %) (:repetition-level-bytes %) (:definition-level-bytes %)
                    (:data-bytes %)))
           avg
           (roughly= target-data-page-size)))))
