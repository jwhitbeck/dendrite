(ns dendrite.column-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [wrap-value]]
            [dendrite.column :refer :all]
            [dendrite.schema :refer [column-type]])
  (:import [dendrite.java ByteArrayWriter ByteArrayReader]))

(defn- rand-wrapped-value []
  (let [schema-depth 2
        definition-level (rand-int (inc schema-depth))
        repetition-level (rand-int (inc schema-depth))
        v (if (= definition-level schema-depth) (rand-int 1024) nil)]
    (wrap-value repetition-level definition-level v)))

(defn- rand-row [] (repeatedly (inc (rand-int 3)) rand-wrapped-value))

(def target-data-page-size 1000)

(deftest write-read-column
  (testing "Write/read a column works"
    (let [ct (column-type :int32 :plain :none false)
          schema-path [:foo :bar]
          input-rows (repeatedly 1000 rand-row)
          baw (ByteArrayWriter.)
          writer (doto (reduce write-row (column-writer target-data-page-size schema-path ct) input-rows)
                   (.writeTo baw))
          num-pages (num-pages writer)
          input-values (flatten input-rows)
          output-values (->> (column-reader (ByteArrayReader. (.buffer baw)) ct schema-path num-pages)
                             read-column)]
      (is (= num-pages 2))
      (is (= input-values output-values)))))
