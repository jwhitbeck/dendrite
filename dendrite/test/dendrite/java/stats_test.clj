;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.stats-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Stats]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(deftest data-page-stats
  (is (= {:num-values 1
          :length 2
          :header-length 3
          :repetition-levels-length 4
          :definition-levels-length 5
          :dictionary-length 0
          :dictionary-header-length 0
          :data-length 6}
         (Stats/dataPageStats 1 2 3 4 5 6))))

(deftest dictionary-page-stats
  (is (= {:num-values 1
          :length 2
          :header-length 0
          :repetition-levels-length 0
          :definition-levels-length 0
          :data-length 0
          :dictionary-header-length 3
          :dictionary-length 4}
         (Stats/dictionaryPageStats 1 2 3 4))))

(defn rand-data-page-stats []
  (Stats/dataPageStats (rand-int 10) (rand-int 10) (rand-int 10) (rand-int 10) (rand-int 10) (rand-int 10)))

(defn rand-dictionary-page-stats []
  (Stats/dictionaryPageStats (rand-int 10) (rand-int 10) (inc (rand-int 10)) (inc (rand-int 10))))

(defn rand-pages-stats []
  (cons (rand-dictionary-page-stats) (repeatedly (+ (rand-int 5) 3) rand-data-page-stats)))

(deftest column-chunk-stats
  (testing "empty pages"
    (is (= {:definition-levels-length 0
            :data-length 0
            :dictionary-length 0
            :num-values 0
            :length 0
            :num-dictionary-values 0
            :dictionary-header-length 0
            :repetition-levels-length 0
            :header-length 0
            :num-pages 0}
           (Stats/columnChunkStats nil))))
  (testing "random pages"
    (let [pages-stats (rand-pages-stats)]
      (is (= {:num-pages (count pages-stats)
              :num-values (->> pages-stats next (map :num-values) (reduce +))
              :length (->> pages-stats (map :length) (reduce +))
              :header-length (->> pages-stats next (map :header-length) (reduce +))
              :repetition-levels-length (->> pages-stats next (map :repetition-levels-length) (reduce +))
              :definition-levels-length (->> pages-stats next (map :definition-levels-length) (reduce +))
              :data-length (->> pages-stats next (map :data-length) (reduce +))
              :num-dictionary-values (-> pages-stats first :num-values)
              :dictionary-header-length (-> pages-stats first :dictionary-header-length)
              :dictionary-length (-> pages-stats first :dictionary-length)}
             (Stats/columnChunkStats pages-stats))))))

(defn rand-column-chunk-stats []
  (Stats/columnChunkStats (rand-pages-stats)))

(deftest column-stats
  (testing "emtpy column-chunks"
    (is (= {:path [:foo :bar],
            :max-definition-level 2
            :encoding 'plain
            :definition-levels-length 0
            :type 'int
            :data-length 0
            :compression 'none
            :num-column-chunks 0
            :dictionary-length 0
            :num-values 0
            :num-dictionary-values 0
            :length 0
            :dictionary-header-length 0
            :max-repetition-level 1
            :repetition-levels-length 0
            :header-length 0
            :num-pages 0}
           (Stats/columnStats 'int 'plain 'none 1 2 [:foo :bar] nil))))
  (testing "random column-chunks"
    (let [column-chunks (repeatedly (rand-int 10) rand-column-chunk-stats)]
      (is (= {:type 'int
              :encoding 'plain
              :compression 'none
              :max-repetition-level 1
              :max-definition-level 2
              :path [:foo :bar]
              :length (->> column-chunks (map :length) (reduce +))
              :num-values (->> column-chunks (map :num-values) (reduce +))
              :num-dictionary-values (->> column-chunks (map :num-dictionary-values) (reduce +))
              :num-column-chunks (count column-chunks)
              :num-pages (->> column-chunks (map :num-pages) (reduce +))
              :header-length (->> column-chunks (map :header-length) (reduce +))
              :repetition-levels-length (->> column-chunks (map :repetition-levels-length) (reduce +))
              :definition-levels-length (->> column-chunks (map :definition-levels-length) (reduce +))
              :data-length (->> column-chunks (map :data-length) (reduce +))
              :dictionary-header-length (->> column-chunks (map :dictionary-header-length) (reduce +))
              :dictionary-length (->> column-chunks (map :dictionary-length) (reduce +))}
             (Stats/columnStats 'int 'plain 'none 1 2 [:foo :bar] column-chunks))))))

(deftest record-group-stats
  (testing "empty record groups"
    (is (= {:num-records 0
            :num-column-chunks 0
            :definition-levels-length 0
            :data-length 0
            :dictionary-length 0
            :length 0
            :dictionary-header-length 0
            :repetition-levels-length 0
            :header-length 0}
           (Stats/recordGroupStats 0 nil))))
  (testing "reandom column-chunks"
    (let [column-chunks (repeatedly (+ (rand-int 10) 2) rand-column-chunk-stats)]
      (is (= {:length (->> column-chunks (map :length) (reduce +))
              :num-records 100
              :num-column-chunks (count column-chunks)
              :header-length (->> column-chunks (map :header-length) (reduce +))
              :repetition-levels-length (->> column-chunks (map :repetition-levels-length) (reduce +))
              :definition-levels-length (->> column-chunks (map :definition-levels-length) (reduce +))
              :data-length (->> column-chunks (map :data-length) (reduce +))
              :dictionary-header-length (->> column-chunks (map :dictionary-header-length) (reduce +))
              :dictionary-length (->> column-chunks (map :dictionary-length) (reduce +))}
             (Stats/recordGroupStats 100 column-chunks))))))

(defn rand-record-group-stats []
  (Stats/recordGroupStats (rand-int 100) (repeatedly (+ (rand-int 10) 2) rand-column-chunk-stats)))

(deftest global-stats
  (testing "empty file"
    (is (= {:num-records 0
            :num-columns 10
            :definition-levels-length 0
            :data-length 0
            :num-record-groups 0
            :dictionary-length 0
            :length 10
            :dictionary-header-length 0
            :repetition-levels-length 0
            :metadata-length 10
            :header-length 0}
           (Stats/globalStats 10 10 nil))))
  (testing "random record-groups"
    (let [record-groups-stats (repeatedly (+ (rand-int 2) 2) rand-record-group-stats)]
      (is (= {:length 1024
              :metadata-length (- 1024 (->> record-groups-stats (map :length) (reduce +)))
              :num-records (->> record-groups-stats (map :num-records) (reduce +))
              :num-columns 10
              :num-record-groups (count record-groups-stats)
              :header-length (->> record-groups-stats (map :header-length) (reduce +))
              :repetition-levels-length (->> record-groups-stats (map :repetition-levels-length) (reduce +))
              :definition-levels-length (->> record-groups-stats (map :definition-levels-length) (reduce +))
              :data-length (->> record-groups-stats (map :data-length) (reduce +))
              :dictionary-header-length (->> record-groups-stats (map :dictionary-header-length) (reduce +))
              :dictionary-length (->> record-groups-stats (map :dictionary-length) (reduce +))}
             (Stats/globalStats 1024 10 record-groups-stats))))))
