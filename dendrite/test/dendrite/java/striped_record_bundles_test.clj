;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.striped-record-bundles-test
  (:require [clojure.test :refer :all]
            [dendrite.utils :as utils]
            [dendrite.test-helpers :as helpers])
  (:import [clojure.lang ISeq]
           [dendrite.java StripedRecordBundle StripedRecordBundleSeq]))

(set! *warn-on-reflection* true)

(deftest bundle-assembly
  (testing "assembly"
    (let [column-values-seqs-array (into-array ISeq [(range 10) (range 10)])
          striped-record-bundle (StripedRecordBundle. column-values-seqs-array 10)]
      (is (= (map (partial * 2) (range 10))
             (.assemble striped-record-bundle (fn [^objects lva] (+ (aget lva 0) (aget lva 1)))))))
    (let [column-values-seqs-array (into-array ISeq [(range 10) (range 10)])
          striped-record-bundle (StripedRecordBundle. column-values-seqs-array 5)]
      (is (= (take 5 (map (partial * 2) (range 10)))
             (.assemble striped-record-bundle (fn [^objects lva] (+ (aget lva 0) (aget lva 1))))))))
  (testing "reduce"
    (let [column-values-seqs-array (into-array ISeq [(range 10) (range 10)])
          striped-record-bundle (StripedRecordBundle. column-values-seqs-array 10)]
      (is (= (->> (range 10) (map (partial * 2)) (reduce +))
             (.reduce striped-record-bundle + (fn [^objects lva] (+ (aget lva 0) (aget lva 1)))))))
    (let [column-values-seqs-array (into-array ISeq [(range 10) (range 10)])
          striped-record-bundle (StripedRecordBundle. column-values-seqs-array 10)]
      (is (= (->> (range 10) (map (partial * 2)) (reduce + 10))
             (.reduce striped-record-bundle + (fn [^objects lva] (+ (aget lva 0) (aget lva 1))) 10))))))

(deftest multiplexing
  (let [page (seq (vec (range 10)))
        column [page page]
        record-group [column column]
        record-groups [record-group record-group]]
    (is (= [[0 2 4]
            [6 8 10]
            [12 14 16]
            [18 0 2]
            [4 6 8]
            [10 12 14]
            [16 18]
            [0 2 4]
            [6 8 10]
            [12 14 16]
            [18 0 2]
            [4 6 8]
            [10 12 14]
            [16 18]]
         (map #(.assemble ^StripedRecordBundle % (fn [^objects lva] (+ (aget lva 0) (aget lva 1))))
              (StripedRecordBundleSeq/create 3 record-groups))))
    (is (empty? (StripedRecordBundleSeq/create 3 [])))))
