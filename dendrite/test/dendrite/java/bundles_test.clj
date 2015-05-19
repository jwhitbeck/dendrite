;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.bundles-test
  (:require [clojure.test :refer :all]
            [dendrite.utils :as utils]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Bundle ChunkedPersistentList Stripe$Fn]
           [java.util Arrays]))

(set! *warn-on-reflection* true)

(deftest bundle-striping
  (let [stripe (reify Stripe$Fn
                 (^boolean invoke [_ record ^objects array]
                   (Arrays/fill array record)
                   true))
        striped-record-bundle (Bundle/stripe (range 10) stripe 4)]
    (is (= (seq striped-record-bundle)
           [(range 10) (range 10) (range 10) (range 10)]))
    (is (every? chunked-seq? (seq striped-record-bundle)))))

(deftest bundle-assembly
  (let [test-bundle (->> [(helpers/as-chunked-list (range 10)) (helpers/as-chunked-list (range 10))]
                         (into-array ChunkedPersistentList)
                         Bundle.)]
    (testing "assembly"
      (is (= (map (partial * 2) (range 10))
             (.assemble test-bundle (fn [^objects lva] (+ (aget lva 0) (aget lva 1)))))))
    (testing "reduce"
      (is (= (->> (range 10) (map (partial * 2)) (reduce +))
             (.reduce test-bundle + (fn [^objects lva] (+ (aget lva 0) (aget lva 1))))))
      (is (= (->> (range 10) (map (partial * 2)) (reduce + 10))
             (.reduce test-bundle + (fn [^objects lva] (+ (aget lva 0) (aget lva 1))) 10))))))

(deftest stripe-and-assemble
  (let [num-columns 10
        assemble (fn [^objects a] (into [] a))
        stripe (reify Stripe$Fn
                 (^boolean invoke [_ record ^objects a]
                   (dotimes [i (count record)]
                     (aset a i (get record i)))
                   true))]
    (testing "single record"
      (let [array (object-array num-columns)
            record (vec (repeatedly num-columns helpers/rand-int))]
        (.invoke stripe record array)
        (is (= record (assemble array)))))
    (testing "bundled records"
      (let [records (repeatedly 100 #(vec (repeatedly num-columns helpers/rand-int)))
            striped-record-bundle (Bundle/stripe records stripe num-columns)]
        (is (= records (.assemble striped-record-bundle assemble)))))))