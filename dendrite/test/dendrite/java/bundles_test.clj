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
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Assemble$Fn Bundle Bundle$Factory Schema$Column Stripe$Fn]
           [java.util Arrays Iterator List]))

(set! *warn-on-reflection* true)

(defn- bundle-factory ^Bundle$Factory [num-columns]
  (Bundle$Factory. (into-array (repeat num-columns (Schema$Column. 0 0 0 0 0 0 0 0 0 nil)))))

(deftest bundle-striping
  (let [stripe (reify Stripe$Fn
                 (^boolean invoke [_ record ^objects array]
                   (Arrays/fill array record)
                   true))
        striped-record-bundle (.stripe (bundle-factory 4) stripe (range 10))]
    (is (= (seq striped-record-bundle)
           [(range 10) (range 10) (range 10) (range 10)]))))

(deftest bundle-reduction
  (let [test-bundle (.create (bundle-factory 2) 10 (into-array List [(range 10) (range 10)]))]
    (testing "reduce"
      (is (= (->> (range 10) (map (partial * 2)) (reduce +))
             (.reduce test-bundle
                      +
                      identity
                      (constantly 0)
                      (reify Assemble$Fn
                        (invoke [_ iterators]
                          (+ (.next ^Iterator (aget iterators 0))
                             (.next ^Iterator (aget iterators 1))))))))
      (is (= (->> (range 10) (map (partial * 2)) (reduce + 10))
             (.reduce test-bundle
                      +
                      identity
                      (constantly 10)
                      (reify Assemble$Fn
                        (invoke [_ iterators]
                          (+ (.next ^Iterator (aget iterators 0))
                             (.next ^Iterator (aget iterators 1)))))))))
    (testing "sampled reduce"
      (is (= (->> (range 10) (map (partial * 2)) (partition 2) (map second) (reduce +))
             (.reduceSampled test-bundle
                             +
                             identity
                             (constantly 0)
                             (reify Assemble$Fn
                               (invoke [_ iterators]
                                 (+ (.next ^Iterator (aget iterators 0))
                                    (.next ^Iterator (aget iterators 1)))))
                             odd?))))
    (testing "indexed reduce"
      (is (= (->> (range 10) (map (partial * 2)) (filter #(zero? (mod % 4))) (reduce +))
             (.reduceIndexed test-bundle
                             ((comp (map :obj) (filter #(zero? (mod % 4))))
                              +)
                             identity
                             (constantly 0)
                             (reify Assemble$Fn
                               (invoke [_ iterators]
                                 (+ (.next ^Iterator (aget iterators 0))
                                    (.next ^Iterator (aget iterators 1)))))
                             (fn [i o] {:obj o :index i})))))
    (testing "sampled and indexed reduce"
      (is (= (->> (range 10) (map (partial * 2)) (partition 2) (map second)
                  (filter #(zero? (mod % 3))) (reduce +))
             (.reduceSampledAndIndexed test-bundle
                                       ((comp (map :obj)
                                              (filter #(zero? (mod % 3))))
                                        +)
                                       identity
                                       (constantly 0)
                                       (reify Assemble$Fn
                                         (invoke [_ iterators]
                                           (+ (.next ^Iterator (aget iterators 0))
                                              (.next ^Iterator (aget iterators 1)))))
                                       odd?
                                       (fn [i o] {:obj o :index i})))))))

(deftest stripe-and-assemble
  (let [num-columns 10
        assemble (reify Assemble$Fn
                   (invoke [_ iterators] (vec (for [^Iterator i iterators]
                                                (.next i)))))
        stripe (reify Stripe$Fn
                 (^boolean invoke [_ record ^objects buffer]
                   (dotimes [i (count record)]
                     (aset buffer i (get record i)))
                   true))]
    (testing "single record"
      (let [array (object-array num-columns)
            record (vec (repeatedly num-columns helpers/rand-int))]
        (.invoke stripe record array)
        (is (= record (.invoke assemble (helpers/as-list-iterators array))))))))
