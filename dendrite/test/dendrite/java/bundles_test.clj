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
  (:import [dendrite.java Assemble$Fn Bundle Bundle$Factory Mangle Schema$Column Stripe$Fn]
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

(deftest bundle-assembly
  (let [test-bundle (.create (bundle-factory 2) 10 (into-array List [(range 10) (range 10)]))]
    (testing "assembly"
      (is (= (map (partial * 2) (range 10))
             (chunk-cons (.assemble test-bundle (reify Assemble$Fn
                                                  (invoke [_ iterators]
                                                    (+ (.next ^Iterator (aget iterators 0))
                                                       (.next ^Iterator (aget iterators 1))))))
                         nil))))
    (testing "sampled assembly"
      (is (= (->> (map (partial * 2) (range 10)) (partition 2) (map second))
             (chunk-cons (.assembleSampled test-bundle
                                           (reify Assemble$Fn
                                             (invoke [_ iterators]
                                               (+ (.next ^Iterator (aget iterators 0))
                                                  (.next ^Iterator (aget iterators 1)))))
                                           odd?)
                         nil))))
    (testing "filtered assembly"
      (is (= (filter #(pos? (mod % 4)) (map (partial * 2) (range 10)))
             (chunk-cons (.assembleMangled test-bundle
                                           (reify Assemble$Fn
                                             (invoke [_ iterators]
                                               (+ (.next ^Iterator (aget iterators 0))
                                                  (.next ^Iterator (aget iterators 1)))))
                                           (Mangle/getFilterFn #(pos? (mod % 4))))
                         nil))))
    (testing "sampled and filtered assembly"
      (is (= (->> (map (partial * 2) (range 10)) (partition 2) (map second) (filter #(zero? (mod % 3))))
             (chunk-cons (.assembleSampledAndMangled test-bundle
                                                     (reify Assemble$Fn
                                                       (invoke [_ iterators]
                                                         (+ (.next ^Iterator (aget iterators 0))
                                                            (.next ^Iterator (aget iterators 1)))))
                                                     odd?
                                                     (Mangle/getFilterFn #(zero? (mod % 3))))
                         nil))))))

(deftest bundle-reduction
  (let [test-bundle (.create (bundle-factory 2) 10 (into-array List [(range 10) (range 10)]))]
    (testing "reduce"
      (is (= (->> (range 10) (map (partial * 2)) (reduce +))
             (.reduce test-bundle + (reify Assemble$Fn
                                      (invoke [_ iterators]
                                        (+ (.next ^Iterator (aget iterators 0))
                                           (.next ^Iterator (aget iterators 1))))) 0)))
      (is (= (->> (range 10) (map (partial * 2)) (reduce + 10))
             (.reduce test-bundle + (reify Assemble$Fn
                                      (invoke [_ iterators]
                                        (+ (.next ^Iterator (aget iterators 0))
                                           (.next ^Iterator (aget iterators 1))))) 10))))
    (testing "sampled reduce"
      (is (= (->> (range 10) (map (partial * 2)) (partition 2) (map second) (reduce +))
             (.reduceSampled test-bundle
                             +
                             (reify Assemble$Fn
                               (invoke [_ iterators]
                                 (+ (.next ^Iterator (aget iterators 0))
                                    (.next ^Iterator (aget iterators 1)))))
                             odd?
                             0))))
    (testing "filtered reduce"
      (is (= (->> (range 10) (map (partial * 2)) (filter #(zero? (mod % 4))) (reduce +))
             (.reduceMangled test-bundle
                             +
                             (reify Assemble$Fn
                               (invoke [_ iterators]
                                 (+ (.next ^Iterator (aget iterators 0))
                                    (.next ^Iterator (aget iterators 1)))))
                             (Mangle/getFilterFn #(zero? (mod % 4)))
                             0))))
    (testing "sampled and filtered reduce"
      (is (= (->> (range 10) (map (partial * 2)) (partition 2) (map second)
                  (filter #(zero? (mod % 3))) (reduce +))
             (.reduceSampledAndMangled test-bundle
                                       +
                                       (reify Assemble$Fn
                                         (invoke [_ iterators]
                                           (+ (.next ^Iterator (aget iterators 0))
                                              (.next ^Iterator (aget iterators 1)))))
                                       odd?
                                       (Mangle/getFilterFn #(zero? (mod % 3)))
                                       0))))))

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
