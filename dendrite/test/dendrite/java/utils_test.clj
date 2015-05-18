;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.utils-test
  (:require [clojure.test :refer :all])
  (:import [dendrite.java Utils ChunkedPersistentList]))

(set! *warn-on-reflection* true)

(deftest chunked-drop
  (let [^ChunkedPersistentList cs (persistent! (reduce conj!
                                                       (transient ChunkedPersistentList/EMPTY)
                                                       (range 100)))]
    (is (= (drop 10 cs) (.drop cs 10)))
    (is (= (drop 32 cs) (.drop cs 32)))
    (is (= (seq (drop 101 cs)) (seq (.drop cs 101))))))

(deftest chunked-take
  (let [^ChunkedPersistentList cs (persistent! (reduce conj!
                                                       (transient ChunkedPersistentList/EMPTY)
                                                       (range 100)))]
    (is (= (take 10 cs) (.take cs 10)))
    (is (= (take 32 cs) (.take cs 32)))
    (is (= (seq (take 101 cs)) (seq (.take cs 101))))))

(deftest concatenation
  (is (= (Utils/concat (range 10) (range 10)) (concat (range 10) (range 10))))
  (is (= (Utils/concat (range 10) (list)) (range 10)))
  (is (nil? (Utils/concat (list) (list)))))

(deftest mapping
  (is (= (map (partial * 2) (range 10))
         (Utils/pmap (partial * 2) (range 10))
         (Utils/map (partial * 2) (range 10)))))
