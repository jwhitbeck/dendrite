(ns dendrite.java.test-helpers
  (:require [clojure.core :except [rand-int]]))

(defn array= [aa ab]
  (and (= (alength aa) (alength ab))
       (every? true? (map = aa ab))))

(defn rand-bool [] (zero? (clojure.core/rand-int 2)))

(defn rand-sign [] (if (rand-bool) 1 -1))

(defn rand-byte [] (unchecked-byte (rand-int 256)))

(defn rand-int [] (-> (rand Integer/MAX_VALUE) (* (rand-sign)) unchecked-int))

(defn rand-long [] (-> (rand Long/MAX_VALUE) (* (rand-sign)) unchecked-long))

(defn rand-float [] (-> (rand Float/MAX_VALUE) (* (rand-sign)) unchecked-float))

(defn rand-double [] (-> (rand Double/MAX_VALUE) (* (rand-sign)) unchecked-double))

(defn rand-int-bits [n]
  (if (= n 32)
    (rand-int)
    (let [mask (bit-not (bit-shift-left -1 n))]
      (bit-and (rand-int) mask))))

(defn rand-long-bits [n]
  (if (= n 64)
      (rand-long)
      (let [mask (bit-not (bit-shift-left -1 n))]
        (bit-and (rand-long) mask))))
