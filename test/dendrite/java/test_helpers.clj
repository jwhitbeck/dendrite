(ns dendrite.java.test-helpers
  (:require [clojure.core :except [rand-int]])
  (:import [java.util Random]))

(def ^:private rng (Random.))

(defn array= [aa ab]
  (and (= (alength aa) (alength ab))
       (every? true? (map = aa ab))))

(defn rand-bool [] (zero? (clojure.core/rand-int 2)))

(defn rand-sign [] (if (rand-bool) 1 -1))

(defn rand-byte [] (unchecked-byte (rand-int 256)))

(defn rand-int-bits [n] (-> (BigInteger. n rng) unchecked-int))

(defn rand-long-bits [n] (-> (BigInteger. n rng) unchecked-long))

(defn rand-int [] (rand-int-bits 32))

(defn rand-long [] (rand-long-bits 64))

(defn rand-float [] (-> (rand Float/MAX_VALUE) (* (rand-sign)) unchecked-float))

(defn rand-double [] (-> (rand Double/MAX_VALUE) (* (rand-sign)) unchecked-double))

(defn rand-big-int [n] (BigInteger. n rng))
