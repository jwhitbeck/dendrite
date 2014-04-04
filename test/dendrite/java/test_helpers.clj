(ns dendrite.java.test-helpers
  (:import [java.util Random]))

(def ^:private rng (Random.))

(defn array= [aa ab]
  (and (= (alength aa) (alength ab))
       (every? true? (map = aa ab))))

(defn rand-bool [] (zero? (rand-int 2)))

(defn rand-sign [] (if (rand-bool) 1 -1))

(defn rand-byte [] (unchecked-byte (rand-int 256)))

(defn rand-int-bits [n] (-> (BigInteger. n rng) unchecked-int))

(defn rand-long-bits [n] (-> (BigInteger. n rng) unchecked-long))

(defn rand-int32 [] (rand-int-bits 32))

(defn rand-int64 [] (rand-long-bits 64))

(defn rand-float [] (-> (rand Float/MAX_VALUE) (* (rand-sign)) unchecked-float))

(defn rand-double [] (-> (rand Double/MAX_VALUE) (* (rand-sign)) unchecked-double))

(defn rand-big-int [n] (BigInteger. n rng))

(defn rand-big-int-signed [n]
  (cond-> (rand-big-int n)
          (= (rand-sign) 1) .negate))

(defn rand-byte-array
  ([] (rand-byte-array (rand-int 24)))
  ([n] (byte-array (repeatedly n rand-byte))))


(def lorem-ipsum
  "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et
  dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex
  ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat
  nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit
  anim id est laborum.")
