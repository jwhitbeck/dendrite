(ns dendrite.test-helpers
  (:require [clojure.string :as string]
            [dendrite.leveled-value :refer [->LeveledValue]])
  (:import [dendrite.java ByteArrayReader ByteArrayWriter Flushable]
           [java.util Random])
  (:refer-clojure :exclude [rand-int]))

(def ^:private rng (Random.))

(defn array= [aa ab]
  (and (= (alength aa) (alength ab))
       (every? true? (map = aa ab))))

(defn rand-bool [] (zero? (clojure.core/rand-int 2)))

(defn rand-biased-bool [r] (< (rand) r))

(defn rand-sign [] (if (rand-bool) 1 -1))

(defn rand-byte [] (unchecked-byte (clojure.core/rand-int 256)))

(defn rand-int-bits [n] (-> (BigInteger. n rng) unchecked-int))

(defn rand-long-bits [n] (-> (BigInteger. n rng) unchecked-long))

(defn rand-int [] (rand-int-bits 32))

(defn rand-long [] (rand-long-bits 64))

(defn rand-float [] (-> (rand Float/MAX_VALUE) (* (rand-sign)) unchecked-float))

(defn rand-double [] (-> (rand Double/MAX_VALUE) (* (rand-sign)) unchecked-double))

(defn rand-biginteger [n] (BigInteger. n rng))

(defn rand-bigint [n] (bigint (rand-biginteger n)))

(defn rand-bigdec [n] (BigDecimal. (rand-biginteger n) (rand-int-bits 2)))

(defn rand-biginteger-signed [n]
  (cond-> (rand-biginteger n)
          (= (rand-sign) 1) .negate))

(defn rand-byte-array
  ([] (rand-byte-array (clojure.core/rand-int 24)))
  ([n] (byte-array (repeatedly n rand-byte))))

(defn leveled [{:keys [max-definition-level max-repetition-level] :or {nested? true} :as spec} coll]
  (lazy-seq
   (let [next-value (first coll)
         rand-repetition-level (clojure.core/rand-int (inc max-repetition-level))
         rand-definition-level
           (clojure.core/rand-int (if next-value (inc max-definition-level) max-definition-level))]
     (if (or (not next-value) (= rand-definition-level max-definition-level))
       (cons (->LeveledValue rand-repetition-level rand-definition-level (first coll))
             (leveled spec (rest coll)))
       (cons (->LeveledValue rand-repetition-level rand-definition-level nil)
             (leveled spec coll))))))

(defn avg [coll] (/ (reduce + coll) (count coll)))

(defn abs [x] (if (pos? x) x (- x)))

(defn roughly
  ([a b] (roughly a b 0.1))
  ([a b r] (< (abs (- a b)) (* a r))))

(def lorem-ipsum
  "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et
  dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex
  ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat
  nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit
  anim id est laborum.")

(defn get-byte-array-reader [^Flushable byte-array-writable]
  (let [baw (ByteArrayWriter.)]
    (.write baw byte-array-writable)
    (-> baw .buffer ByteArrayReader.)))

(def test-schema-str
  "{:docid #req #col [long delta lz4]
    :links {:backward (long)
            :forward [#col [long delta]]}
    :name [{:language [{:code #req string
                        :country string}]
           :url string}]
    :meta {string string}
    :keywords #{string}
    :is-active #req boolean}")

(defn- rand-test-record [docid]
  (letfn [(rand-language []
            (rand-nth [{:code "us"} {:code "us" :country "USA"} {:code "gb" :country "Great Britain"}
                       {:code "fr" :country "France"}]))
          (rand-name []
            (let [language (take (clojure.core/rand-int 3) (repeatedly rand-language))
                  url (when (pos? (clojure.core/rand-int 3))
                        (str "http://" (->> (range 65 90) (map (comp str char)) rand-nth)))
                  n (cond-> {}
                            (seq language) (assoc :language language)
                            url (assoc :url url))]
              (when (seq n)
                n)))
          (rand-word []
            (->> (string/split lorem-ipsum #"\W") set vec rand-nth))]
    (let [meta-map (->> (repeatedly rand-word)
                        (partition 2)
                        (map vec)
                        (take (clojure.core/rand-int 10))
                        (into {}))
          keywords (->> (repeatedly rand-word)
                        (take (clojure.core/rand-int 4))
                        set)
          backward (take (clojure.core/rand-int 3) (repeatedly rand-long))
          forward (take (clojure.core/rand-int 3) (repeatedly rand-long))
          names (take (clojure.core/rand-int 3) (remove nil? (repeatedly rand-name)))
          links (cond-> {}
                        (seq backward) (assoc :backward backward)
                        (seq forward) (assoc :forward forward))]
      (cond-> {:docid docid
               :is-active (rand-bool)}
       (seq links) (assoc :links links)
       (seq names) (assoc :name names)
       (seq meta-map) (assoc :meta meta-map)
       (seq keywords) (assoc :keywords keywords)))))

(defn rand-test-records [] (map rand-test-record (range)))

(defmacro throw-cause [& body]
  `(try
     ~@body
     (catch Exception e#
       (throw (.getCause e#)))))