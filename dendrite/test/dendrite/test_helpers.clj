;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.test-helpers
  (:require [clojure.string :as string])
  (:import [dendrite.java LeveledValue MemoryOutputStream IOutputBuffer Types]
           [java.io Writer]
           [java.nio ByteBuffer]
           [java.util ArrayList Collections List ListIterator Random UUID])
  (:refer-clojure :exclude [rand-int]))

(set! *warn-on-reflection* true)

(def ^:private ^Random rng (Random.))

(def ^Types default-types (Types/create))

(defmethod print-method LeveledValue
  [^LeveledValue lv ^Writer w]
  (.write w (format "#<LeveledValue[r:%d, d:%d, v:%s]>"
                    (.repetitionLevel lv) (.definitionLevel lv) (.value lv))))

(defn rand-bool [] (zero? (clojure.core/rand-int 2)))

(defn rand-biased-bool [r] (< (rand) r))

(defn rand-sign [] (if (rand-bool) 1 -1))

(defn rand-byte [] (unchecked-byte (clojure.core/rand-int 256)))

(defn rand-int-bits [n] (-> (BigInteger. (int n) rng) unchecked-int))

(defn rand-long-bits [n] (-> (BigInteger. (int n) rng) unchecked-long))

(defn rand-int [] (rand-int-bits 32))

(defn rand-long [] (rand-long-bits 64))

(defn rand-float [] (-> (rand Float/MAX_VALUE) (* (rand-sign)) unchecked-float))

(defn rand-double [] (-> (rand Double/MAX_VALUE) (* (rand-sign)) unchecked-double))

(defn rand-biginteger [n] (BigInteger. (int n) rng))

(defn rand-bigint [n] (bigint (rand-biginteger n)))

(defn rand-bigdec [n] (BigDecimal. ^BigInteger (rand-biginteger n) (int (rand-int-bits 2))))

(defn rand-ratio [n] (/ (rand-bigint n) (rand-bigint n)))

(defn rand-biginteger-signed [n]
  (cond-> ^BigInteger (rand-biginteger n)
          (= (rand-sign) 1) .negate))

(defn rand-byte-array
  ([] (rand-byte-array (clojure.core/rand-int 24)))
  ([n] (byte-array (repeatedly n rand-byte))))

(defn rand-byte-buffer []
  (ByteBuffer/wrap (rand-byte-array 32) (clojure.core/rand-int 10) (clojure.core/rand-int 22)))

(defn byte-buffer->seq [^ByteBuffer bb]
  (seq (Types/toByteArray bb)))

(defn rand-uuid [] (UUID/randomUUID))

(defn rand-map [p f s]
  (lazy-seq
   (when (seq s)
     (if (< (rand) p)
       (cons (f (first s)) (rand-map p f (rest s)))
       (cons (first s) (rand-map p f (rest s)))))))

(defn rand-partition [max-n coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [n (clojure.core/rand-int (inc max-n))]
       (cons (take n s) (rand-partition max-n (drop n s)))))))

(defn leveled [{:keys [max-definition-level max-repetition-level] :as spec} coll]
  (lazy-seq
   (let [next-value (first coll)
         rand-repetition-level (clojure.core/rand-int (inc max-repetition-level))
         rand-definition-level
           (clojure.core/rand-int (if next-value (inc max-definition-level) max-definition-level))]
     (if (or (not next-value) (= rand-definition-level max-definition-level))
       (cons (LeveledValue. rand-repetition-level rand-definition-level (first coll))
             (leveled spec (rest coll)))
       (cons (LeveledValue. rand-repetition-level rand-definition-level nil)
             (leveled spec coll))))))

(defn partition-by-record [leveled-values]
  (lazy-seq
   (when-let [coll (seq leveled-values)]
     (let [fst (first coll)
           [in-record remaining] (split-with (fn [^LeveledValue lv] (pos? (.repetitionLevel lv))) (next coll))]
       (cons (cons fst in-record) (partition-by-record remaining))))))

(defn map-leveled [f enclosing-coll-max-definition-level leveled-values]
  (map (fn [^LeveledValue lv]
         (if (or (.value lv) (= (.definitionLevel lv) enclosing-coll-max-definition-level))
           (LeveledValue. (.repetitionLevel lv) (.definitionLevel lv) (f (.value lv)))
           lv))
       leveled-values))

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

(defn output-buffer->byte-buffer ^java.nio.ByteBuffer [^IOutputBuffer output-buffer]
  (let [mos (MemoryOutputStream.)]
    (.writeTo output-buffer mos)
    (.toByteBuffer mos)))

(def test-schema-str
  "#req
   {:docid #req #col [long delta deflate]
    :links {:backward (long)
            :forward #req [#col [long delta]]}
    :name [{:language [{:code #req string
                        :country string}]
           :url string}]
    :meta {#req string #req string}
    :keywords #{#req string}
    :internal/is-active #req boolean
    :ngrams [[#req string]]}")

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
          backward (take (clojure.core/rand-int 3)
                         (repeatedly #(when (pos? (clojure.core/rand-int 10)) (rand-long))))
          forward (vec (take (clojure.core/rand-int 3) (repeatedly rand-long)))
          names (take (clojure.core/rand-int 3) (repeatedly rand-name))
          links (cond-> {:forward forward}
                        (seq backward) (assoc :backward backward))
          ngrams (repeatedly (clojure.core/rand-int 3)
                             #(repeatedly (inc (clojure.core/rand-int 2)) rand-word))]
      (cond-> {:docid docid
               :internal/is-active (rand-bool)}
       (rand-bool) (assoc :links links)
       (seq names) (assoc :name names)
       (seq meta-map) (assoc :meta meta-map)
       (seq keywords) (assoc :keywords keywords)
       (seq ngrams) (assoc :ngrams ngrams)))))

(defmacro with-in-column-logical-types [& body]
  `(do (set! Types/USE_IN_COLUMN_LOGICAL_TYPES true)
       ~@body
       (set! Types/USE_IN_COLUMN_LOGICAL_TYPES false)))

(defn use-in-column-logical-types [f]
  (with-in-column-logical-types (f)))

(defn rand-test-records [] (map rand-test-record (range)))

(defmacro throw-cause [& body]
  `(try
     ~@body
     (catch Exception e#
       (throw (.getCause e#)))))

(defn flatten-1 [seqs]
  (letfn [(step [cs rs]
            (lazy-seq
             (when-let [s (seq cs)]
               (if (chunked-seq? s)
                 (let [cf (chunk-first s)
                       cr (chunk-rest s)]
                   (chunk-cons cf (if (seq cr) (step cr rs) (step (first rs) (rest rs)))))
                 (let [r (rest s)]
                   (cons (first s) (if (seq r) (step r rs) (step (first rs) (rest rs)))))))))]
    (step (first seqs) (rest seqs))))

(defn as-list-iterators [leveled-values]
  (->> (for [column-values leveled-values]
         (if (instance? List column-values)
           (.listIterator ^List column-values)
           (.listIterator (Collections/singletonList column-values))))
       (into-array ListIterator)))
