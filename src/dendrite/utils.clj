(ns dendrite.utils
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.nio.file StandardOpenOption OpenOption]
           [java.nio.channels FileChannel FileChannel$MapMode]))

(set! *warn-on-reflection* true)

(defmacro defalias
  [alias target]
  `(do
     (-> (def ~alias ~target)
         (alter-meta! merge (meta (var ~target))))
     (var ~alias)))

(defn format-ks [ks] (format "[%s]" (string/join " " ks)))

(defn- filter-indices* [indices-set ^long next-index coll]
  (lazy-seq (when (seq coll)
              (if (indices-set next-index)
                (cons (first coll) (filter-indices* indices-set (inc next-index) (rest coll)))
                (filter-indices* indices-set (inc next-index) (rest coll))))))

(defn filter-indices [indices-set coll]
  (filter-indices* indices-set 0 coll))

(defn file-channel
  ^FileChannel [filename mode]
  (let [path (-> filename io/as-file .toPath)
        opts (case mode
               :write (into-array OpenOption [StandardOpenOption/WRITE
                                              StandardOpenOption/CREATE
                                              StandardOpenOption/TRUNCATE_EXISTING])
               :read (into-array OpenOption [StandardOpenOption/READ]))]
    (FileChannel/open path opts)))

(defn map-bytes
  ^ByteBuffer [^FileChannel file-channel offset length]
  (.map file-channel FileChannel$MapMode/READ_ONLY (long offset) (long length)))

(defn sub-byte-buffer
  ^ByteBuffer [^ByteBuffer bb offset length]
  (doto (.slice bb)
    (.position offset)
    (.limit (+ offset length))))

(defn int->byte-buffer
  ^ByteBuffer [i]
  (doto (ByteBuffer/wrap (byte-array 4))
    (.order ByteOrder/LITTLE_ENDIAN)
    (.putInt i)
    .rewind))

(defn byte-buffer->int [^ByteBuffer bb]
  (.getInt (doto bb (.order ByteOrder/LITTLE_ENDIAN))))

(defn str->byte-buffer
  ^ByteBuffer [^String s]
  (ByteBuffer/wrap (.getBytes s)))

(defn byte-buffer->str [^ByteBuffer bb]
  (let [length (- (.limit bb) (.position bb))
        chars (byte-array length)]
    (.get bb chars)
    (String. chars)))

(defn pmap-next
  ([f coll]
     (letfn [(step [[x & xs]]
               (lazy-seq
                (if-let [s (seq xs)]
                  (cons (deref x) (step (cons (future (f (first xs))) (rest xs))))
                  (list (deref x)))))]
       (if-not (seq coll)
         (list)
         (step (cons (future (f (first coll))) (rest coll))))))
  ([f coll & colls]
     (pmap-next (partial apply f) (apply map vector coll colls))))
