(ns dendrite.utils
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import [clojure.lang ITransientCollection]
           [dendrite.java Singleton PersistentLinkedSeq]
           [java.io BufferedWriter]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.file StandardOpenOption OpenOption]
           [java.nio.channels FileChannel FileChannel$MapMode]
           [java.util.concurrent Callable LinkedBlockingQueue]))

(set! *warn-on-reflection* true)

(defmacro defalias
  [alias target]
  `(do
     (-> (def ~alias ~target)
         (alter-meta! merge (meta (var ~target))))
     (var ~alias)))

(defmacro defenum [s vs]
  (let [values-symb (symbol (str s "s"))
        values-set-symb (symbol (str values-symb "-set"))
        reverse-mapping-symb (symbol (str "reverse-" values-symb))
        reverse-mapping (reduce-kv (fn [m i v] (assoc m v i)) {} vs)]
    `(do (def ~values-symb ~vs)
         (def ^:private ~reverse-mapping-symb ~reverse-mapping)
         (defn ~(symbol (str "int->" s)) [i#] (get ~values-symb i#))
         (defn ~(symbol (str s "->int")) [k#] (get ~reverse-mapping-symb k#))
         (def ^:private ~values-set-symb ~(set vs))
         (defn ~(symbol (str "is-" s "?")) [k#] (contains? ~values-set-symb k#)))))

(defn format-ks [ks] (format "[%s]" (string/join " " ks)))

(defn warn [^String msg]
  (doto ^BufferedWriter *err*
    (.write msg)
    (.write "\n")))

(defn filter-indices [indices-set coll]
  (letfn [(fi [indices-set ^long next-index coll]
            (lazy-seq
             (when (seq coll)
               (if (indices-set next-index)
                 (cons (first coll) (fi indices-set (inc next-index) (rest coll)))
                 (fi indices-set (inc next-index) (rest coll))))))]
    (fi indices-set 0 coll)))

(defn file-channel
  ^FileChannel [f mode]
  (let [path (-> f io/as-file .toPath)
        opts (case mode
               :write (into-array OpenOption [StandardOpenOption/WRITE
                                              StandardOpenOption/CREATE
                                              StandardOpenOption/TRUNCATE_EXISTING])
               :read (into-array OpenOption [StandardOpenOption/READ]))]
    (FileChannel/open path opts)))

(defn map-file-channel
  ^ByteBuffer [^FileChannel file-channel]
  (.map file-channel FileChannel$MapMode/READ_ONLY 0 (.size file-channel)))

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

(defn multiplex [seqs]
  (letfn [(all-first [^objects seq-array]
            (let [^objects af (make-array Object (alength seq-array))]
              (loop [i (int 0)]
                (if (< i (alength seq-array))
                  (do (aset af i (first (aget seq-array i)))
                      (recur (unchecked-inc i)))
                  (seq af)))))
          (rest! [^objects seq-array]
            (loop [i (int 0)]
              (if (< i (alength seq-array))
                (do (aset seq-array i (rest (aget seq-array i)))
                    (recur (unchecked-inc i)))
                seq-array)))
          (step [^objects seq-array]
            (lazy-seq
             (when (seq (aget seq-array 0))
               (let [size 32
                     b (chunk-buffer size)]
                 (loop [i 0]
                   (when (and (< i size) (seq (aget seq-array 0)))
                     (chunk-append b (all-first seq-array))
                     (rest! seq-array)
                     (recur (inc i))))
                 (chunk-cons (chunk b) (step seq-array))))))]
    (step (into-array Object seqs))))

(definline single [x] `(Singleton. ~x))

(definline transient-linked-seq [] `(PersistentLinkedSeq/newEmptyTransient))

(defn transient? [x] (instance? ITransientCollection x))

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

(defn upmap
  [f coll]
  (let [n (+ 2 (.. Runtime getRuntime availableProcessors))
        queue (LinkedBlockingQueue.)
        rets (map #(future
                     (try (let [r (f %)]
                            (.put queue (if (nil? r) ::nil r)))
                          (catch Exception e
                            (.put queue e))))
                  coll)
        qget (fn []
               (let [r (.take queue)]
                 (if (instance? Exception r)
                   (throw r)
                   (when-not (= r ::nil) r))))
        step (fn step [[x & xs :as vs] fs]
               (lazy-seq
                (if-let [s (seq fs)]
                  (cons (qget) (step xs (rest s)))
                  (repeatedly (count vs) qget))))]
    (step rets (drop n rets))))

(defn pmap-1
  ([f coll]
     (letfn [(step [cur tail]
               (lazy-seq
                (if (seq tail)
                  (cons (deref cur) (step (future (f (first tail))) (rest tail)))
                  [(deref cur)])))]
       (step (future (f (first coll))) (rest coll))))
  ([f c1 c2]
     (pmap-1 (partial apply f) (multiplex [c1 c2]))))

(defn chunked-pmap
  ([f coll]
     (chunked-pmap f 255 coll))
  ([f chunk-size coll]
     (->> coll
          (partition-all chunk-size)
          (pmap (comp doall (partial map f)))
          flatten-1)))

(defn callable? [f] (instance? Callable f))

(defn boolean? [b] (or (true? b) (false? b)))
