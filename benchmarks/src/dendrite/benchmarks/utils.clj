(ns dendrite.benchmarks.utils
  (:require [abracad.avro :as avro]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fressian]
            [cheshire.core :as json]
            [dendrite.core :as d]
            [clj-http.client :as http]
            [ring.util.codec :as codec]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang ArrayChunk IChunk]
           [net.jpountz.lz4 LZ4BlockInputStream LZ4BlockOutputStream]
           [java.io BufferedReader BufferedWriter FileInputStream FileOutputStream InputStreamReader
            OutputStreamWriter ObjectOutputStream ObjectInputStream BufferedOutputStream BufferedInputStream
            ByteArrayOutputStream ByteArrayInputStream InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [org.fressian FressianWriter FressianReader]))

(set! *warn-on-reflection* true)

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

(defn chunked-pmap
  ([f coll]
   (chunked-pmap f 256 coll))
  ([f chunk-size coll]
   (->> coll
        (partition-all chunk-size)
        (pmap (partial mapv f))
        flatten-1)))

(deftype Batch [^objects object-array ^long cnt])

(defn line-batch-seq [^BufferedReader rdr]
  (lazy-seq
   (let [^objects lines-array (make-array Object 256)]
     (loop [i (int 0)]
       (if (< i 256)
         (if-let [line (.readLine rdr)]
           (do (aset lines-array i line)
               (recur (inc i)))
           [(Batch. lines-array i)])
         (cons (Batch. lines-array i) (line-batch-seq rdr)))))))

(defn batch->chunk-fn [f]
  (fn ^clojure.lang.IChunk [^Batch batch]
    (let [n (.cnt batch)
          ^objects arr (.object-array batch)]
      (loop [i (int 0)]
        (if (< i n)
          (do (aset arr i (f (aget arr i)))
              (recur (inc i)))
          (ArrayChunk. arr 0 n))))))

(defn pmap-batched [f batches]
  (letfn [(step [chunks]
            (lazy-seq
             (when (seq chunks)
               (chunk-cons (first chunks) (step (rest chunks))))))]
    (step (pmap (batch->chunk-fn f) batches))))

(defn file-input-stream
  ^java.io.FileInputStream [filename]
  (-> filename io/as-file FileInputStream.))

(defn gzip-input-stream
  ^java.util.zip.GZIPInputStream [^FileInputStream fis]
  (GZIPInputStream. fis))

(defn lz4-input-stream
  ^net.jpountz.lz4.LZ4BlockInputStream [^FileInputStream fis]
  (LZ4BlockInputStream. fis))

(defn compressed-input-stream
  ^java.io.InputStream [^FileInputStream fis compression]
  (case compression
    :gzip (gzip-input-stream fis)
    :lz4 (lz4-input-stream fis)))

(defn buffered-reader
  ^java.io.BufferedReader [^InputStream is]
  (BufferedReader. (InputStreamReader. is) (* 256 1024)))

(defn buffered-input-stream
  ^java.io.BufferedInputStream [^InputStream is]
  (BufferedInputStream. is (* 256 1024)))

(defn object-input-stream
  ^java.io.ObjectInputStream [^BufferedInputStream bis]
  (ObjectInputStream. bis))

(defn file-output-stream
  ^java.io.FileOutputStream [filename]
  (-> filename io/as-file FileOutputStream.))

(defn gzip-output-stream
  ^java.util.zip.GZIPOutputStream [^FileOutputStream fos]
  (GZIPOutputStream. fos))

(defn lz4-output-stream
  ^net.jpountz.lz4.LZ4BlockOutputStream [^FileOutputStream fos]
  (LZ4BlockOutputStream. fos))

(defn buffered-writer
  ^java.io.BufferedWriter [^OutputStream os]
  (BufferedWriter. (OutputStreamWriter. os)))

(defn buffered-output-stream
  ^java.io.BufferedOutputStream [^OutputStream os]
  (BufferedOutputStream. os))

(defn object-output-stream
  ^java.io.ObjectOutputStream [^BufferedOutputStream bos]
  (ObjectOutputStream. bos))

(defn compressed-object-output-stream
  ^java.io.ObjectOutputStream [compression filename]
  (let [compressed-output-stream (case compression
                                   :gzip gzip-output-stream
                                   :lz4 lz4-output-stream)]
    (-> filename file-output-stream compressed-output-stream buffered-output-stream object-output-stream)))

(defn rand-samples [n mockaroo-columns mockaroo-api-key]
  (-> (format "http://www.mockaroo.com/api/generate.json?count=%d&key=%s&columns=%s"
              n mockaroo-api-key (-> mockaroo-columns json/generate-string codec/url-encode))
      http/get
      :body
      codec/url-decode
      (json/parse-string keyword)))

(defn generate-samples-file [filename n mockaroo-columns mockaroo-api-key]
  (let [batch-size 100
        samples (->> (repeatedly #(rand-samples batch-size mockaroo-columns mockaroo-api-key))
                     (apply concat)
                     (map #(assoc %2 :id %1) (range))
                     (take n))]
    (with-open [w (-> filename file-output-stream gzip-output-stream buffered-writer)]
      (doseq [sample samples]
        (when (zero? (mod (:id sample) 100))
          (println (str "Processing sample " (:id sample))))
        (.write w (str (json/generate-string sample) "\n"))))))

(defn fix-json-file [fix-fn input-json-filename output-json-filename]
  (with-open [r (-> input-json-filename file-input-stream gzip-input-stream buffered-reader)
              w (-> output-json-filename file-output-stream gzip-output-stream buffered-writer)]
    (let [records (->> r
                       line-seq
                       (chunked-pmap (comp json/generate-string fix-fn #(json/parse-string % true))))]
      (doseq [rec records]
        (.write w (str rec))
        (.newLine w )))))

(defn json-file->json-file [compression json-filename-input json-filename-output]
  (let [open-writer (case compression
                      :gzip (comp buffered-writer gzip-output-stream file-output-stream)
                      :lz4 (comp buffered-writer lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename-input file-input-stream gzip-input-stream buffered-reader)
                ^BufferedWriter w (open-writer json-filename-output)]
      (doseq [obj (line-seq r)]
        (.write w (str obj))
        (.newLine w)))))

(defn json-file->edn-file [compression json-filename edn-filename]
  (let [open-writer (case compression
                      :gzip (comp buffered-writer gzip-output-stream file-output-stream)
                      :lz4 (comp buffered-writer lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^BufferedWriter w (open-writer edn-filename)]
      (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
        (.write w (str obj))
        (.newLine w)))))

(defn json-file->dendrite-file [schema json-filename dendrite-filename]
  (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
              w (d/file-writer {:map-fn #(json/parse-string % true)} schema dendrite-filename)]
    (.writeAll w (line-seq r))))

(defn json-file->java-objects-file [compression json-filename output-filename]
  (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
              ^ObjectOutputStream w (compressed-object-output-stream compression output-filename)]
    (doseq [json-obj (->> r line-seq (map #(json/parse-string % true)))]
      (.writeObject w json-obj))))

(defn serialize-byte-buffer [obj]
  (let [baos (ByteArrayOutputStream. 1024)
        oos (ObjectOutputStream. baos)]
    (.writeObject oos obj)
    (ByteBuffer/wrap (.toByteArray baos))))

(defn deserialize-byte-buffer [^ByteBuffer byte-buffer]
  (let [bais (ByteArrayInputStream. (.array byte-buffer))
        ois (ObjectInputStream. bais)]
    (.readObject ois)))

(defn write-byte-buffer! [^ObjectOutputStream oos ^ByteBuffer byte-buffer]
  (.rewind byte-buffer)
  (let [num-bytes (int (- (.limit byte-buffer) (.position byte-buffer)))]
    (doto oos
      (.writeInt num-bytes)
      (.write (.array byte-buffer) (.position byte-buffer) num-bytes))))

(defn read-byte-buffer! ^ByteBuffer [^ObjectInputStream ois]
  (let [num-bytes (.readInt ois)
        buf (make-array Byte/TYPE num-bytes)]
    (.readFully ois buf 0 num-bytes)
    (ByteBuffer/wrap buf)))

(defn json-file->parallel-byte-buffer-file [compression serialize-fn json-filename output-filename]
  (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
              ^ObjectOutputStream w (compressed-object-output-stream compression output-filename)]
    (doseq [byte-buffer (chunked-pmap (comp serialize-fn #(json/parse-string % true)) (line-seq r))]
      (write-byte-buffer! w byte-buffer))))

(defn json-file->smile-file [compression json-filename output-filename]
  (json-file->parallel-byte-buffer-file compression #(ByteBuffer/wrap (json/generate-smile %))
                                        json-filename output-filename))

(defn json-file->parallel-java-objects-file [compression json-filename output-filename]
  (json-file->parallel-byte-buffer-file compression serialize-byte-buffer json-filename output-filename))

(defn byte-buffer-seq [n ^ObjectInputStream ois]
  (repeatedly n #(read-byte-buffer! ois)))

(defn byte-buffer-batched-seq [n ^ObjectInputStream ois]
  (lazy-seq
   (let [^objects bb-array (make-array Object 256)
         cnt (min 256 n)]
     (loop [i (int 0)]
       (if (< i cnt)
         (do (aset bb-array i (read-byte-buffer! ois))
             (recur (inc i)))
         (let [remaining (- n 256)]
           (if (pos? remaining)
             (cons (Batch. bb-array i) (byte-buffer-batched-seq (- n 256) ois))
             [(Batch. bb-array i)])))))))

(defn fressian-reader
  ^org.fressian.FressianReader [^BufferedReader br]
  (fressian/create-reader br))

(defn json-file->fressian-file [compression json-filename output-filename]
  (let [fressian-output-stream (case compression
                                 :gzip (comp fressian/create-writer buffered-output-stream
                                             gzip-output-stream file-output-stream)
                                 :lz4 (comp fressian/create-writer buffered-output-stream
                                            lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^FressianWriter w (fressian-output-stream output-filename)]
      (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
        (.writeObject w obj)))))

(defn json-file->parallel-fressian-file [compression json-filename output-filename]
  (json-file->parallel-byte-buffer-file compression fressian/write json-filename output-filename))

(defn json-file->nippy-file [compression json-filename output-filename]
  (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
              ^ObjectOutputStream w (compressed-object-output-stream compression output-filename)]
    (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
      (nippy/freeze-to-out! w obj))))

(defn json-file->parallel-nippy-file [compression json-filename output-filename]
  (json-file->parallel-byte-buffer-file compression (comp #(ByteBuffer/wrap %) nippy/freeze)
                                        json-filename output-filename))

(defn json-file->avro-file [compression schema json-filename avro-filename]
  (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
              w (avro/data-file-writer (case compression :deflate "deflate" :snappy "snappy")
                                       schema
                                       avro-filename)]
    (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
      (.append w obj))))

(defn json-file->parallel-avro-file [compression schema json-filename avro-filename]
  (json-file->parallel-byte-buffer-file compression
                                        (comp #(ByteBuffer/wrap %) #(avro/binary-encoded schema %))
                                        json-filename
                                        avro-filename))

(defn json-file->protobuf-file [compression proto-serialize json-filename protobuf-filename]
  (json-file->parallel-byte-buffer-file compression
                                        (comp #(ByteBuffer/wrap %) proto-serialize)
                                        json-filename
                                        protobuf-filename))

(defn read-plain-text-file [compression parse-fn filename]
  (with-open [r (-> filename file-input-stream (compressed-input-stream compression) buffered-reader)]
    (->> r line-seq (map parse-fn) last)))

(defn read-plain-text-file-parallel [compression parse-fn filename]
  (with-open [r (-> filename file-input-stream (compressed-input-stream compression) buffered-reader)]
    (->> r line-batch-seq (pmap-batched parse-fn) last)))

(defn read-json-file [compression keywordize? filename]
  (read-plain-text-file compression #(json/parse-string % keywordize?) filename))

(defn read-json-file-parallel [compression keywordize? filename]
  (read-plain-text-file-parallel compression #(json/parse-string % keywordize?) filename))

(defn read-edn-file [compression filename]
  (read-plain-text-file compression edn/read-string filename))

(defn read-edn-file-parallel [compression filename]
  (read-plain-text-file-parallel compression edn/read-string filename))

(defn read-dendrite-file [filename]
  (with-open [r (d/file-reader filename)]
    (last (d/read r))))

(defn read-byte-buffer-file [n compression parse-fn filename]
  (with-open [r (-> filename file-input-stream (compressed-input-stream compression)
                    buffered-input-stream object-input-stream)]
    (->> r (byte-buffer-batched-seq n) (pmap-batched parse-fn) last)))

(defn read-java-object-files [n compression filename]
  (with-open [^ObjectInputStream ois (-> filename file-input-stream (compressed-input-stream compression)
                                         buffered-input-stream object-input-stream)]
    (last (repeatedly n #(.readObject ois)))))

(defn read-java-object-files-parallel [n compression filename]
  (read-byte-buffer-file n compression deserialize-byte-buffer filename))

(defn read-smile-file [n compression keywordize? filename]
  (with-open [r (-> filename file-input-stream (compressed-input-stream compression)
                    buffered-input-stream object-input-stream)]
    (->> r
         (byte-buffer-seq n)
         (map #(json/parse-smile (.array ^ByteBuffer %) keywordize?))
         last)))

(defn read-smile-file-parallel [n compression keywordize? filename]
  (read-byte-buffer-file n compression #(json/parse-smile (.array ^ByteBuffer %) keywordize?) filename))

(defn read-fressian-file [n compression filename]
  (with-open [^FressianReader r (-> filename file-input-stream (compressed-input-stream compression)
                                    buffered-input-stream fressian/create-reader)]
    (last (repeatedly n #(.readObject r)))))

(defn read-fressian-file-parallel [n compression filename]
  (read-byte-buffer-file n compression fressian/read filename))

(defn read-nippy-file [n compression filename]
  (with-open [r (-> filename file-input-stream (compressed-input-stream compression)
                    buffered-input-stream object-input-stream)]
    (last (repeatedly n #(nippy/thaw-from-in! r)))))

(defn read-nippy-file-parallel [n compression filename]
  (read-byte-buffer-file n compression (comp nippy/thaw #(.array ^ByteBuffer %)) filename))

(defn read-avro-file [filename]
  (with-open [r (avro/data-file-reader filename)]
    (last r)))

(defn read-avro-file-parallel [n compression avro-schema filename]
  (read-byte-buffer-file n compression (comp #(avro/decode avro-schema %) #(.array ^ByteBuffer %)) filename))

(defn read-protobuf-file [n compression proto-deserialize filename]
  (with-open [r (-> filename file-input-stream (compressed-input-stream compression)
                    buffered-input-stream object-input-stream)]
    (->> r
         (byte-buffer-seq n)
         (map (comp proto-deserialize #(.array ^ByteBuffer %)))
         last)))

(defn read-protobuf-file-parallel [n compression proto-deserialize filename]
  (read-byte-buffer-file n compression (comp proto-deserialize #(.array ^ByteBuffer %)) filename))

(defmacro time-with-gc [& body]
  `(do (System/gc)
       (Thread/sleep 2000)
       (let [begin# (System/nanoTime)]
         ~@body
         (double (/ (- (System/nanoTime) begin#) 1000000)))))

(defn benchmark-read-fn [create-fn read-fn]
  (fn [benchmarked-filename]
    (create-fn benchmarked-filename)
    ;; ensure JIT kicks in
    (dotimes [_ 10] (read-fn benchmarked-filename))
    (let [result {:file-size (-> benchmarked-filename io/as-file .length)
                  :read-times (vec (repeatedly 20 #(time-with-gc (read-fn benchmarked-filename))))}]
      (-> benchmarked-filename io/as-file .delete)
      result)))

(defn json-benchmarks []
  [{:name "json-gz"
    :description "json + gzip"
    :family "json"
    :create-fn io/copy
    :bench-fn #(read-json-file :gzip false %)}
   {:name "json-kw-gz"
    :description "json + gzip with keyword keys"
    :family "json"
    :create-fn io/copy
    :bench-fn #(read-json-file :gzip true %)}
   {:name "json-lz4"
    :description "json + lz4"
    :family "json"
    :create-fn #(json-file->json-file :lz4 %1 %2)
    :bench-fn #(read-json-file :lz4 false %)}
   {:name "json-kw-lz4"
    :description "json + lz4"
    :family "json"
    :create-fn #(json-file->json-file :lz4 %1 %2)
    :bench-fn #(read-json-file :lz4 true %)}
   {:name "json-gz-par"
    :description "json + gzip with parallel deserialization"
    :family "json"
    :create-fn io/copy
    :bench-fn #(read-json-file-parallel :gzip false %)}
   {:name "json-kw-gz-par"
    :description "json + gzip with keyword keys and parallel deserialization"
    :family "json"
    :create-fn io/copy
    :bench-fn #(read-json-file-parallel :gzip true %)}
   {:name "json-lz4-par"
    :description "json + lz4 with parallel deserialization"
    :family "json"
    :create-fn #(json-file->json-file :lz4 %1 %2)
    :bench-fn #(read-json-file-parallel :lz4 false %)}
   {:name "json-kw-lz4-par"
    :description "json + lz4 with parallel deserialization"
    :family "json"
    :create-fn #(json-file->json-file :lz4 %1 %2)
    :bench-fn #(read-json-file-parallel :lz4 true %)}])

(defn smile-benchmarks [num-records]
  [{:name "smile-gz"
    :description "smile + gzip"
    :family "smile"
    :create-fn #(json-file->smile-file :gzip %1 %2)
    :bench-fn #(read-smile-file num-records :gzip false %)}
   {:name "smile-kw-gz"
    :description "smile + gzip with keyword keys"
    :family "smile"
    :create-fn #(json-file->smile-file :gzip %1 %2)
    :bench-fn #(read-smile-file num-records :gzip true %)}
   {:name "smile-lz4"
    :description "smile + lz4"
    :family "smile"
    :create-fn #(json-file->smile-file :lz4 %1 %2)
    :bench-fn #(read-smile-file num-records :lz4 false %)}
   {:name "smile-kw-lz4"
    :description "smile + lz4"
    :family "smile"
    :create-fn #(json-file->smile-file :lz4 %1 %2)
    :bench-fn #(read-smile-file num-records :lz4 true %)}
   {:name "smile-gz-par"
    :description "smile + gzip with parallel deserialization"
    :family "smile"
    :create-fn #(json-file->smile-file :gzip %1 %2)
    :bench-fn #(read-smile-file-parallel num-records :gzip false %)}
   {:name "smile-kw-gz-par"
    :description "smile + gzip with keyword keys and parallel deserialization"
    :family "smile"
    :create-fn #(json-file->smile-file :gzip %1 %2)
    :bench-fn #(read-smile-file-parallel num-records :gzip true %)}
   {:name "smile-lz4-par"
    :description "smile + lz4 with parallel deserialization"
    :family "smile"
    :create-fn #(json-file->smile-file :lz4 %1 %2)
    :bench-fn #(read-smile-file-parallel num-records :lz4 false %)}
   {:name "smile-kw-lz4-par"
    :description "smile + lz4 with parallel deserialization"
    :family "smile"
    :create-fn #(json-file->smile-file :lz4 %1 %2)
    :bench-fn #(read-smile-file-parallel num-records :lz4 true %)}])

(defn edn-benchmarks []
  [{:name "edn-gz"
    :description "edn + gzip"
    :family "edn"
    :create-fn #(json-file->edn-file :gzip %1 %2)
    :bench-fn #(read-edn-file :gzip %)}
   {:name "edn-lz4"
    :description "edn + lz4"
    :family "edn"
    :create-fn #(json-file->edn-file :lz4 %1 %2)
    :bench-fn #(read-edn-file :lz4 %)}
   {:name "edn-gz-par"
    :description "edn + gzip with parallel deserialization"
    :family "edn"
    :create-fn #(json-file->edn-file :gzip %1 %2)
    :bench-fn #(read-edn-file-parallel :gzip %)}
   {:name "edn-lz4-par"
    :description "edn + lz4 with parallel deserialization"
    :family "edn"
    :create-fn #(json-file->edn-file :lz4 %1 %2)
    :bench-fn #(read-edn-file-parallel :lz4 %)}])

(defn fressian-benchmarks [num-records]
  [{:name "fressian-gz"
    :description "fressian + gzip"
    :family "fressian"
    :create-fn #(json-file->fressian-file :gzip %1 %2)
    :bench-fn #(read-fressian-file num-records :gzip %)}
   {:name "fressian-lz4"
    :description "fressian + lz4"
    :family "fressian"
    :create-fn #(json-file->fressian-file :lz4 %1 %2)
    :bench-fn #(read-fressian-file num-records :lz4 %)}
   {:name "fressian-gz-par"
    :description "fressian + gzip with parallel deserialization"
    :family "fressian"
    :create-fn #(json-file->parallel-fressian-file :gzip %1 %2)
    :bench-fn #(read-fressian-file-parallel num-records :gzip %)}
   {:name "fressian-lz4-par"
    :description "fressian + lz4 with parallel deserialization"
    :family "fressian"
    :create-fn #(json-file->parallel-fressian-file :lz4 %1 %2)
    :bench-fn #(read-fressian-file-parallel num-records :lz4 %)}])

(defn nippy-benchmarks [num-records]
  [{:name "nippy-gz"
    :description "nippy + gzip"
    :family "nippy"
    :create-fn #(json-file->nippy-file :gzip %1 %2)
    :bench-fn #(read-nippy-file num-records :gzip %)}
   {:name "nippy-lz4"
    :description "nippy + lz4"
    :family "nippy"
    :create-fn #(json-file->nippy-file :lz4 %1 %2)
    :bench-fn #(read-nippy-file num-records :lz4 %)}
   {:name "nippy-gz-par"
    :description "nippy + gzip with parallel deserialization"
    :family "nippy"
    :create-fn #(json-file->parallel-nippy-file :gzip %1 %2)
    :bench-fn #(read-nippy-file-parallel num-records :gzip %)}
   {:name "nippy-lz4-par"
    :description "nippy + lz4 with parallel deserialization"
    :family "nippy"
    :create-fn #(json-file->parallel-nippy-file :lz4 %1 %2)
    :bench-fn #(read-nippy-file-parallel num-records :lz4 %)}])

(defn avro-benchmarks [num-records avro-schema]
  [{:name "avro-deflate"
    :description "avro + deflate"
    :family "avro"
    :create-fn #(json-file->avro-file :deflate avro-schema %1 %2)
    :bench-fn #(read-avro-file %)}
   {:name "avro-snappy"
    :description "avro + snappy"
    :family "avro"
    :create-fn #(json-file->avro-file :snappy avro-schema %1 %2)
    :bench-fn #(read-avro-file %)}
   {:name "avro-gz-par"
    :description "avro + gzip with parallel deserialization"
    :family "avro"
    :create-fn #(json-file->parallel-avro-file :gzip avro-schema %1 %2)
    :bench-fn #(read-avro-file-parallel num-records :gzip avro-schema %)}
   {:name "avro-lz4-par"
    :description "avro + lz4 with parallel deserialization"
    :family "avro"
    :create-fn #(json-file->parallel-avro-file :lz4 avro-schema %1 %2)
    :bench-fn #(read-avro-file-parallel num-records :lz4 avro-schema %)}])

(defn protobuf-benchmarks [num-records proto-serialize proto-deserialize]
  [{:name "protobuf-gz"
    :description "protobuf + gzip"
    :family "protobuf"
    :create-fn #(json-file->protobuf-file :gzip proto-serialize %1 %2)
    :bench-fn #(read-protobuf-file num-records :gzip proto-deserialize %)}
   {:name "protobuf-lz4"
    :description "protobuf + lz4"
    :family "protobuf"
    :create-fn #(json-file->protobuf-file :lz4 proto-serialize %1 %2)
    :bench-fn #(read-protobuf-file num-records :lz4 proto-serialize %)}
   {:name "protobuf-gz-par"
    :description "protobuf + gzip with parallel deserialization"
    :family "protobuf"
    :create-fn #(json-file->protobuf-file :gzip proto-serialize %1 %2)
    :bench-fn #(read-protobuf-file-parallel num-records :gzip proto-deserialize %)}
   {:name "protobuf-lz4-par"
    :description "protobuf + lz4 with parallel deserialization"
    :family "protobuf"
    :create-fn #(json-file->protobuf-file :lz4 proto-serialize %1 %2)
    :bench-fn #(read-protobuf-file-parallel num-records :lz4 proto-deserialize %)}])

(defn dendrite-benchmarks [schema]
  [{:name "dendrite-defaults"
    :description "dendrite with defauls parameters"
    :family "dendrite"
    :create-fn #(json-file->dendrite-file schema %1 %2)
    :bench-fn read-dendrite-file}])
