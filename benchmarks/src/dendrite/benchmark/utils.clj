(ns dendrite.benchmark.utils
  (:require [abracad.avro :as avro]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.data.fressian :as fressian]
            [cheshire.core :as json]
            [dendrite.core :as d]
            [dendrite.utils :as du]
            [org.httpkit.client :as http]
            [ring.util.codec :as codec]
            [taoensso.nippy :as nippy])
  (:import [net.jpountz.lz4 LZ4BlockInputStream LZ4BlockOutputStream]
           [java.io BufferedReader BufferedWriter FileInputStream FileOutputStream InputStreamReader
            OutputStreamWriter ObjectOutputStream ObjectInputStream BufferedOutputStream BufferedInputStream
            ByteArrayOutputStream ByteArrayInputStream InputStream OutputStream]
           [java.nio ByteBuffer]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [org.fressian FressianWriter FressianReader]))

(set! *warn-on-reflection* true)

(defn file-input-stream
  ^java.io.FileInputStream [filename]
  (-> filename io/as-file FileInputStream.))

(defn gzip-input-stream
  ^java.util.zip.GZIPInputStream [^FileInputStream fis]
  (GZIPInputStream. fis))

(defn lz4-input-stream
  ^net.jpountz.lz4.LZ4BlockInputStream [^FileInputStream fis]
  (LZ4BlockInputStream. fis))

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

(defn rand-samples [n mockaroo-columns mockaroo-api-key]
  (-> (format "http://www.mockaroo.com/api/generate.json?count=%d&key=%s&columns=%s"
              n mockaroo-api-key (-> mockaroo-columns json/generate-string codec/url-encode))
      http/get
      deref
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
                       (du/chunked-pmap (comp json/generate-string fix-fn #(json/parse-string % true))))]
      (doseq [rec records]
        (.write w (str rec))
        (.newLine w )))))

(defn json-file->edn-file [compression json-filename edn-filename]
  (let [open-writer (case compression
                      :gzip (comp buffered-writer gzip-output-stream file-output-stream)
                      :lz4 (comp buffered-writer lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^BufferedWriter w (open-writer edn-filename)]
      (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
        (.write w (str obj))
        (.newLine w)))))

(defn json-file->dendrite-file [schema-resource json-filename dendrite-filename]
  (let [schema (-> schema-resource io/resource slurp d/read-schema-string)]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                w (d/file-writer schema dendrite-filename)]
      (->> r
           line-seq
           (map #(json/parse-string % true))
           (reduce d/write! w)))))

(defn json-file->java-objects-file [compression json-filename output-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream output-filename)]
      (doseq [json-obj (->> r line-seq (map #(json/parse-string % true)))]
        (.writeObject w json-obj)))))

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

(defn json-file->parallel-java-objects-file [compression json-filename output-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream output-filename)]
      (doseq [byte-buffer (->> r line-seq (map (comp serialize-byte-buffer #(json/parse-string % true))))]
        (write-byte-buffer! w byte-buffer)))))

(defn byte-byffer-seq [n ^ObjectInputStream ois]
  (repeatedly n #(read-byte-buffer! ois)))

(defn fressian-reader
  ^org.fressian.FressianReader [^BufferedReader br]
  (fressian/create-reader br))

(defn json-file->fressian-file [compression json-filename output-filename]
  (let [open-stream (case compression
                      :gzip (comp fressian/create-writer buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp fressian/create-writer buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^FressianWriter w (open-stream output-filename)]
      (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
        (.writeObject w obj)))))

(defn json-file->parallel-fressian-file [compression json-filename output-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream output-filename)]
      (doseq [byte-buffer (->> r line-seq (map (comp fressian/write #(json/parse-string % true))))]
        (write-byte-buffer! w byte-buffer)))))

(defn json-file->nippy-file [compression json-filename output-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream output-filename)]
      (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
        (nippy/freeze-to-out! w obj)))))

(defn json-file->parallel-nippy-file [compression json-filename output-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream output-filename)]
      (doseq [byte-buffer (->> (line-seq r)
                               (map (comp #(ByteBuffer/wrap %) nippy/freeze #(json/parse-string % true))))]
        (write-byte-buffer! w byte-buffer)))))

(defn json-file->avro-file [compression schema json-filename avro-filename]
  (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
              w (avro/data-file-writer (if (= compression :gzip) "deflate" "snappy") schema avro-filename)]
    (doseq [obj (->> r line-seq (map #(json/parse-string % true)))]
      (.append w obj))))

(defn json-file->parallel-avro-file [compression schema json-filename avro-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream avro-filename)]
      (doseq [byte-buffer (->> (line-seq r)
                               (map (comp #(ByteBuffer/wrap %)
                                          #(avro/binary-encoded schema %)
                                          #(json/parse-string % true))))]
        (write-byte-buffer! w byte-buffer)))))

(defn json-file->protobuf-file [compression proto-serialize json-filename avro-filename]
  (let [open-stream (case compression
                      :gzip (comp object-output-stream buffered-output-stream
                                  gzip-output-stream file-output-stream)
                      :lz4 (comp object-output-stream buffered-output-stream
                                 lz4-output-stream file-output-stream))]
    (with-open [r (-> json-filename file-input-stream gzip-input-stream buffered-reader)
                ^ObjectOutputStream w (open-stream avro-filename)]
      (doseq [byte-buffer (->> (line-seq r)
                               (map (comp #(ByteBuffer/wrap %)
                                          proto-serialize
                                          #(json/parse-string % true))))]
        (write-byte-buffer! w byte-buffer)))))

