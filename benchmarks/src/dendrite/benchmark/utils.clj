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
    (doseq [byte-buffer (->> r line-seq (map (comp serialize-fn #(json/parse-string % true))))]
      (write-byte-buffer! w byte-buffer))))

(defn json-file->parallel-java-objects-file [compression json-filename output-filename]
  (json-file->parallel-byte-buffer-file compression serialize-byte-buffer json-filename output-filename))

(defn byte-byffer-seq [n ^ObjectInputStream ois]
  (repeatedly n #(read-byte-buffer! ois)))

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
              w (avro/data-file-writer (if (= compression :gzip) "deflate" "snappy") schema avro-filename)]
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
    (->> r line-seq (du/chunked-pmap parse-fn) last)))

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
    (->> r (byte-byffer-seq n) (du/chunked-pmap parse-fn) last)))

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
         (byte-byffer-seq n)
         (map (comp proto-deserialize #(.array ^ByteBuffer %)))
         last)))

(defn read-protobuf-file-parallel [n compression proto-deserialize filename]
  (read-byte-buffer-file n compression (comp proto-deserialize #(.array ^ByteBuffer %)) filename))
