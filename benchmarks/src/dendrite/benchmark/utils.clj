(ns dendrite.benchmark.utils
  (:require [clojure.java.io :as io]
            [cheshire.core :as json]
            [dendrite.core :as d]
            [dendrite.utils :as du]
            [org.httpkit.client :as http]
            [ring.util.codec :as codec])
  (:import [java.io BufferedReader BufferedWriter FileInputStream FileOutputStream InputStreamReader
            OutputStreamWriter]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

(set! *warn-on-reflection* true)

(defn gzip-writer
  ^java.io.BufferedWriter [f]
  (-> f io/as-file FileOutputStream. GZIPOutputStream. OutputStreamWriter. BufferedWriter.))

(defn gzip-reader
  ^java.io.BufferedReader [f]
  (-> f io/as-file FileInputStream. GZIPInputStream. InputStreamReader. BufferedReader.))

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
    (with-open [f (gzip-writer filename)]
      (doseq [sample samples]
        (when (zero? (mod (:id sample) 100))
          (println (str "Processing sample " (:id sample))))
        (.write f (str (json/generate-string sample) "\n"))))))

(defn fix-json-file [fix-fn input-json-filename output-json-filename]
  (with-open [r (gzip-reader input-json-filename)
              w (gzip-writer output-json-filename)]
    (let [records (->> r
                       line-seq
                       (du/chunked-pmap (comp json/generate-string fix-fn #(json/parse-string % true))))]
      (doseq [rec records]
        (.write w (str rec))
        (.newLine w )))))

(defn json-file->dendrite-file [schema-resource json-filename dendrite-filename]
  (let [schema (-> schema-resource io/resource slurp d/read-schema-string)]
    (with-open [r (gzip-reader json-filename)
                w (d/file-writer schema dendrite-filename)]
      (->> r
           line-seq
           (map #(json/parse-string % true))
           (reduce d/write! w)))))
