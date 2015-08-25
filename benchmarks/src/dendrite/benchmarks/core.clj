(ns dendrite.benchmarks.core
  (:require [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [dendrite.core :as d]
            [dendrite.benchmarks.media-content :as media-content]
            [dendrite.benchmarks.tpc-h :as tpc-h]
            [dendrite.benchmarks.user-events :as user-events])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn- delete-recursively! [d]
  (doseq [^java.io.File file (reverse (file-seq (io/as-file d)))]
    (println (str "Deleting " (.getPath file)))
    (.delete file)))

(defn- sync-file! [base-file-url local-file]
  (when-not (.exists (io/as-file local-file))
    (println "Downloading" base-file-url "to" (.getPath (io/as-file local-file)))
    (io/copy (:body (http/get base-file-url {:as :stream})) (io/as-file local-file))))

(defmacro time-with-gc [& body]
  `(do (System/gc)
       (Thread/sleep 2000)
       (let [begin# (System/nanoTime)]
         ~@body
         (double (/ (- (System/nanoTime) begin#) 1000000)))))

(defn- create-benchmark-fn [create-fn uncompress-fn read-fn]
  (fn [base-filename benchmarked-filename]
    ;; ensure JIT kicks in for writes
    (create-fn base-filename benchmarked-filename)
    (let [write-time (time-with-gc (create-fn base-filename benchmarked-filename))]
      ;; ensure JIT kicks in for reads
      (dotimes [_ 5] (read-fn benchmarked-filename))
      (let [result (cond-> {:file-size (-> benchmarked-filename io/as-file .length)
                            :write-time (double write-time)
                            :read-times (vec (repeatedly 10 #(time-with-gc (read-fn benchmarked-filename))))}
                     uncompress-fn
                     (assoc :uncompress-times (vec (repeatedly 10 #(time-with-gc
                                                                    (uncompress-fn benchmarked-filename))))))]
        (-> benchmarked-filename io/as-file .delete)
        result))))

(defn- run-full-schema-benchmark! [output-dir base-file-url benchmark]
  (println "Running benchmark:" (:name benchmark))
  (let [base-file (io/file output-dir "base-file.dat")]
    (sync-file! base-file-url base-file)
    (let [tmp-file (io/file output-dir "tmp-file.dat")
          {:keys [create-fn bench-fn uncompress-fn]} benchmark
          benchmark-fn (create-benchmark-fn create-fn uncompress-fn bench-fn)]
      (merge (select-keys benchmark [:name :description :family])
             (benchmark-fn (.getPath base-file) (.getPath tmp-file))))))

(defn- write-full-schema-results-to-csv! [results output-file]
  (with-open [w (io/writer output-file)]
    (.write w (str "name,description,family,file_size,avg_read_time,write_time,avg_uncompress_time\n"))
    (doseq [{:keys [description family avg-read-time write-time avg-uncompress-time] :as res} results]
      (.write w (format "%s,'%s',%s,%d,%.2f,%.2f,%.2f\n"
                        (:name res) description family (:file-size res) avg-read-time write-time
                        (or avg-uncompress-time 0.0))))))

(defn- run-full-schema-benchmarks! [output-dir benchmark-name base-file-url benchmarks]
  (println "Running full schema benchmarks for:" benchmark-name)
  (let [dir (io/file output-dir benchmark-name "full_schema")
        results-file (io/file dir "results.edn")]
    (.mkdirs dir)
    (let [results (atom (when (.exists results-file)
                          (edn/read-string (slurp results-file))))
          completed-benchmarks (into #{} (map :name @results))
          remaining-benchmarks (remove (comp completed-benchmarks :name) benchmarks)]
      (doseq [benchmark remaining-benchmarks]
        (let [res (run-full-schema-benchmark! dir base-file-url benchmark)]
          (swap! results conj res)
          (spit results-file @results))))
    (let [final-results (->> (slurp results-file)
                             edn/read-string
                             (map (fn [{:keys [read-times uncompress-times] :as res}]
                                    (assoc res
                                           :avg-read-time (double (/ (reduce + read-times)
                                                                     (count read-times)))
                                           :avg-uncompress-time (when (seq uncompress-times)
                                                                  (double (/ (reduce + uncompress-times)
                                                                             (count uncompress-times))))))))
          csv-results-file (io/file dir "results.csv")]
      (when-not (.exists csv-results-file)
        (write-full-schema-results-to-csv! final-results csv-results-file)))))

(defn- run-sub-schema-benchmarks! [output-dir benchmark-name base-file-url sub-schema-benchmarks]
  (println "Running sub-schema benchmarks for:" benchmark-name)
  (let [dir (io/file output-dir benchmark-name "sub_schema")
        results-file (io/file dir "results.edn")
        csv-results-file (io/file dir "results.csv")
        {:keys [create-fn random-queries-fn]} sub-schema-benchmarks]
    (.mkdirs dir)
    (when-not (.exists csv-results-file)
      (let [base-file (io/file dir "base-file.dat")
            tmp-file (io/file dir "tmp-file.dat")
            results (atom [])]
        (sync-file! base-file-url base-file)
        (create-fn (.getPath base-file) (.getPath tmp-file))
        ;; Do a few full schema scans to get the JIT going
        (dotimes [_ 5] (with-open [r (d/file-reader tmp-file)]
                         (last (d/read r))))
        (doseq [{:keys [query] :as benchmark} (random-queries-fn tmp-file)]
          (let [query-time (time-with-gc (with-open [r (d/file-reader tmp-file)]
                                           (last (d/read {:query query} r))))]
            (swap! results conj (assoc benchmark :query-time query-time))
            (spit results-file @results)))
        (with-open [w (io/writer csv-results-file)]
          (.write w "num_columns,total_length,max_column_length,query_time\n")
          (doseq [{:keys [total-length max-column-length num-columns query-time]} @results]
            (.write w (format "%d,%d,%d,%.2f\n" num-columns total-length max-column-length query-time))))))))

(defn run-benchmarks! [output-dir clean?]
  (let [output-dir (io/as-file output-dir)]
    (when clean?
      (delete-recursively! output-dir))
    (.mkdirs output-dir)
    (run-full-schema-benchmarks! output-dir
                                 "tpc_h"
                                 tpc-h/base-file-url
                                 tpc-h/full-schema-benchmarks)
    (run-sub-schema-benchmarks! output-dir
                                "tpc_h"
                                tpc-h/base-file-url
                                tpc-h/sub-schema-benchmarks)
    (run-full-schema-benchmarks! output-dir
                                 "media_content"
                                 media-content/base-file-url
                                 media-content/full-schema-benchmarks)
    (run-sub-schema-benchmarks! output-dir
                                "media_content"
                                media-content/base-file-url
                                media-content/sub-schema-benchmarks)
    (run-full-schema-benchmarks! output-dir
                                 "user_events"
                                 user-events/base-file-url
                                 user-events/full-schema-benchmarks)
    (run-sub-schema-benchmarks! output-dir
                                "user_events"
                                user-events/base-file-url
                                user-events/sub-schema-benchmarks)))

(def cli-options
  [["-h" "--help" "Print this help."]
   [nil "--output DIR" "Output results and temporary artificats to this folder." :default "output"]
   [nil "--clean" "Don't attempt to resume previous benchmark run."]])

(defn -main [& args]
  (let [{:keys [options summary errors]} (cli/parse-opts args cli-options)]
    (if (or errors (:help options))
      (println (str errors "\n\n" summary))
      (run-benchmarks! (:output options) (:clean options)))))
