(ns dendrite.benchmark.utils
  (:require [clojure.java.io :as io])
  (:import [java.io BufferedReader BufferedWriter FileInputStream FileOutputStream InputStreamReader
            OutputStreamWriter]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

(defn gzip-writer [f]
  (-> f io/as-file FileOutputStream. GZIPOutputStream. OutputStreamWriter. BufferedWriter.))

(defn gzip-reader [f]
  (-> f io/as-file FileInputStream. GZIPInputStream. InputStreamReader. BufferedReader.))
