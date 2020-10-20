(ns dendrite.cli.core
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pprint]
            [clojure.tools.cli :as cli]
            [clojure.string :as str]
            [dendrite.core :as d])
  (:import [java.io BufferedReader]
           [dendrite.java Options])
  (:gen-class))

(set! *warn-on-reflection* true)

(def help-cli-option ["-h" "--help" "Print this help." :id :help?])

(defn- exit-err! [msg]
  (binding [*out* *err*]
    (println msg)
    (System/exit 1)))

(defn- ex-usage [msg]
  (ex-info msg {::usage? true}))

(defn- run-cmd [cli-options cmd-name cmd args]
  (let [{:keys [options
                arguments
                summary
                errors]} (cli/parse-opts args cli-options)]
    (when (:help? options)
      (println "Usage:" cmd-name "[OPTIONS] <path>")
      (println)
      (println summary)
      (System/exit 0))
    (when (seq errors)
      (exit-err! (str/join "\n" errors)))
    (try
      (cmd options (first arguments))
      (System/exit 0)
      (catch Exception e
        (if (-> e ex-data ::usage?)
          (exit-err! (.getMessage e))
          (throw e))))))

(def schema-cli-options
  [[nil "--compact" "Don't pretty-print the schema."
    :id :compact?]
   [nil "--full" "Show all encoding and compression annotations on columns."
    :id :full?]
   help-cli-option])

(defn schema [options filename]
  (with-open [r (d/file-reader filename)]
    (let [schema (if (:full? options)
                   (d/full-schema r)
                   (d/schema r))]
      (if (:compact? options)
        (prn schema)
        (pprint/pprint schema)))))

(def metadata-cli-options
  [[nil "--compact" "Don't pretty-print the metadata."
    :id :compact?]
   help-cli-option])

(defn metadata [options filename]
  (with-open [r (d/file-reader filename)]
    (let [metadata (d/metadata r)]
      (if (:compact? options)
        (prn metadata)
        (pprint/pprint metadata)))))

(def read-cli-options
  [[nil "--pretty" "Pretty-print the records"
    :id :pretty?]
   [nil "--head N" "Only print the first N records."
    :parse-fn #(Long/parseLong %)]
   [nil "--tail N" "Only print the last N records."
    :parse-fn #(Long/parseLong %)]
   [nil "--query QUERY" "Apply this query to the file. Expects a valid EDN string."
    :parse-fn edn/read-string]
   [nil "--query-file FILE" "Read the query from this file."]
   [nil "--sub-schema-in PATH" "Path to begining of sub-schema. Expects a valid EDN vector."
    :parse-fn edn/read-string]
   help-cli-option])

(defn read-file [cli-options filename]
  (let [{:keys [pretty?
                head
                tail
                query
                query-file
                sub-schema-in]} cli-options]
    (with-open [r (d/file-reader filename)]
      (let [opts (cond-> {}
                   query (assoc :query query)
                   query-file (assoc :query (-> query-file slurp edn/read-string))
                   sub-schema-in (assoc :sub-schema-in sub-schema-in))
            str-fn (if pretty?
                     #(with-out-str (pprint/pprint %))
                     str)
            view (->> (cond->> (d/read opts r)
                        head (d/sample #(< % head))
                        tail (d/sample #(>= % (- (d/num-records r) tail))))
                      (d/eduction (map str-fn)))
            print-fn (if pretty? print println)
            record-strs (cond
                          head (take head view)
                          tail (take-last tail view)
                          :else (seq view))]
        (doseq [s record-strs]
          (when (.checkError System/out)
            (System/exit 1))
          (print-fn s))
        (flush)))))

(def write-cli-options
  [[nil "--schema SCHEMA" "Write with this schema. Expects a valid EDN string."
    :parse-fn d/read-schema-string]
   [nil "--schema-file FILE" "Write with the schema from this file."]
   [nil "--metadata METADATA" "Set the file's metadata to METADATA. Expects a valid EDN string."
    :parse-fn edn/read-string]
   [nil "--metadata-file FILE" "Set the file's metadata to the contents of this file."]
   [nil "--data-page-length N" "The length in bytes of the data pages."
    :parse-fn #(Long/parseLong %)]
   [nil "--record-group-length N" "The length in bytes of each record group."
    :parse-fn #(Long/parseLong %)]
   [nil "--compression-thresholds MAP" "A map of compression method to the minimum compression threshold."
    :parse-fn edn/read-string]
   [nil "--optimize-columns all/default/none"  "Optimize encoding and compression for these columns."
    :default :default
    :parse-fn keyword
    :id :optimize-columns?]
   help-cli-option])

(defn write-file [cli-options filename]
  (let [schema (or (:schema cli-options)
                   (some-> (:schema-file cli-options) slurp edn/read-string))]
    (when-not schema
      (throw (ex-usage "Must specify at least one of --schema or --schema-file.")))
    (let [opts (select-keys cli-options [:data-page-length
                                         :record-group-length
                                         :compression-thresholds
                                         :optimize-columns?])]
      (with-open [w (d/file-writer opts schema filename)]
        (.writeAll w (pmap edn/read-string (line-seq (BufferedReader. *in*))))
        (when-let [metadata (or (:metadata cli-options)
                                (some-> (:metadata-file cli-options)
                                        slurp
                                        edn/read-string))]
          (d/set-metadata! w metadata))))))

(def stats-column-order
  ["column"
   "record-group"
   "type"
   "encoding"
   "compression"
   "length"
   "num-column-chunks"
   "num-pages"
   "num-records"
   "num-columns"
   "num-values"
   "num-dictionary-values"
   "header-length"
   "dictionary-header-length"
   "data-length"
   "dictionary-length"
   "definition-levels-length"
   "repetition-levels-length"
   "max-definition-level"
   "max-repetition-level"])

(def col-name->index
  (reduce-kv (fn [m i col-name]
               (assoc m col-name i))
             {}
             stats-column-order))

(def stats-cli-options
  [[nil "--sort-by COL" "Sort results by this column"]
   [nil "--desc" "Sort by descending order (default is ascending)"]
   [nil "--columns" "Breakdown stats by column"]
   [nil "--record-groups" "Breakdown stats by record-groups"]
   help-cli-option])

(defn- pull-up-byte-stats [stats]
  (merge (dissoc stats :byte-stats) (:byte-stats stats)))

(defn- format-global-stats [global-stats sort-col]
  (->> global-stats
       (map (fn [[k v]] {"name" (name k) "value" v}))
       (sort-by #(get % (or sort-col "name")))))

(defn- str-keys [m] (reduce-kv (fn [nm k v] (assoc nm (name k) v)) {} m))

(defn- format-column-stats [column-stats sort-col]
  (->> column-stats
       (map (fn [col-stats]
              (let [column (->> col-stats
                                :path
                                (remove nil?)
                                (map name)
                                (str/join "."))]
                (str-keys (-> col-stats
                              (dissoc :path)
                              (assoc :column column))))))
       (sort-by #(get % (or sort-col "column")))))

(defn- format-record-group-stats [record-group-stats sort-col]
  (sort-by
   #(get % (or sort-col "record-group"))
   (map (fn [i rgs] (assoc (str-keys rgs) "record-group" i))
        (range)
        record-group-stats)))

(defn stats [options filename]
  (with-open [r (d/file-reader filename)]
    (let [sort-col (:sort-by options)
          rows (cond->> (cond (:columns options)
                              (format-column-stats (:columns (d/stats r))
                                                   sort-col)
                              (:record-groups options)
                              (format-record-group-stats (:record-groups (d/stats r))
                                                         sort-col)
                              :else
                              (format-global-stats (:global (d/stats r))
                                                   (:sort-by options)))
                 (:desc options) reverse)]
      (if (or (:columns options) (:record-groups options))
        (pprint/print-table (filter (-> rows first keys set) stats-column-order)
                            rows)
        (pprint/print-table rows)))))

(def custom-types-cli-options
  [help-cli-option])

(defn custom-types [options filename]
  (with-open [r (d/file-reader filename)]
    (prn (d/custom-types r))))

(def commands {"schema" [schema schema-cli-options]
               "read" [read-file read-cli-options]
               "meta" [metadata metadata-cli-options]
               "stats" [stats stats-cli-options]
               "custom-types" [custom-types custom-types-cli-options]
               "write" [write-file write-cli-options]})

(def main-help-str
  (->> ["Usage: CMD [OPTIONS] [ARGS]

where CMD is one of:

" (str/join "\n" (map #(str " - " %) (keys commands))) "

To get help on any command, use CMD --help."]
       flatten
       (apply str)))

(defn -main [& args]
  (let [[cmd-name & cmd-args] args
        [cmd cli-options] (get commands cmd-name)]
    (when-not (and cmd cli-options)
      (println main-help-str)
      (System/exit 0))
    (run-cmd cli-options cmd-name cmd cmd-args)))
