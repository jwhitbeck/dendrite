(ns dendrite.cli.core
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pprint]
            [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [dendrite.core :as d])
  (:import [java.io BufferedReader])
  (:gen-class))

(set! *warn-on-reflection* true)

(def help-cli-option ["-h" "--help" "Print this help."])

(defn- run-cmd [cli-options cmd-name cmd args]
  (let [{:keys [options arguments summary errors]} (cli/parse-opts args cli-options)]
    (if (or errors (not= (count arguments) 1))
      (println (string/join "\n\n" (concat errors [(str "Usage: " cmd-name " [OPTIONS] <path>") summary])))
      (cmd options (first arguments)))))

(def schema-cli-options
  [[nil "--pretty" "Pretty-print the schema."]
   help-cli-option])

(defn schema [options filename]
  (with-open [r (d/file-reader filename)]
    (let [schema (d/schema r)]
      (if (:pretty options)
        (pprint/pprint schema)
        (prn schema)))))

(def metadata-cli-options
  [[nil "--pretty" "Pretty-print the metadata"]
   help-cli-option])

(defn metadata [options filename]
  (with-open [r (d/file-reader filename)]
    (let [metadata (d/metadata r)]
      (if (:pretty options)
        (pprint/pprint metadata)
        (prn metadata)))))

(def read-cli-options
  [[nil "--pretty" "Pretty-print the records"]
   [nil "--head N" "Only print the first N records." :parse-fn #(Long/parseLong %)]
   [nil "--tail N" "Only print the last N records." :parse-fn #(Long/parseLong %)]
   [nil "--query QUERY" "Apply this query to the file. Expects a valid EDN string."
    :parse-fn edn/read-string]
   [nil "--query-file FILE" "Read the query from this file." :parse-fn (comp edn/read-string slurp)]
   [nil "--entrypoint ENTRYPOINT" "Path to begining of sub-schema. Should be a valid EDN vector."
    :parse-fn edn/read-string]
   help-cli-option])

(defn read-file [cli-options filename]
  (with-open [r (d/file-reader filename)]
    (let [opts (cond-> {}
                 (:query cli-options) (assoc :query (:query cli-options))
                 (:query-file cli-options) (assoc :query (:query-file cli-options))
                 (:entrypoint cli-options) (assoc :entrypoint (:entrypoint cli-options)))
          record->str-fn (if (:pretty cli-options)
                           #(with-out-str (pprint/pprint %))
                           #(str (pr-str %) "\n"))
          record-strs (cond
                        (:head cli-options) (map record->str-fn
                                                 (take (:head cli-options) (d/read opts r)))
                        (:tail cli-options) (map record->str-fn
                                                 (take-last (:tail cli-options) (d/read opts r)))
                        :else (d/pmap opts record->str-fn r))]
      (doseq [s record-strs]
        (when (.checkError System/out)
          (System/exit 1))
        (print s))
      (flush))))

(def write-cli-options
  [[nil "--schema SCHEMA" "Write with this schema. Expects a valid EDN string."
    :parse-fn d/read-schema-string]
   [nil "--schema-file FILE" "Write with the schema from this file."
    :parse-fn (comp d/read-schema-string slurp)]
   [nil "--metadata METADATA" "Set the file's metadata to METADATA. Expects a valid EDN string."
    :parse-fn edn/read-string]
   [nil "--metadata-file FILE" "Set the file's metadata to the contents of this file."
    :parse-fn (comp edn/read-string slurp)]
   help-cli-option])

(defn write-file [cli-options filename]
  (let [schema (or (:schema cli-options) (:schema-file cli-options))]
    (when-not schema
      (throw (IllegalStateException. "must specify at least one of --schema or --schema-file.")))
    (with-open [w (d/file-writer schema filename)]
      (doseq [record (pmap edn/read-string (line-seq (BufferedReader. *in*)))]
        (d/write! w record))
      (when-let [metadata (or (:metadata cli-options) (:metadata-file cli-options))]
        (d/set-metadata! w metadata)))))

(def stats-column-order
  ["column" "record-group" "type" "encoding" "compression" "length" "num-column-chunks" "num-pages"
   "num-records" "num-columns" "header-length" "dictionary-header-length" "data-length" "dictionary-length"
   "definition-levels-length" "repetition-levels-length" "num-values" "num-dictionary-values"
   "max-definition-level" "max-repetition-level"])

(def col-name->index (reduce-kv (fn [m i col-name] (assoc m col-name i)) {} stats-column-order))

(def stats-cli-options
  [[nil "--sort COL" "Sort results by this column"]
   [nil "--desc" "Sort by descending order (default is ascending)"]
   [nil "--columns" "Breakdown stats by column"]
   [nil "--record-groups" "Breakdown stats by record-groups"]
   help-cli-option])

(defn- pull-up-byte-stats [stats]
  (merge (dissoc stats :byte-stats) (:byte-stats stats)))

(defn- format-global-stats [global-stats sort-col]
  (->> (pull-up-byte-stats global-stats)
       (map (fn [[k v]] {"name" (name k) "value" v}))
       (sort-by #(get % (or sort-col "name")))))

(defn- str-keys [m] (reduce-kv (fn [nm k v] (assoc nm (name k) v)) {} m))

(defn- format-column-stats [column-stats sort-col]
  (->> column-stats
       (map (fn [col-stats]
              (let [column (->> col-stats :spec :path (map name) (string/join "."))
                    spec (-> col-stats :spec (dissoc :path))]
                (str-keys (merge (pull-up-byte-stats (dissoc col-stats :spec :num-column-chunks))
                                 spec
                                 {:column column})))))
       (sort-by #(get % (or sort-col "column")))))

(defn- format-record-group-stats [record-group-stats sort-col]
  (sort-by
   #(get % (or sort-col "record-group"))
   (map (fn [i rgs] (assoc (str-keys (pull-up-byte-stats rgs)) "record-group" i))
        (range)
        record-group-stats)))

(defn stats [options filename]
  (with-open [r (d/file-reader filename)]
    (let [sort-col (:sort options)
          rows (cond->> (cond (:columns options)
                              (format-column-stats (:columns (d/stats r)) sort-col)
                              (:record-groups options)
                              (format-record-group-stats (:record-groups (d/stats r)) sort-col)
                              :else
                              (format-global-stats (:global (d/stats r)) (:sort options)))
                 (:desc options) reverse)]
      (if (or (:columns options) (:record-groups options))
        (pprint/print-table (filter (-> rows first keys set) stats-column-order) rows)
        (pprint/print-table rows)))))

(def commands {"schema" [schema schema-cli-options]
               "read" [read-file read-cli-options]
               "meta" [metadata metadata-cli-options]
               "stats" [stats stats-cli-options]
               "write" [write-file write-cli-options]})

(def main-help-str
  (string/join "\n" (concat ["Usage: CMD [OPTIONS] [ARGS]"
                             ""
                             "where CMD is one of:"]
                            (for [cmd-name (keys commands)]
                              (str " - " cmd-name))
                            [""
                             "To get help on any command, use CMD --help."])))

(defn -main [& args]
  (let [[cmd-name & cmd-args] args
        [cmd cli-options] (get commands cmd-name)]
    (if-not (and cmd cli-options)
      (println main-help-str)
      (run-cmd cli-options cmd-name cmd cmd-args)))
  (shutdown-agents))