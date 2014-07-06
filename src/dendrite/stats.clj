(ns dendrite.stats)

(defrecord ByteStats [header-bytes repetition-levels-bytes definition-levels-bytes data-bytes
                      dictionary-header-bytes dictionary-bytes])

(defrecord PageStats [num-values total-bytes byte-stats])

(defn add-byte-stats [byte-stats-a byte-stats-b]
  (->> (map (fnil + 0 0) (vals byte-stats-a) (vals byte-stats-b))
       (apply ->ByteStats)))

(defrecord ColumnChunkStats [spec num-pages num-values total-bytes byte-stats])

(defn pages->column-chunk-stats [spec pages-stats]
  (let [all-bytes-stats (reduce add-byte-stats (map :byte-stats pages-stats))]
    (map->ColumnChunkStats
     {:spec spec
      :num-pages (count pages-stats)
      :num-values (reduce + (map :num-values pages-stats))
      :total-bytes (reduce + (map :total-bytes pages-stats))
      :byte-stats all-bytes-stats})))

(defn add-column-chunk-stats [column-chunk-stats-a column-chunk-stats-b]
  (map->ColumnChunkStats
   {:spec (:spec column-chunk-stats-a)
    :num-pages (+ (:num-pages column-chunk-stats-a) (:num-pages column-chunk-stats-b))
    :total-bytes (+ (:total-bytes column-chunk-stats-a) (:total-bytes column-chunk-stats-b))
    :byte-stats (add-byte-stats (:byte-stats column-chunk-stats-a) (:byte-stats column-chunk-stats-b))}))

(defrecord ColumnStats [spec total-bytes num-chunks num-pages num-values byte-stats])

(defn column-chunks->column-stats [column-chunks-stats]
  (map->ColumnStats
   {:spec (-> column-chunks-stats first :spec)
    :total-bytes (reduce + (map :total-bytes column-chunks-stats))
    :num-values (reduce + (map :num-values column-chunks-stats))
    :num-chunks (count column-chunks-stats)
    :num-pages (reduce + (map :num-pages column-chunks-stats))
    :byte-stats (reduce add-byte-stats (map :byte-stats column-chunks-stats))}))

(defrecord RecordGroupStats [total-bytes num-records num-columns byte-stats])

(defn column-chunks->record-group-stats [num-records column-chunks-stats]
  (map->RecordGroupStats
   {:total-bytes (reduce + (map :total-bytes column-chunks-stats))
    :num-records num-records
    :num-columns (count column-chunks-stats)
    :byte-stats (reduce add-byte-stats (map :byte-stats column-chunks-stats))}))

(defrecord GlobalStats [total-data-bytes total-bytes num-records num-columns byte-stats])

(defn record-groups->global-stats [total-bytes record-groups-stats]
  (map->GlobalStats
   {:total-data-bytes (reduce + (map :total-bytes record-groups-stats))
    :total-bytes total-bytes
    :num-records (reduce + (map :num-records record-groups-stats))
    :num-columns (-> record-groups-stats first :num-columns)
    :num-record-groups (count record-groups-stats)
    :byte-stats (reduce add-byte-stats (map :byte-stats record-groups-stats))}))
