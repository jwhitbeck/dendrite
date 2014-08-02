(ns dendrite.stats)

(set! *warn-on-reflection* true)

(defrecord ByteStats [header-length repetition-levels-length definition-levels-length data-length
                      dictionary-header-length dictionary-length])

(defrecord PageStats [num-values length byte-stats])

(defn add-byte-stats [byte-stats-a byte-stats-b]
  (->> (map (fnil + 0 0) (vals byte-stats-a) (vals byte-stats-b))
       (apply ->ByteStats)))

(defrecord ColumnChunkStats [spec num-pages num-values length byte-stats])

(defn pages->column-chunk-stats [spec pages-stats]
  (let [all-bytes-stats (reduce add-byte-stats (map :byte-stats pages-stats))]
    (map->ColumnChunkStats
     {:spec spec
      :num-pages (count pages-stats)
      :num-values (reduce + (map :num-values pages-stats))
      :length (reduce + (map :length pages-stats))
      :byte-stats all-bytes-stats})))

(defn add-column-chunk-stats [column-chunk-stats-a column-chunk-stats-b]
  (map->ColumnChunkStats
   {:spec (:spec column-chunk-stats-a)
    :num-pages (+ (:num-pages column-chunk-stats-a) (:num-pages column-chunk-stats-b))
    :length (+ (:length column-chunk-stats-a) (:length column-chunk-stats-b))
    :byte-stats (add-byte-stats (:byte-stats column-chunk-stats-a) (:byte-stats column-chunk-stats-b))}))

(defrecord ColumnStats [spec length num-chunks num-pages num-values byte-stats])

(defn column-chunks->column-stats [column-chunks-stats]
  (map->ColumnStats
   {:spec (-> column-chunks-stats first :spec)
    :length (reduce + (map :length column-chunks-stats))
    :num-values (reduce + (map :num-values column-chunks-stats))
    :num-chunks (count column-chunks-stats)
    :num-pages (reduce + (map :num-pages column-chunks-stats))
    :byte-stats (reduce add-byte-stats (map :byte-stats column-chunks-stats))}))

(defrecord RecordGroupStats [length num-records num-columns byte-stats])

(defn column-chunks->record-group-stats [num-records column-chunks-stats]
  (map->RecordGroupStats
   {:length (reduce + (map :length column-chunks-stats))
    :num-records num-records
    :num-columns (count column-chunks-stats)
    :byte-stats (reduce add-byte-stats (map :byte-stats column-chunks-stats))}))

(defrecord GlobalStats [data-length length num-records num-columns byte-stats])

(defn record-groups->global-stats [length record-groups-stats]
  (map->GlobalStats
   {:data-length (reduce + (map :length record-groups-stats))
    :length length
    :num-records (reduce + (map :num-records record-groups-stats))
    :num-columns (-> record-groups-stats first :num-columns)
    :num-record-groups (count record-groups-stats)
    :byte-stats (reduce add-byte-stats (map :byte-stats record-groups-stats))}))
