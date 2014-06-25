(ns dendrite.metadata)

(defrecord ColumnChunkMetadata [bytes-size num-data-pages data-page-offset dictionary-page-offset])

(def column-chunk-metadata ->ColumnChunkMetadata)

(defrecord RecordGroupMetadata [bytes-size num-records column-chunks-metadata])

(def record-group-metadata ->RecordGroupMetadata)
