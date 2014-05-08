(ns dendrite.metadata)

(defrecord ColumnChunkMetadata [bytes-size num-data-pages data-page-offset dictionary-page-offset])

(def column-chunk-metadata ->ColumnChunkMetadata)
