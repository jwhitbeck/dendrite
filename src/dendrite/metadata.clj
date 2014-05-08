(ns dendrite.metadata)

(defrecord ColumnChunkMetadata [bytes-size num-data-pages data-page-offset dictionnary-page-offset])

(def column-chunk-metadata ->ColumnChunkMetadata)
