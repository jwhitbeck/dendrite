(ns dendrite.schema)

(defrecord ColumnType [value-type encoding compression-type required?])

(def column-type ->ColumnType)
