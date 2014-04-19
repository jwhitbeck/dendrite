(ns dendrite.schema)

(defrecord ColumnType [value-type encoding compression-type required?])

(defn column-type [value-type encoding compression-type required?]
  (ColumnType. value-type encoding compression-type required?))
