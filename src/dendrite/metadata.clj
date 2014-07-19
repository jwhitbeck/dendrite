(ns dendrite.metadata
  (:require [clojure.data.fressian :as fressian])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [java.io Writer]
           [java.nio ByteBuffer])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defrecord ColumnChunkMetadata [length num-data-pages data-page-offset dictionary-page-offset])

(defrecord RecordGroupMetadata [length num-records column-chunks-metadata])

(defrecord ColumnSpec [type encoding compression column-index query-column-index
                       max-repetition-level max-definition-level map-fn path])

(defn map->column-spec-with-defaults [m]
  (map->ColumnSpec (merge {:encoding :plain :compression :none} m)))

(defmethod print-method ColumnSpec
  [v ^Writer w]
  (.write w (str "#col " (cond-> (dissoc v :column-index :query-column-index
                                         :max-definition-level :max-repetition-level)
                                 (= (:compression v) :none) (dissoc :compression)
                                 (= (:encoding v) :plain) (dissoc :encoding)))))

(defrecord Field [name repetition repetition-level reader-fn column-spec sub-fields])

(def ^:private column-spec-tag "dendrite/column-spec")

(def ^:private column-spec-writer
  (reify WriteHandler
    (write [_ writer column-spec]
      (doto writer
        (.writeTag column-spec-tag 6)
        (.writeString (-> column-spec :type name))
        (.writeString (-> column-spec :encoding name))
        (.writeString (-> column-spec :compression name))
        (.writeInt (:column-index column-spec))
        (.writeInt (:max-repetition-level column-spec))
        (.writeInt (:max-definition-level column-spec))))))

(def ^:private field-tag "dendrite/field")

(def ^:private field-writer
  (reify WriteHandler
    (write [_ writer field]
      (doto writer
        (.writeTag field-tag 5)
        (.writeString (some-> field :name name))
        (.writeString (-> field :repetition name))
        (.writeInt (:repetition-level field))
        (.writeObject (:column-spec field))
        (.writeObject (:sub-fields field))))))

(def ^:private write-handlers
  (-> (merge {ColumnSpec {column-spec-tag column-spec-writer}
              Field {field-tag field-writer}}
             fressian/clojure-write-handlers)
      fressian/associative-lookup
      fressian/inheritance-lookup))

(def ^:private column-spec-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->ColumnSpec {:type (-> reader .readObject keyword)
                        :encoding (-> reader .readObject keyword)
                        :compression (-> reader .readObject keyword)
                        :column-index (.readInt reader)
                        :max-repetition-level (.readInt reader)
                        :max-definition-level (.readInt reader)}))))

(def ^:private field-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->Field {:name (-> reader .readObject keyword)
                   :repetition (-> reader .readObject keyword)
                   :repetition-level (.readInt reader)
                   :column-spec (.readObject reader)
                   :sub-fields (.readObject reader)}))))

(def ^:private read-handlers
  (-> (merge {column-spec-tag column-spec-reader
              field-tag field-reader}
             fressian/clojure-read-handlers)
      fressian/associative-lookup))

(defrecord Metadata [record-groups-metadata schema custom])

(defn read [^ByteBuffer byte-buffer]
  (fressian/read byte-buffer :handlers read-handlers))

(defn write
  ^ByteBuffer [metadata]
  (fressian/write metadata :handlers write-handlers))
