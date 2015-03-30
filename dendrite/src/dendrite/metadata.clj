;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.metadata
  (:require [clojure.data.fressian :as fressian]
            [dendrite.utils :refer [defenum]])
  (:import [org.fressian.handlers WriteHandler ReadHandler]
           [java.io Writer]
           [java.nio ByteBuffer])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(defenum compression-type [:none :lz4 :deflate])

(defenum repetition-type [:optional :required :list :vector :map :set])

(defenum encoding-type
  [:plain :dictionary :packed-run-length :delta :incremental :delta-length :vlq :zig-zag :frequency])

(defrecord Metadata [record-groups-metadata schema custom-types custom])

(defrecord RecordGroupMetadata [length num-records column-chunks-metadata])

(defrecord ColumnChunkMetadata [length num-data-pages data-page-offset dictionary-page-offset])

(defrecord ColumnSpec [type encoding compression column-index query-column-index
                       max-repetition-level max-definition-level map-fn path])

(defn map->column-spec-with-defaults [m]
  (map->ColumnSpec (merge {:encoding :plain :compression :none} m)))

(defn print-colspec [column-spec ^Writer w]
  (let [{:keys [type encoding compression]} column-spec]
    (if (and (= compression :none) (= encoding :plain))
      (.write w (name type))
      (.write w (str "#col [" (name type) " " (name encoding)
                     (when-not (= compression :none) (str " " (name compression))) "]")))))

(.addMethod ^clojure.lang.MultiFn print-method ColumnSpec print-colspec)

(defn read-column-spec [vs]
  (let [[type encoding compression] (map keyword vs)]
    (map->column-spec-with-defaults (cond-> {:type type :encoding encoding}
                                            compression (assoc :compression compression)))))

(defrecord Field [name repetition repetition-level definition-level reader-fn column-spec sub-fields])

(def ^:private column-spec-tag "dendrite/column-spec")

(def ^:private column-spec-writer
  (reify WriteHandler
    (write [_ writer column-spec]
      (doto writer
        (.writeTag column-spec-tag 6)
        (.writeString (-> column-spec :type name))
        (.writeInt (-> column-spec :encoding encoding-type->int))
        (.writeInt (-> column-spec :compression compression-type->int))
        (.writeInt (:column-index column-spec))
        (.writeInt (:max-repetition-level column-spec))
        (.writeInt (:max-definition-level column-spec))))))

(def ^:private field-tag "dendrite/field")

(def ^:private field-writer
  (reify WriteHandler
    (write [_ writer field]
      (doto writer
        (.writeTag field-tag 6)
        (.writeString (some-> field :name name))
        (.writeInt (-> field :repetition repetition-type->int))
        (.writeInt (:repetition-level field))
        (.writeInt (:definition-level field))
        (.writeObject (:column-spec field))
        (.writeObject (:sub-fields field))))))

(def ^:private column-chunk-metadata-tag "dendrite/column-chunk-metadata")

(def ^:private column-chunk-metadata-writer
  (reify WriteHandler
    (write [_ writer {:keys [length num-data-pages data-page-offset dictionary-page-offset]}]
      (doto writer
        (.writeTag column-chunk-metadata-tag 4)
        (.writeInt length)
        (.writeInt num-data-pages)
        (.writeInt data-page-offset)
        (.writeInt dictionary-page-offset)))))

(def ^:private record-group-metadata-tag "dendrite/record-group-metadata")

(def ^:private record-group-metadata-writer
  (reify WriteHandler
    (write [_ writer {:keys [length num-records column-chunks-metadata]}]
      (doto writer
        (.writeTag record-group-metadata-tag 3)
        (.writeInt length)
        (.writeInt num-records)
        (.writeObject column-chunks-metadata)))))

(def ^:private metadata-tag "dendrite/metadata")

(defn- pack-custom-types [custom-types]
  (reduce-kv (fn [m t ct] (assoc m t (:base-type ct))) {} custom-types))

(defn- unpack-custom-types [packed-custom-types]
  (reduce-kv (fn [m k v] (assoc m k {:base-type v})) {} packed-custom-types))

(def ^:private metadata-writer
  (reify WriteHandler
    (write [_ writer {:keys [record-groups-metadata schema custom-types custom]}]
      (doto writer
        (.writeTag metadata-tag 4)
        (.writeObject record-groups-metadata)
        (.writeObject schema)
        (.writeObject (pack-custom-types custom-types))
        (.writeObject custom)))))

(def ^:private write-handlers
  (-> (merge {ColumnSpec {column-spec-tag column-spec-writer}
              Field {field-tag field-writer}
              ColumnChunkMetadata {column-chunk-metadata-tag column-chunk-metadata-writer}
              RecordGroupMetadata {record-group-metadata-tag record-group-metadata-writer}
              Metadata {metadata-tag metadata-writer}}
             fressian/clojure-write-handlers)
      fressian/associative-lookup
      fressian/inheritance-lookup))

(def ^:private column-spec-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->ColumnSpec {:type (-> reader .readObject keyword)
                        :encoding (-> reader .readInt int->encoding-type)
                        :compression (-> reader .readInt int->compression-type)
                        :column-index (.readInt reader)
                        :max-repetition-level (.readInt reader)
                        :max-definition-level (.readInt reader)}))))

(def ^:private field-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->Field {:name (-> reader .readObject keyword)
                   :repetition (-> reader .readInt int->repetition-type)
                   :repetition-level (.readInt reader)
                   :definition-level (.readInt reader)
                   :column-spec (.readObject reader)
                   :sub-fields (.readObject reader)}))))

(def ^:private column-chunk-metadata-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->ColumnChunkMetadata {:length (.readInt reader)
                                 :num-data-pages (.readInt reader)
                                 :data-page-offset (.readInt reader)
                                 :dictionary-page-offset (.readInt reader)}))))

(def ^:private record-group-metadata-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->RecordGroupMetadata {:length (.readInt reader)
                                 :num-records (.readInt reader)
                                 :column-chunks-metadata (.readObject reader)}))))

(def ^:private metadata-reader
  (reify ReadHandler
    (read [_ reader tag component-count]
      (map->Metadata {:record-groups-metadata (.readObject reader)
                      :schema (.readObject reader)
                      :custom-types (unpack-custom-types (.readObject reader))
                      :custom (.readObject reader)}))))

(def ^:private read-handlers
  (fressian/associative-lookup
   (merge {column-spec-tag column-spec-reader
           field-tag field-reader
           column-chunk-metadata-tag column-chunk-metadata-reader
           record-group-metadata-tag record-group-metadata-reader
           metadata-tag metadata-reader}
          fressian/clojure-read-handlers)))

(defn read [^ByteBuffer byte-buffer]
  (fressian/read byte-buffer :handlers read-handlers))

(defn write
  ^ByteBuffer [metadata]
  (fressian/write metadata :handlers write-handlers))
