;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.page
  (:require [dendrite.compression :as compression]
            [dendrite.encoding :refer [levels-encoder levels-decoder encoder decoder-ctor]]
            [dendrite.estimation :as estimation]
            [dendrite.stats :as stats]
            [dendrite.utils :refer [defenum] :as utils])
  (:import [dendrite.java Bytes ICompressor IDecompressor Dictionary IEncoder LeveledValue LeveledValues
            MemoryOutputStream IOutputBuffer IWriteable]
           [java.nio ByteBuffer])
  (:refer-clojure :exclude [read type]))

(set! *warn-on-reflection* true)

(defenum page-type [:data :dictionary])

(defn- read-next-page-type [^ByteBuffer bb]
  (-> bb Bytes/readUInt int->page-type))

(defn- read-next-data-page-type [^ByteBuffer bb]
  (let [data-page-type (read-next-page-type bb)]
    (when-not (= data-page-type :data)
      (throw
       (IllegalArgumentException. (format "Page type %s is not a supported data page type" data-page-type))))
    data-page-type))

(defn- read-next-dictionary-page-type [^ByteBuffer bb]
  (let [dictionary-page-type (read-next-page-type bb)]
    (when-not (= dictionary-page-type :dictionary)
      (throw
       (IllegalArgumentException.
        (format "Page type %s is not a supported dictionary page type" dictionary-page-type))))
    dictionary-page-type))

(defprotocol IPageHeader
  (type [this])
  (header-length [this])
  (body-length [this])
  (byte-offset-body [this])
  (stats [_]))

(defprotocol IDataPageHeader
  (has-repetition-levels? [this])
  (has-definition-levels? [this])
  (byte-offset-repetition-levels [this])
  (byte-offset-definition-levels [this]))

(defn- uints32-encoded-length [coll]
  (reduce #(+ %1 (Bytes/getNumUIntBytes %2)) 0 coll))

(defn- encode-uints32! [^MemoryOutputStream mos coll]
  (doseq [item coll]
    (Bytes/writeUInt mos item)))

(defrecord DataPageHeader [encoded-page-type
                           num-values
                           repetition-levels-length
                           definition-levels-length
                           compressed-data-length
                           uncompressed-data-length]
  IPageHeader
  (type [this]
    (int->page-type encoded-page-type))
  (header-length [this]
    (uints32-encoded-length (vals this)))
  (body-length [_]
    (+ repetition-levels-length definition-levels-length compressed-data-length))
  (byte-offset-body [_]
    (+ repetition-levels-length definition-levels-length))
  (stats [this]
    (stats/map->PageStats
     {:num-values num-values
      :length (+ (header-length this) (body-length this))
      :byte-stats (stats/map->ByteStats {:header-length (header-length this)
                                         :repetition-levels-length repetition-levels-length
                                         :definition-levels-length definition-levels-length
                                         :data-length compressed-data-length})}))
  IDataPageHeader
  (has-repetition-levels? [_]
    (pos? repetition-levels-length))
  (has-definition-levels? [_]
    (pos? definition-levels-length))
  (byte-offset-repetition-levels [_]
    0)
  (byte-offset-definition-levels [_]
    repetition-levels-length)
  IWriteable
  (writeTo [this mos]
    (encode-uints32! mos (vals this))))

(defn- read-data-page-header [^ByteBuffer bb data-page-type]
  (map->DataPageHeader
   {:encoded-page-type (page-type->int data-page-type)
    :num-values (Bytes/readUInt bb)
    :repetition-levels-length (Bytes/readUInt bb)
    :definition-levels-length (Bytes/readUInt bb)
    :compressed-data-length (Bytes/readUInt bb)
    :uncompressed-data-length (Bytes/readUInt bb)}))

(defrecord DictionaryPageHeader [encoded-page-type
                                 num-values
                                 compressed-data-length
                                 uncompressed-data-length]
  IPageHeader
  (type [this]
    (int->page-type encoded-page-type))
  (header-length [this]
    (uints32-encoded-length (vals this)))
  (body-length [_]
    compressed-data-length)
  (byte-offset-body [_]
    0)
  (stats [this]
    (stats/map->PageStats
     {:num-values num-values
      :length (+ (header-length this) (body-length this))
      :byte-stats (stats/map->ByteStats {:dictionary-header-length (header-length this)
                                         :dictionary-length compressed-data-length})}))
  IWriteable
  (writeTo [this mos]
    (encode-uints32! mos (vals this))))

(defn- read-dictionary-page-header [^ByteBuffer bb dictionary-page-type]
  (map->DictionaryPageHeader
   {:encoded-page-type (page-type->int dictionary-page-type)
    :num-values (Bytes/readUInt bb)
    :compressed-data-length (Bytes/readUInt bb)
    :uncompressed-data-length (Bytes/readUInt bb)}))

(defprotocol IPageWriter
  (num-values [this]))

(defprotocol IDataPageWriter
  (write! [this values]))

(defprotocol IDictionaryPageWriter
  (write-entry! [this entry]))

(defprotocol IPageWriterImpl
  (provisional-header [this])
  (header [this]))

(defrecord DataPageWriter
    [body-length-estimator
     ^IEncoder repetition-level-encoder
     ^IEncoder definition-level-encoder
     ^IEncoder data-encoder
     ^ICompressor data-compressor
     finished?]
  IDataPageWriter
  (write! [this v]
    (if repetition-level-encoder
      (doseq [^LeveledValue lv v]
        (let [v (.value lv)]
          (when-not (nil? v)
            (.encode data-encoder v)))
        (.encode repetition-level-encoder (.repetitionLevel lv))
        (.encode definition-level-encoder (.definitionLevel lv)))
      (if definition-level-encoder
        (if (nil? v)
          (.encode definition-level-encoder (int 0))
          (do (.encode definition-level-encoder (int 1))
              (.encode data-encoder v)))
        (.encode data-encoder v)))
    this)
  IPageWriter
  (num-values [_]
    (if definition-level-encoder
      (.numEncodedValues definition-level-encoder)
      (.numEncodedValues data-encoder)))
  IPageWriterImpl
  (provisional-header [this]
    (map->DataPageHeader
     {:encoded-page-type (page-type->int :data)
      :num-values (num-values this)
      :repetition-levels-length (if repetition-level-encoder (.estimatedLength repetition-level-encoder) 0)
      :definition-levels-length (if definition-level-encoder (.estimatedLength definition-level-encoder) 0)
      :compressed-data-length (.estimatedLength data-encoder)
      :uncompressed-data-length (.estimatedLength data-encoder)}))
  (header [this]
    (map->DataPageHeader
     {:encoded-page-type (page-type->int :data)
      :num-values (num-values this)
      :repetition-levels-length (if repetition-level-encoder (.length repetition-level-encoder) 0)
      :definition-levels-length (if definition-level-encoder (.length definition-level-encoder) 0)
      :compressed-data-length (if data-compressor
                                   (.length data-compressor)
                                   (.length data-encoder))
      :uncompressed-data-length (.length data-encoder)}))
  IOutputBuffer
  (reset [_]
    (reset! finished? false)
    (when repetition-level-encoder
      (.reset repetition-level-encoder))
    (when definition-level-encoder
      (.reset definition-level-encoder))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not @finished?
      (let [estimated-body-length (-> this provisional-header body-length)]
        (when repetition-level-encoder
          (.finish repetition-level-encoder))
        (when definition-level-encoder
          (.finish definition-level-encoder))
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (estimation/update! body-length-estimator
                            (-> this header body-length) estimated-body-length))
      (reset! finished? true)))
  (length [this]
    (let [h (header this)]
      (+ (header-length h) (body-length h))))
  (estimatedLength [this]
    (let [provisional-header (provisional-header this)]
      (+ (header-length provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (writeTo [this mos]
    (.finish this)
    (.writeTo ^IWriteable (header this) mos)
    (when repetition-level-encoder
      (.writeTo repetition-level-encoder mos))
    (when definition-level-encoder
      (.writeTo definition-level-encoder mos))
    (if data-compressor
      (.writeTo data-compressor mos)
      (.writeTo data-encoder mos))))

(defn data-page-writer [max-repetition-level max-definition-level type-store value-type encoding compression]
  (map->DataPageWriter
     {:body-length-estimator (estimation/ratio-estimator)
      :repetition-level-encoder (when (pos? max-repetition-level) (levels-encoder max-repetition-level))
      :definition-level-encoder (when (pos? max-definition-level) (levels-encoder max-definition-level))
      :data-encoder (encoder type-store value-type encoding)
      :data-compressor (compression/compressor compression)
      :finished? (atom false)}))

(defrecord DictionaryPageWriter [body-length-estimator
                                 ^IEncoder data-encoder
                                 ^ICompressor data-compressor
                                 finished?]
  IDictionaryPageWriter
  (write-entry! [this v]
    (.encode data-encoder v)
    this)
  IPageWriter
  (num-values [_] (.numEncodedValues data-encoder))
  IPageWriterImpl
  (provisional-header [this]
    (map->DictionaryPageHeader
     {:encoded-page-type (page-type->int :dictionary)
      :num-values (num-values this)
      :compressed-data-length (.estimatedLength data-encoder)
      :uncompressed-data-length (.estimatedLength data-encoder)}))
  (header [this]
    (map->DictionaryPageHeader
     {:encoded-page-type (page-type->int :dictionary)
      :num-values (num-values this)
      :compressed-data-length (if data-compressor
                                   (.length data-compressor)
                                   (.length data-encoder))
      :uncompressed-data-length (.length data-encoder)}))
  IOutputBuffer
  (reset [_]
    (reset! finished? false)
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not @finished?
      (let [estimated-body-length (-> this provisional-header body-length)]
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (estimation/update! body-length-estimator
                            (-> this header body-length) estimated-body-length))
      (reset! finished? true)))
  (length [this]
    (let [h (header this)]
      (+ (header-length h) (body-length h))))
  (estimatedLength [this]
    (let [provisional-header (provisional-header this)]
      (+ (header-length provisional-header)
         (estimation/correct body-length-estimator (body-length provisional-header)))))
  (writeTo [this mos]
    (.finish this)
    (.writeTo ^IWriteable (header this) mos)
    (if data-compressor
      (.writeTo data-compressor mos)
      (.writeTo data-encoder mos))))

(defn dictionary-page-writer [type-store value-type encoding compression]
  (map->DictionaryPageWriter
   {:body-length-estimator (estimation/ratio-estimator)
    :data-encoder (encoder type-store value-type encoding)
    :data-compressor (compression/compressor compression)
    :finished? (atom false)}))

(defprotocol IDataPageReader
  (read [_] [_ map-fn])
  (skip [_]))

(defrecord DataPageReader [^ByteBuffer byte-buffer
                           max-repetition-level
                           max-definition-level
                           data-decoder-ctor
                           decompressor-ctor
                           header]
  IDataPageReader
  (read [this]
    (read this nil))
  (read [this map-fn]
    (let [repetition-levels-decoder (when (has-repetition-levels? header)
                                      (-> byte-buffer
                                          (utils/skip (byte-offset-repetition-levels header))
                                          (levels-decoder max-repetition-level)))
          definition-levels-decoder (when (has-definition-levels? header)
                                      (-> byte-buffer
                                          (utils/skip (byte-offset-definition-levels header))
                                          (levels-decoder max-definition-level)))
          data-byte-buffer (utils/skip byte-buffer (byte-offset-body header))
          data-byte-buffer (if-let [^IDecompressor decompressor (decompressor-ctor)]
                             (.decompress decompressor
                                          data-byte-buffer
                                          (:compressed-data-length header)
                                          (:uncompressed-data-length header))
                             data-byte-buffer)
          data-decoder (data-decoder-ctor data-byte-buffer)]
      (LeveledValues/assemble repetition-levels-decoder
                              definition-levels-decoder
                              data-decoder
                              max-definition-level
                              map-fn)))
  (skip [_]
    (utils/skip byte-buffer (body-length header))))

(defn data-page-reader
  [^ByteBuffer byte-buffer max-repetition-level max-definition-level type-store value-type encoding
   compression]
  (let [bb (.duplicate byte-buffer)
        page-type (read-next-data-page-type bb)]
    (map->DataPageReader
     {:byte-buffer bb
      :max-repetition-level max-repetition-level
      :max-definition-level max-definition-level
      :data-decoder-ctor (decoder-ctor type-store value-type encoding)
      :decompressor-ctor (compression/decompressor-ctor compression)
      :header (read-data-page-header bb page-type)})))

(defn- data-page-readers
  [^ByteBuffer byte-buffer num-data-pages max-repetition-level max-definition-level type-store value-type
   encoding compression]
  (let [num-data-pages (int num-data-pages)]
    (lazy-seq
     (when (pos? num-data-pages)
       (let [next-data-page-reader (data-page-reader byte-buffer max-repetition-level
                                                     max-definition-level type-store value-type
                                                     encoding compression)]
         (cons next-data-page-reader
               (data-page-readers (skip next-data-page-reader) (dec num-data-pages) max-repetition-level
                                  max-definition-level type-store value-type encoding compression)))))))

(defn read-data-pages
  [^ByteBuffer byte-buffer num-data-pages max-repetition-level max-definition-level type-store value-type
   encoding compression map-fn]
  (utils/pmap-1 #(read % map-fn)
                (data-page-readers byte-buffer num-data-pages max-repetition-level max-definition-level
                                   type-store value-type encoding compression)))

(defn read-data-page-headers
  [^ByteBuffer byte-buffer num-data-pages]
  (let [num-data-pages (int num-data-pages)
        bb (.duplicate byte-buffer)]
    (lazy-seq
     (when (pos? num-data-pages)
       (let [page-type (read-next-data-page-type bb)
             next-header (read-data-page-header bb page-type)
             next-byte-array-reader (utils/skip bb (body-length next-header))]
         (cons next-header (read-data-page-headers next-byte-array-reader (dec num-data-pages))))))))

(defprotocol IDictionaryPageReader
  (read-array [_] [_ map-fn]))

(defrecord DictionaryPageReader [^ByteBuffer byte-buffer
                                 data-decoder-ctor
                                 decompressor-ctor
                                 header]
  IDictionaryPageReader
  (read-array [this]
    (read-array this nil))
  (read-array [_ map-fn]
    (let [data-byte-buffer (utils/skip byte-buffer (byte-offset-body header))
          data-byte-buffer (if-let [^IDecompressor decompressor (decompressor-ctor)]
                             (.decompress decompressor
                                          data-byte-buffer
                                          (:compressed-data-length header)
                                          (:uncompressed-data-length header))
                              data-byte-buffer)]
      (Dictionary/read (data-decoder-ctor data-byte-buffer) map-fn))))

(defn dictionary-page-reader
  [^ByteBuffer byte-buffer type-store value-type encoding compression]
  (let [bb (.duplicate byte-buffer)
        page-type (read-next-dictionary-page-type bb)]
    (map->DictionaryPageReader
     {:byte-buffer bb
      :data-decoder-ctor (decoder-ctor type-store value-type encoding)
      :decompressor-ctor (compression/decompressor-ctor compression)
      :header (read-dictionary-page-header bb page-type)})))

(defn read-dictionary [^ByteBuffer byte-buffer type-store value-type encoding compression map-fn]
  (read-array (dictionary-page-reader byte-buffer type-store value-type encoding compression) map-fn))

(defn read-dictionary-header [^ByteBuffer byte-buffer]
  (let [bb (.duplicate byte-buffer)
        page-type (read-next-dictionary-page-type bb)]
    (read-dictionary-page-header bb page-type)))
