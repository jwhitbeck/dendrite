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
            [dendrite.stats :as stats]
            [dendrite.utils :refer [defenum] :as utils])
  (:import [dendrite.java Bytes Estimator ICompressor IDecompressor DictionaryValues IEncoder
            LeveledValue LeveledValues MemoryOutputStream IOutputBuffer IWriteable]
           [java.nio ByteBuffer]))

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

(definterface IPageHeader
  (type [])
  (^long headerLength [])
  (^long bodyLength [])
  (^long byteOffsetBody [])
  (stats []))

(defn stats [^IPageHeader header] (.stats header))

(definterface IDataPageHeader
  (^boolean hasRepetitionLevels [])
  (^boolean hasDefinitionLevels [])
  (^long byteOffsetRepetitionLevels [])
  (^long byteOffsetDefinitionLevels []))

(deftype DataPageHeader [^long encoded-page-type
                         ^long num-values
                         ^long repetition-levels-length
                         ^long definition-levels-length
                         ^long compressed-data-length
                         ^long uncompressed-data-length]
  IPageHeader
  (type [this]
    (int->page-type encoded-page-type))
  (headerLength [this]
    (+ (Bytes/getNumUIntBytes encoded-page-type)
       (Bytes/getNumUIntBytes num-values)
       (Bytes/getNumUIntBytes repetition-levels-length)
       (Bytes/getNumUIntBytes definition-levels-length)
       (Bytes/getNumUIntBytes compressed-data-length)
       (Bytes/getNumUIntBytes uncompressed-data-length)))
  (bodyLength [_]
    (+ repetition-levels-length definition-levels-length compressed-data-length))
  (byteOffsetBody [_]
    (+ repetition-levels-length definition-levels-length))
  (stats [this]
    (stats/map->PageStats
     {:num-values num-values
      :length (+ (.headerLength this) (.bodyLength this))
      :byte-stats (stats/map->ByteStats {:header-length (.headerLength this)
                                         :repetition-levels-length repetition-levels-length
                                         :definition-levels-length definition-levels-length
                                         :data-length compressed-data-length})}))
  IDataPageHeader
  (hasRepetitionLevels [_]
    (pos? repetition-levels-length))
  (hasDefinitionLevels [_]
    (pos? definition-levels-length))
  (byteOffsetRepetitionLevels [_]
    0)
  (byteOffsetDefinitionLevels [_]
    repetition-levels-length)
  IWriteable
  (writeTo [this mos]
    (Bytes/writeUInt mos encoded-page-type)
    (Bytes/writeUInt mos num-values)
    (Bytes/writeUInt mos repetition-levels-length)
    (Bytes/writeUInt mos definition-levels-length)
    (Bytes/writeUInt mos compressed-data-length)
    (Bytes/writeUInt mos uncompressed-data-length)))

(defn- read-data-page-header
  ^DataPageHeader [^ByteBuffer bb data-page-type]
  (DataPageHeader. (page-type->int data-page-type)
                   (Bytes/readUInt bb)
                   (Bytes/readUInt bb)
                   (Bytes/readUInt bb)
                   (Bytes/readUInt bb)
                   (Bytes/readUInt bb)))

(deftype DictionaryPageHeader [^long encoded-page-type
                               ^long num-values
                               ^long compressed-data-length
                               ^long uncompressed-data-length]
  IPageHeader
  (type [this]
    (int->page-type encoded-page-type))
  (headerLength [this]
    (+ (Bytes/getNumUIntBytes encoded-page-type)
       (Bytes/getNumUIntBytes num-values)
       (Bytes/getNumUIntBytes compressed-data-length)
       (Bytes/getNumUIntBytes uncompressed-data-length)))
  (bodyLength [_]
    compressed-data-length)
  (byteOffsetBody [_]
    0)
  (stats [this]
    (stats/map->PageStats
     {:num-values num-values
      :length (+ (.headerLength this) (.bodyLength this))
      :byte-stats (stats/map->ByteStats {:dictionary-header-length (.headerLength this)
                                         :dictionary-length compressed-data-length})}))
  IWriteable
  (writeTo [this mos]
    (Bytes/writeUInt mos encoded-page-type)
    (Bytes/writeUInt mos num-values)
    (Bytes/writeUInt mos compressed-data-length)
    (Bytes/writeUInt mos uncompressed-data-length)))

(defn- read-dictionary-page-header [^ByteBuffer bb dictionary-page-type]
  (DictionaryPageHeader. (page-type->int dictionary-page-type)
                         (Bytes/readUInt bb)
                         (Bytes/readUInt bb)
                         (Bytes/readUInt bb)))

(definterface IPageWriter
  (^long numValues []))

(definterface IDataPageWriter
  (^dendrite.page.IDataPageWriter write [striped-values]))

(definterface IDictionaryPageWriter
  (^dendrite.page.IDictionaryPageWriter writeEntry [entry]))

(definterface IPageWriterImpl
  (^dendrite.page.IPageHeader provisionalHeader [])
  (^dendrite.page.IPageHeader header []))

(deftype DataPageWriter
    [^Estimator body-length-estimator
     ^IEncoder repetition-level-encoder
     ^IEncoder definition-level-encoder
     ^IEncoder data-encoder
     ^ICompressor data-compressor
     ^{:unsynchronized-mutable true :tag boolean} finished?
     ^{:unsynchronized-mutable true :tag long} num-stripes]
  IDataPageWriter
  (write [this striped-values]
    (if repetition-level-encoder
      (doseq [repeated-values striped-values
              ^LeveledValue lv repeated-values]
        (let [v (.value lv)]
          (when-not (nil? v)
            (.encode data-encoder v)))
        (.encode repetition-level-encoder (.repetitionLevel lv))
        (.encode definition-level-encoder (.definitionLevel lv)))
      (if definition-level-encoder
        (doseq [v striped-values]
          (if (nil? v)
            (.encode definition-level-encoder (int 0))
            (do (.encode definition-level-encoder (int 1))
                (.encode data-encoder v))))
        (doseq [v striped-values]
          (.encode data-encoder v))))
    (set! num-stripes (+ num-stripes (count striped-values)))
    this)
  IPageWriter
  (numValues [_]
    num-stripes)
  IPageWriterImpl
  (provisionalHeader [this]
    (DataPageHeader. (page-type->int :data)
                     (.numValues this)
                     (if repetition-level-encoder (.estimatedLength repetition-level-encoder) 0)
                     (if definition-level-encoder (.estimatedLength definition-level-encoder) 0)
                     (.estimatedLength data-encoder)
                     (.estimatedLength data-encoder)))
  (header [this]
    (DataPageHeader. (page-type->int :data)
                     (.numValues this)
                     (if repetition-level-encoder (.length repetition-level-encoder) 0)
                     (if definition-level-encoder (.length definition-level-encoder) 0)
                     (if data-compressor (.length data-compressor) (.length data-encoder))
                     (.length data-encoder)))
  IOutputBuffer
  (reset [_]
    (set! finished? (boolean false))
    (set! num-stripes 0)
    (when repetition-level-encoder
      (.reset repetition-level-encoder))
    (when definition-level-encoder
      (.reset definition-level-encoder))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not finished?
      (let [estimated-body-length (-> this .provisionalHeader .bodyLength)]
        (when repetition-level-encoder
          (.finish repetition-level-encoder))
        (when definition-level-encoder
          (.finish definition-level-encoder))
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (.update body-length-estimator (-> this .header .bodyLength) estimated-body-length))
      (set! finished? (boolean true))))
  (length [this]
    (let [h (.header this)]
      (+ (.headerLength h) (.bodyLength h))))
  (estimatedLength [this]
    (let [provisional-header (.provisionalHeader this)]
      (+ (.headerLength provisional-header)
         (.correct body-length-estimator (.bodyLength provisional-header)))))
  (writeTo [this mos]
    (.finish this)
    (.writeTo ^IWriteable (.header this) mos)
    (when repetition-level-encoder
      (.writeTo repetition-level-encoder mos))
    (when definition-level-encoder
      (.writeTo definition-level-encoder mos))
    (if data-compressor
      (.writeTo data-compressor mos)
      (.writeTo data-encoder mos))))

(defn data-page-writer
  ^dendrite.page.DataPageWriter
  [max-repetition-level max-definition-level type-store value-type encoding compression]
  (DataPageWriter. (Estimator.)
                   (when (pos? max-repetition-level) (levels-encoder max-repetition-level))
                   (when (pos? max-definition-level) (levels-encoder max-definition-level))
                   (encoder type-store value-type encoding)
                   (compression/compressor compression)
                   false
                   0))

(deftype DictionaryPageWriter [^Estimator body-length-estimator
                               ^IEncoder data-encoder
                               ^ICompressor data-compressor
                               ^{:unsynchronized-mutable true :tag boolean} finished?]
  IDictionaryPageWriter
  (writeEntry [this v]
    (.encode data-encoder v)
    this)
  IPageWriter
  (numValues [_] (.numEncodedValues data-encoder))
  IPageWriterImpl
  (provisionalHeader [this]
    (DictionaryPageHeader. (page-type->int :dictionary)
                           (.numValues this)
                           (.estimatedLength data-encoder)
                           (.estimatedLength data-encoder)))
  (header [this]
    (DictionaryPageHeader. (page-type->int :dictionary)
                           (.numValues this)
                           (if data-compressor (.length data-compressor) (.length data-encoder))
                           (.length data-encoder)))
  IOutputBuffer
  (reset [_]
    (set! finished? (boolean false))
    (.reset data-encoder)
    (when data-compressor
      (.reset data-compressor)))
  (finish [this]
    (when-not finished?
      (let [estimated-body-length (-> this .provisionalHeader .bodyLength)]
        (.finish data-encoder)
        (when data-compressor
          (.compress data-compressor data-encoder))
        (.update body-length-estimator (-> this .header .bodyLength) estimated-body-length))
      (set! finished? (boolean true))))
  (length [this]
    (let [h (.header this)]
      (+ (.headerLength h) (.bodyLength h))))
  (estimatedLength [this]
    (let [provisional-header (.provisionalHeader this)]
      (+ (.headerLength provisional-header)
         (.correct body-length-estimator (.bodyLength provisional-header)))))
  (writeTo [this mos]
    (.finish this)
    (.writeTo ^IWriteable (.header this) mos)
    (if data-compressor
      (.writeTo data-compressor mos)
      (.writeTo data-encoder mos))))

(defn dictionary-page-writer
  ^dendrite.page.DictionaryPageWriter
  [type-store value-type encoding compression]
  (DictionaryPageWriter. (Estimator.)
                         (encoder type-store value-type encoding)
                         (compression/compressor compression)
                         false))

(definterface IDataPageReader
  (read [])
  (read [map-fn])
  (skip []))

(deftype DataPageReader [^ByteBuffer byte-buffer
                         ^long max-repetition-level
                         ^long max-definition-level
                         data-decoder-ctor
                         decompressor-ctor
                         ^DataPageHeader header]
  IDataPageReader
  (read [this]
    (.read this nil))
  (read [this map-fn]
    (let [repetition-levels-decoder (when (.hasRepetitionLevels header)
                                      (-> byte-buffer
                                          (utils/skip (.byteOffsetRepetitionLevels header))
                                          (levels-decoder max-repetition-level)))
          definition-levels-decoder (when (.hasDefinitionLevels header)
                                      (-> byte-buffer
                                          (utils/skip (.byteOffsetDefinitionLevels header))
                                          (levels-decoder max-definition-level)))
          data-byte-buffer (utils/skip byte-buffer (.byteOffsetBody header))
          data-byte-buffer (if-let [^IDecompressor decompressor (decompressor-ctor)]
                             (.decompress decompressor
                                          data-byte-buffer
                                          (.compressed-data-length header)
                                          (.uncompressed-data-length header))
                             data-byte-buffer)
          data-decoder (data-decoder-ctor data-byte-buffer)]
      (LeveledValues/read repetition-levels-decoder
                          definition-levels-decoder
                          data-decoder
                          max-definition-level
                          map-fn)))
  (skip [_]
    (utils/skip byte-buffer (.bodyLength header))))

(defn data-page-reader
  ^dendrite.page.DataPageReader
  [^ByteBuffer byte-buffer max-repetition-level max-definition-level type-store value-type encoding
   compression]
  (let [bb (.duplicate byte-buffer)
        page-type (read-next-data-page-type bb)]
    (DataPageReader. bb
                     max-repetition-level
                     max-definition-level
                     (decoder-ctor type-store value-type encoding)
                     (compression/decompressor-ctor compression)
                     (read-data-page-header bb page-type))))

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
               (data-page-readers (.skip next-data-page-reader) (dec num-data-pages) max-repetition-level
                                  max-definition-level type-store value-type encoding compression)))))))

(defn read-data-pages
  [^ByteBuffer byte-buffer num-data-pages max-repetition-level max-definition-level type-store value-type
   encoding compression map-fn]
  (utils/pmap-1 #(.read ^DataPageReader % map-fn)
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
             next-byte-array-reader (utils/skip bb (.bodyLength next-header))]
         (cons next-header (read-data-page-headers next-byte-array-reader (dec num-data-pages))))))))

(definterface IDictionaryPageReader
  (readArray [])
  (readArray [map-fn]))

(deftype DictionaryPageReader [^ByteBuffer byte-buffer
                               data-decoder-ctor
                               decompressor-ctor
                               ^DictionaryPageHeader header]
  IDictionaryPageReader
  (readArray [this]
    (.readArray this nil))
  (readArray [_ map-fn]
    (let [data-byte-buffer (utils/skip byte-buffer (.byteOffsetBody header))
          data-byte-buffer (if-let [^IDecompressor decompressor (decompressor-ctor)]
                             (.decompress decompressor
                                          data-byte-buffer
                                          (.compressed-data-length header)
                                          (.uncompressed-data-length header))
                              data-byte-buffer)]
      (DictionaryValues/read (data-decoder-ctor data-byte-buffer) map-fn))))

(defn dictionary-page-reader
  ^dendrite.page.DictionaryPageReader
  [^ByteBuffer byte-buffer type-store value-type encoding compression]
  (let [bb (.duplicate byte-buffer)
        page-type (read-next-dictionary-page-type bb)]
    (DictionaryPageReader. bb
                           (decoder-ctor type-store value-type encoding)
                           (compression/decompressor-ctor compression)
                           (read-dictionary-page-header bb page-type))))

(defn read-dictionary [^ByteBuffer byte-buffer type-store value-type encoding compression map-fn]
  (.readArray (dictionary-page-reader byte-buffer type-store value-type encoding compression) map-fn))

(defn read-dictionary-header [^ByteBuffer byte-buffer]
  (let [bb (.duplicate byte-buffer)
        page-type (read-next-dictionary-page-type bb)]
    (read-dictionary-page-header bb page-type)))
