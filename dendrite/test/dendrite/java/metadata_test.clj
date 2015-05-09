;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.metadata-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ColumnChunkMetadata CustomType FileMetadata MemoryOutputStream
            RecordGroupMetadata Schema Schema$Column Schema$Field Schema$Record Schema$Collection]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn rand-name [] (rand-nth [:foo :bar :baz]))

(declare rand-schema)

(defn rand-schema-column []
  (Schema$Column. (rand-int 10) (rand-int 10) (rand-int 10) (- (rand-int 10) 4)
                  (rand-int 10) (rand-int 10) (rand-int 100) nil))

(defn rand-field [max-depth]
  (Schema$Field. (rand-name) (rand-schema (dec max-depth))))

(defn rand-schema-record [max-depth]
  (Schema$Record. (rand-int 10) (rand-int 10) (rand-int 10) (rand-int 10)
                  (repeatedly (rand-int 3) #(rand-field max-depth))
                  nil))

(defn rand-schema-coll [max-depth]
  (Schema$Collection. (rand-int 10) (rand-int 10) (rand-int 10) (rand-int 10)
                      (rand-schema (dec max-depth)) nil))

(defn rand-schema
  ([] (rand-schema 5))
  ([max-depth]
   (if (zero? max-depth)
     (rand-schema-column)
     (case (int (rand-int 3))
       0 (rand-schema-column)
       1 (rand-schema-record max-depth)
       2 (rand-schema-coll max-depth)))))

(deftest schemas
  (let [mos (MemoryOutputStream.)
        rand-schemas (repeatedly 100 rand-schema)]
    (doseq [^Schema schema rand-schemas]
      (.write mos schema))
    (let [bb (.byteBuffer mos)
          read-schemas (repeatedly 100 #(Schema/read bb))]
      (is (= read-schemas rand-schemas)))))

(defn rand-column-chunk-metadata []
  (ColumnChunkMetadata. (rand-int 1024) (rand-int 10) (rand-int 128) (rand-int 128)))

(deftest column-chunk-metadata
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-column-chunk-metadatas (repeatedly 100 rand-column-chunk-metadata)]
      (doseq [^ColumnChunkMetadata column-chunk-metadata rand-column-chunk-metadatas]
        (.writeTo column-chunk-metadata mos))
      (let [bb (.byteBuffer mos)
            read-column-chunk-metadatas (repeatedly 100 #(ColumnChunkMetadata/read bb))]
        (is (= read-column-chunk-metadatas rand-column-chunk-metadatas))))))

(defn rand-record-group-metadata []
  (RecordGroupMetadata. (rand-int (* 1024 1024))
                        (rand-int 10000)
                        (repeatedly (rand-int 10) rand-column-chunk-metadata)))

(deftest record-group-metadata
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-record-group-metadatas (repeatedly 100 rand-record-group-metadata)]
      (doseq [^RecordGroupMetadata record-group-metadata rand-record-group-metadatas]
        (.writeTo record-group-metadata mos))
      (let [bb (.byteBuffer mos)
            read-record-group-metadatas (repeatedly 100 #(RecordGroupMetadata/read bb))]
        (is (= read-record-group-metadatas rand-record-group-metadatas))))))

(defn rand-custom-type []
  (CustomType. (rand-int 100) (rand-int 100) (rand-nth ['foo 'bar 'baz])))

(deftest custom-type
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-custom-types (repeatedly 100 rand-custom-type)]
      (doseq [^CustomType custom-type rand-custom-types]
        (.writeTo custom-type mos))
      (let [bb (.byteBuffer mos)
            read-custom-types (repeatedly 100 #(CustomType/read bb))]
        (is (= read-custom-types rand-custom-types))))))

(defn rand-file-metadata []
  (FileMetadata. (repeatedly (rand-int 5) rand-record-group-metadata)
                 (rand-schema)
                 (repeatedly (rand-int 5) rand-custom-type)
                 (helpers/rand-byte-buffer)))

(deftest file-metadata
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-file-metadatas (repeatedly 100 rand-file-metadata)]
      (doseq [^FileMetadata file-metadata rand-file-metadatas]
        (.writeTo file-metadata mos))
      (let [bb (.byteBuffer mos)
            read-file-metadatas (repeatedly 100 #(FileMetadata/read bb))]
        (is (= read-file-metadatas rand-file-metadatas))))))
