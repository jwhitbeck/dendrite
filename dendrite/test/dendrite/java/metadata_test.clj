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
  (:import [dendrite.java ColumnChunkMetadata ColumnSpec Field FileMetadata MemoryOutputStream
            RecordGroupMetadata]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn rand-column-spec []
  (ColumnSpec. (rand-nth [:int :long :string]) (rand-int 10) (rand-int 2) (rand-int 10) 0
               (rand-int 10) (rand-int 10) nil nil))

(deftest column-spec
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-column-specs (repeatedly 100 rand-column-spec)]
      (doseq [^ColumnSpec cs rand-column-specs]
        (.writeTo cs mos))
      (let [bb (.byteBuffer mos)
            read-column-specs (repeatedly 100 #(ColumnSpec/read bb))]
        (is (= read-column-specs rand-column-specs)))))
  (testing "nil column-specs"
    (let [mos (MemoryOutputStream.)]
      (ColumnSpec/writeNullTo mos)
      (is (nil? (ColumnSpec/read (.byteBuffer mos)))))))

(defn rand-field
  ([] (rand-field 5))
  ([max-depth]
   (let [subfields (when (pos? max-depth)
                     (seq (repeatedly (rand-int 3) #(rand-field (dec max-depth)))))
         field-name (rand-nth [:foo :bar :baz])]
     (if (seq subfields)
       (Field. field-name (rand-int 4) (rand-int 3) (rand-int 3) nil subfields)
       (Field. field-name (rand-int 4) (rand-int 3) (rand-int 3) (rand-column-spec) nil)))))

(deftest field
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-fields (repeatedly 100 rand-field)]
      (doseq [^Field field rand-fields]
        (.writeTo field mos))
      (let [bb (.byteBuffer mos)
            read-fields (repeatedly 100 #(Field/read bb))]
        (is (= read-fields rand-fields))))))

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
                        (seq (repeatedly (rand-int 10) rand-column-chunk-metadata))))

(deftest record-group-metadata
  (testing "serialization-deserialization"
    (let [mos (MemoryOutputStream.)
          rand-record-group-metadatas (repeatedly 100 rand-record-group-metadata)]
      (doseq [^RecordGroupMetadata record-group-metadata rand-record-group-metadatas]
        (.writeTo record-group-metadata mos))
      (let [bb (.byteBuffer mos)
            read-record-group-metadatas (repeatedly 100 #(RecordGroupMetadata/read bb))]
        (is (= read-record-group-metadatas rand-record-group-metadatas))))))

(defn rand-custom-types []
  (let [base-types [:int :long :string]
        custom-types [:foo :bar :baz]]
    (some->> (map vector (repeatedly #(rand-nth custom-types)) (repeatedly #(rand-nth base-types)))
             (take (rand-int 4))
             seq
             (into {}))))

(defn rand-file-metadata []
  (FileMetadata. (seq (repeatedly (rand-int 5) rand-record-group-metadata))
                 (rand-field)
                 (rand-custom-types)
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
