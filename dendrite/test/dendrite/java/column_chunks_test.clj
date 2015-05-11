;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.column-chunks-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers :refer [leveled partition-by-record]]
            [dendrite.utils :as utils])
  (:import [dendrite.java LeveledValue ColumnChunks ChunkedPersistentList DataColumnChunk$Reader
            DataColumnChunk$Writer IColumnChunkReader IColumnChunkWriter IPageHeader
            OptimizingColumnChunkWriter Schema$Column Types]
           [java.util Date Calendar]
           [java.text SimpleDateFormat]))

(set! *warn-on-reflection* true)

(def test-target-data-page-length 1000)

(def ^Types types (Types/create nil nil))

(defn write-column-chunk-and-get-reader
  (^IColumnChunkReader
   [column input-values]
   (write-column-chunk-and-get-reader column test-target-data-page-length types input-values))
  (^IColumnChunkReader
   [column target-data-page-length types input-values]
   (let [w (ColumnChunks/createWriter types column target-data-page-length)]
     (.write w input-values)
     (ColumnChunks/createReader types (.byteBuffer w) (.metadata w) column))))

(defn- write-optimized-column-chunk-and-get-reader
  (^IColumnChunkReader
   [column input-values]
   (write-optimized-column-chunk-and-get-reader column {'deflate 10.0} input-values))
  (^IColumnChunkReader
   [column compression-thresholds input-values]
   (let [w (OptimizingColumnChunkWriter/create types column test-target-data-page-length)]
     (.write w input-values)
     (let [opt-w (.optimize w types compression-thresholds)]
       (ColumnChunks/createReader types (.byteBuffer opt-w) (.metadata opt-w) (.column opt-w))))))

(defn- column-repeated ^Schema$Column [type encoding compression]
  (Schema$Column. 0 2 3 type encoding compression 0 nil))

(defn- column-non-repeated ^Schema$Column [type encoding compression]
  (Schema$Column. 0 0 3 type encoding compression 0 nil))

(defn- column-required ^Schema$Column [type encoding compression]
  (Schema$Column. 0 0 0 type encoding compression 0 nil))

(defn- as-chunked-list [coll]
  (persistent! (reduce conj! (ChunkedPersistentList/newEmptyTransient) coll)))

(defn- rand-repeated-values [^Schema$Column column n coll]
  (->> coll
       (leveled {:max-definition-level (.definitionLevel column)
                 :max-repetition-level (.repetitionLevel column)})
       partition-by-record
       (take n)
       (map as-chunked-list)
       as-chunked-list))

(deftest data-column-chunk
  (let [column (column-repeated Types/INT Types/PLAIN Types/DEFLATE)
        input-values (->> (repeatedly #(helpers/rand-int-bits 10)) (rand-repeated-values column 1000))
        reader (write-column-chunk-and-get-reader column input-values)
        output-values (utils/flatten-1 (.readPartitioned reader 100))]
    (testing "write/read a data colum-chunk"
      (is (= (-> reader .metadata .numDataPages) 4))
      (is (= input-values output-values)))
    (testing "value mapping"
      (let [^clojure.lang.IFn f (fnil (partial * 2) 1)
            reader-with-f (write-column-chunk-and-get-reader (.withFn column f) input-values)]
        (is (= (map (partial helpers/map-leveled f) input-values)
               (-> reader-with-f (.readPartitioned 100) utils/flatten-1)))))
    (testing "repeatable writes"
      (let [w (ColumnChunks/createWriter types column test-target-data-page-length)]
        (.write w input-values)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (.readPartitioned reader 100) (.readPartitioned reader 100))))
    (testing "Page length estimation converges"
      (letfn [(avg-page-length [target-length]
                (let [reader (write-column-chunk-and-get-reader column target-length types input-values)]
                  (->> (.getPageHeaders reader)
                       rest    ; the first page is always inaccurate
                       butlast ; the last page can have any length
                       (map (comp :length #(.stats ^IPageHeader %)))
                       helpers/avg
                       double)))]
        (is (helpers/roughly 1024 (avg-page-length 1024)))
        (is (helpers/roughly 256 (avg-page-length 256)))))))

(deftest dictionary-column-chunk
  (let [column (column-repeated Types/INT Types/DICTIONARY Types/DEFLATE)
        input-values (->> (repeatedly #(helpers/rand-int-bits 10)) (rand-repeated-values column 1000))
        reader (write-column-chunk-and-get-reader column input-values)
        output-values (utils/flatten-1 (.readPartitioned reader 100))]
    (testing "write/read a dictionary colum-chunk"
      (is (= input-values output-values)))
    (testing "value mapping"
      (let [^clojure.lang.IFn f #(if % (int (* 2 %)) %)
            reader-with-f (write-column-chunk-and-get-reader (.withFn column f) input-values)]
        (is (= (map (partial helpers/map-leveled f) input-values)
               (-> reader-with-f (.readPartitioned 100) utils/flatten-1)))))
    (testing "repeatable writes"
      (let [w (ColumnChunks/createWriter types column test-target-data-page-length)]
        (.write w input-values)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (.readPartitioned reader 100) (.readPartitioned reader 100))))))


(deftest frequency-column-chunk
  (let [column (column-repeated Types/INT Types/FREQUENCY Types/DEFLATE)
        input-values (->> (repeatedly #(helpers/rand-int-bits 10)) (rand-repeated-values column 1000))
        reader (write-column-chunk-and-get-reader column input-values)
        output-values (utils/flatten-1 (.readPartitioned reader 100))]
    (testing "write/read a frequency colum-chunk"
      (is (= input-values output-values)))
    (testing "value mapping"
      (let [^clojure.lang.IFn f #(if % (int (* 2 %)) %)
            reader-with-f (write-column-chunk-and-get-reader (.withFn column f) input-values)]
        (is (= (map (partial helpers/map-leveled f) input-values)
               (-> reader-with-f (.readPartitioned 100) utils/flatten-1)))))
    (testing "repeatable writes"
      (let [w (ColumnChunks/createWriter types column test-target-data-page-length)]
        (.write w input-values)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (.readPartitioned reader 100) (.readPartitioned reader 100))))))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [column (column-required Types/BOOLEAN Types/PLAIN Types/NONE)
          input-values (as-chunked-list (repeatedly 1000 helpers/rand-bool))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (utils/flatten-1 (.readPartitioned reader 100))]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "mostly true booleans"
    (let [column (column-required Types/BOOLEAN Types/PLAIN Types/NONE)
          input-values (as-chunked-list (repeatedly 1000 #(helpers/rand-biased-bool 0.98)))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (utils/flatten-1 (.readPartitioned reader 100))]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding))))))
