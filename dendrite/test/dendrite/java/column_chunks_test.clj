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
            [dendrite.test-helpers :as helpers :refer [leveled partition-by-record flatten-1]])
  (:import [dendrite.java LeveledValue ColumnChunks ChunkedPersistentList DataColumnChunk$Reader
            DataColumnChunk$Writer IColumnChunkReader IColumnChunkWriter IPageHeader
            OptimizingColumnChunkWriter Schema$Column Types]
           [java.util Date Calendar]
           [java.text SimpleDateFormat]))

(set! *warn-on-reflection* true)

(def test-target-data-page-length (* 128 1024))

(def ^Types types (Types/create))

(defn write-column-chunk-and-get-reader
  (^IColumnChunkReader
   [^Schema$Column column input-values]
   (write-column-chunk-and-get-reader column test-target-data-page-length types input-values))
  (^IColumnChunkReader
   [^Schema$Column column target-data-page-length types input-values]
   (let [w (ColumnChunks/createWriter types column target-data-page-length)]
     (.write w input-values)
     (ColumnChunks/createReader types (.byteBuffer w) (.metadata w) column 100))))

(defn read-flat [^IColumnChunkReader reader])

(defn- write-optimized-column-chunk-and-get-reader
  (^IColumnChunkReader
   [column input-values]
   (write-optimized-column-chunk-and-get-reader column types {'deflate 10.0} input-values))
  (^IColumnChunkReader
   [column custom-types compression-thresholds input-values]
   (let [w (OptimizingColumnChunkWriter/create custom-types column test-target-data-page-length)]
     (.write w input-values)
     (let [opt-w (.optimize w compression-thresholds)]
       (ColumnChunks/createReader custom-types (.byteBuffer opt-w) (.metadata opt-w) (.column opt-w) 100)))))

(defn- column-repeated ^Schema$Column [type encoding compression]
  (Schema$Column. 0 2 3 type encoding compression 0 -1 nil))

(defn- column-non-repeated ^Schema$Column [type encoding compression]
  (Schema$Column. 0 0 3 type encoding compression 0 -1 nil))

(defn- column-required ^Schema$Column [type encoding compression]
  (Schema$Column. 0 0 0 type encoding compression 0 -1 nil))

(defn- rand-repeated-values [^Schema$Column column n coll]
  (->> coll
       (leveled {:max-definition-level (.definitionLevel column)
                 :max-repetition-level (.repetitionLevel column)})
       partition-by-record
       (take n)))

(def ^SimpleDateFormat simple-date-format (SimpleDateFormat. "yyyy-MM-dd"))

(defn- iterate-calendar-by-day [^Calendar calendar]
  (lazy-seq (cons (.getTime calendar)
                  (iterate-calendar-by-day (doto calendar (.add Calendar/DATE 1))))))

(defn- days-seq [start-date-str]
  (-> (doto (Calendar/getInstance)
        (.setTime (.parse ^SimpleDateFormat simple-date-format start-date-str)))
      iterate-calendar-by-day))

(deftest data-column-chunk
  (let [column (column-repeated Types/INT Types/PLAIN Types/DEFLATE)
        input-values (->> (repeatedly #(helpers/rand-int-bits 10)) (rand-repeated-values column 1000))
        reader (write-column-chunk-and-get-reader column input-values)
        output-values (flatten-1 reader)]
    (testing "write/read a data colum-chunk"
      (is (= input-values output-values)))
    (testing "value mapping"
      (let [^clojure.lang.IFn f (fnil (partial * 2) 1)
            reader-with-f (write-column-chunk-and-get-reader (.withFn column f) input-values)]
        (is (= (map (partial helpers/map-leveled f) input-values)
               (flatten-1 reader-with-f)))))
    (testing "repeatable writes"
      (let [w (ColumnChunks/createWriter types column test-target-data-page-length)]
        (.write w input-values)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (seq reader) (seq reader))))
    (testing "Page length estimation converges"
      (letfn [(avg-page-length [target-length]
                (let [reader (write-column-chunk-and-get-reader column target-length types input-values)]
                  (->> (.getPageHeaders reader)
                       rest    ; the first page is always inaccurate
                       butlast ; the last page can have any length
                       (map (fn [^IPageHeader h]
                              (.length (.stats h))))
                       helpers/avg
                       double)))]
        (is (helpers/roughly 1024 (avg-page-length 1024)))
        (is (helpers/roughly 256 (avg-page-length 256)))))))

(deftest dictionary-column-chunk
  (let [column (column-repeated Types/INT Types/DICTIONARY Types/DEFLATE)
        input-values (->> (repeatedly #(helpers/rand-int-bits 10)) (rand-repeated-values column 1000))
        reader (write-column-chunk-and-get-reader column input-values)
        output-values (flatten-1 reader)]
    (testing "write/read a dictionary colum-chunk"
      (is (= input-values output-values)))
    (testing "value mapping"
      (let [^clojure.lang.IFn f #(if % (int (* 2 %)) %)
            reader-with-f (write-column-chunk-and-get-reader (.withFn column f) input-values)]
        (is (= (map (partial helpers/map-leveled f) input-values)
               (flatten-1 reader-with-f)))))
    (testing "repeatable writes"
      (let [w (ColumnChunks/createWriter types column test-target-data-page-length)]
        (.write w input-values)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (seq reader) (seq reader))))))

(deftest frequency-column-chunk
  (let [column (column-repeated Types/INT Types/FREQUENCY Types/DEFLATE)
        input-values (->> (repeatedly #(helpers/rand-int-bits 10)) (rand-repeated-values column 1000))
        reader (write-column-chunk-and-get-reader column input-values)
        output-values (flatten-1 reader)]
    (testing "write/read a frequency colum-chunk"
      (is (= input-values output-values)))
    (testing "value mapping"
      (let [^clojure.lang.IFn f #(if % (int (* 2 %)) %)
            reader-with-f (write-column-chunk-and-get-reader (.withFn column f) input-values)]
        (is (= (map (partial helpers/map-leveled f) input-values)
               (flatten-1 reader-with-f)))))
    (testing "repeatable writes"
      (let [w (ColumnChunks/createWriter types column test-target-data-page-length)]
        (.write w input-values)
        (let [bb1 (helpers/output-buffer->byte-buffer w)
              bb2 (helpers/output-buffer->byte-buffer w)]
          (is (= (-> bb1 .array seq) (-> bb2 .array seq))))))
    (testing "repeatable reads"
      (is (= (seq reader) (seq reader))))))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [column (column-required Types/BOOLEAN Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-bool)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "mostly true booleans"
    (let [column (column-required Types/BOOLEAN Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-biased-bool 0.98))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding))))))

(deftest find-best-int-encodings
  (testing "random ints"
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-int)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "random ints (non-repeated)"
    (let [column (column-non-repeated Types/INT Types/PLAIN Types/NONE)
          input-values (->> (repeatedly helpers/rand-int)
                            (helpers/rand-map 0.2 (constantly nil))
                            (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "random ints (repeated)"
    (let [column (column-repeated Types/INT Types/PLAIN Types/NONE)
          input-values (rand-repeated-values column 100 (repeatedly helpers/rand-int))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "random small ints"
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-int-bits 10))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PACKED_RUN_LENGTH (-> reader .column .encoding)))))
  (testing "increasing ints"
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          input-values (->> (range) (map int) (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DELTA (-> reader .column .encoding)))))
  (testing "small selection of random ints"
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          random-ints (repeatedly 100 helpers/rand-int)
          input-values (repeatedly 1000 #(rand-nth random-ints))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding)))))
  (testing "skewed selection of random ints"
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          input-values (concat (repeatedly 255 helpers/rand-int)
                               (apply interleave (repeatedly 10 #(repeat 100 (helpers/rand-int)))))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/FREQUENCY (-> reader .column .encoding)))))
  (testing "small random unsigned ints with an occasional large one."
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          input-values (->> (repeatedly #(helpers/rand-int-bits 7))
                            (helpers/rand-map 0.1 (constantly (helpers/rand-int-bits 24)))
                            (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/VLQ (-> reader .column .encoding)))))
  (testing "small random signed ints with an occasional large one."
    (let [column (column-required Types/INT Types/PLAIN Types/NONE)
          input-values (->> (repeatedly #(helpers/rand-int-bits 7))
                            (helpers/rand-map 0.1 (constantly (helpers/rand-int-bits 24)))
                            (map * (repeatedly helpers/rand-sign))
                            (map unchecked-int)
                            (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/ZIG_ZAG (-> reader .column .encoding)))))
  (testing "small selection of chars"
    (let [column (column-required Types/CHAR Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(rand-nth [\c \return \u1111]))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding))))))


(deftest find-best-long-encodings
  (testing "random longs"
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-long)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "random small longs"
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-long-bits 10))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DELTA (-> reader .column .encoding)))))
  (testing "small random unsigned longs with an occasional large one."
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (->> (repeatedly #(helpers/rand-long-bits 7))
                            (helpers/rand-map 0.1 (constantly (helpers/rand-long-bits 24)))
                            (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/VLQ (-> reader .column .encoding)))))
  (testing "small random signed longs with an occasional large one."
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (->> (repeatedly #(helpers/rand-long-bits 7))
                            (helpers/rand-map 0.1 (constantly (helpers/rand-long-bits 24)))
                            (map * (repeatedly helpers/rand-sign))
                            (map unchecked-long)
                            (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/ZIG_ZAG (-> reader .column .encoding)))))
  (testing "increasing longs"
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (->> (range) (map long) (take 1000))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DELTA (-> reader .column .encoding)))))
  (testing "increasing timestamps"
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(System/nanoTime))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DELTA (-> reader .column .encoding)))))
  (testing "incrementing dates as a custom-type"
    (let [column (column-required Types/INST Types/PLAIN Types/NONE)
          input-values (take 1000 (days-seq "2014-01-01"))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DELTA (-> reader .column .encoding)))))
  (testing "small selection of random longs"
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          random-longs (repeatedly 100 helpers/rand-long)
          input-values (repeatedly 1000 #(rand-nth random-longs))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding)))))
  (testing "skewed selection of random longs"
    (let [column (column-required Types/LONG Types/PLAIN Types/NONE)
          input-values (concat (repeatedly 255 helpers/rand-long)
                               (apply interleave (repeatedly 10 #(repeat 100 (helpers/rand-long)))))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/FREQUENCY (-> reader .column .encoding))))))

(deftest find-best-float-encodings
  (testing "random floats"
    (let [column (column-required Types/FLOAT Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-float)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "small selection of random floats"
    (let [column (column-required Types/FLOAT Types/PLAIN Types/NONE)
          random-floats (repeatedly 100 helpers/rand-float)
          input-values (repeatedly 1000 #(rand-nth random-floats))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding)))))
  (testing "skewed selection of random floats"
    (let [column (column-required Types/FLOAT Types/PLAIN Types/NONE)
          input-values (concat (repeatedly 255 helpers/rand-float)
                               (apply interleave (repeatedly 10 #(repeat 100 (helpers/rand-float)))))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/FREQUENCY (-> reader .column .encoding))))))

(deftest find-best-double-encodings
  (testing "random doubles"
    (let [column (column-required Types/DOUBLE Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-double)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "small selection of random doubles"
    (let [column (column-required Types/DOUBLE Types/PLAIN Types/NONE)
          random-doubles (repeatedly 100 helpers/rand-double)
          input-values (repeatedly 1000 #(rand-nth random-doubles))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding)))))
  (testing "skewed selection of random doubles"
    (let [column (column-required Types/DOUBLE Types/PLAIN Types/NONE)
          input-values (concat (repeatedly 255 helpers/rand-double)
                               (apply interleave (repeatedly 10 #(repeat 100 (helpers/rand-double)))))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/FREQUENCY (-> reader .column .encoding))))))

(deftest find-best-byte-array-encodings
  (testing "random byte arrays"
    (let [column (column-required Types/BYTE_ARRAY Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-byte-array)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= (map seq output-values) (map seq input-values)))
      (is (= Types/DELTA_LENGTH (-> reader .column .encoding)))))
  (testing "random byte buffers"
    (let [column (column-required Types/BYTE_BUFFER Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-byte-buffer)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= (map helpers/byte-buffer->seq output-values) (map helpers/byte-buffer->seq input-values)))
      (is (= Types/DELTA_LENGTH (-> reader .column .encoding)))))
  (testing "random big ints"
    (let [column (column-required Types/BIGINT Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-bigint 100))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DELTA_LENGTH (-> reader .column .encoding)))))
  (testing "random big decimals"
    (let [column (column-required Types/BIGDEC Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-bigdec 40))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/INCREMENTAL (-> reader .column .encoding)))))
  (testing "random ratios"
    (let [column (column-required Types/RATIO Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-ratio 40))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/INCREMENTAL (-> reader .column .encoding)))))
  (testing "incrementing dates"
    (let [custom-types (Types/create {'date-str {:base-type 'string
                                                 :to-base-type-fn #(locking simple-date-format
                                                                     (.format simple-date-format %))
                                                 :from-base-type-fn #(locking simple-date-format
                                                                       (.parse simple-date-format %))}})
          column (column-required (.getType custom-types 'date-str) Types/PLAIN Types/NONE)
          input-values (take 1000 (days-seq "2014-01-01"))
          reader (write-optimized-column-chunk-and-get-reader column custom-types {} input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/INCREMENTAL (-> reader .column .encoding)))))
  (testing "small set of keywords"
    (let [column (column-required Types/KEYWORD Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(rand-nth [:foo ::bar :baz]))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding)))))
  (testing "small set of symbols"
    (let [column (column-required Types/SYMBOL Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(rand-nth ['foo 'bar 'baz]))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/DICTIONARY (-> reader .column .encoding))))))

(deftest find-best-fixed-length-byte-array-encodings
  (testing "random byte arrays"
    (let [column (column-required Types/FIXED_LENGTH_BYTE_ARRAY Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 #(helpers/rand-byte-array 16))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= (map seq output-values) (map seq input-values)))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "UUIDs"
    (let [column (column-required Types/UUID Types/PLAIN Types/NONE)
          input-values (repeatedly 1000 helpers/rand-uuid)
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= output-values input-values))
      (is (= Types/PLAIN (-> reader .column .encoding)))))
  (testing "small selection of random byte arrays"
    (let [column (column-required Types/FIXED_LENGTH_BYTE_ARRAY Types/PLAIN Types/NONE)
          rand-byte-arrays (repeatedly 10 #(helpers/rand-byte-array 16))
          input-values (repeatedly 1000 #(rand-nth rand-byte-arrays))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= (map seq output-values) (map seq input-values)))
      (is (= Types/DICTIONARY (-> reader .column .encoding)))))
  (testing "skewed selection of random byte arrays"
    (let [column (column-required Types/FIXED_LENGTH_BYTE_ARRAY Types/PLAIN Types/NONE)
          input-values (concat (repeatedly 255 #(helpers/rand-byte-array 10))
                               (apply interleave (repeatedly 10 #(repeat 100 (helpers/rand-byte-array 10)))))
          reader (write-optimized-column-chunk-and-get-reader column input-values)
          output-values (flatten-1 reader)]
      (is (= (map seq output-values) (map seq input-values)))
      (is (= Types/FREQUENCY (-> reader .column .encoding))))))
