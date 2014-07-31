(ns dendrite.column-chunk-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.column-chunk :refer :all]
            [dendrite.encoding :as encoding]
            [dendrite.page :as page]
            [dendrite.metadata :as metadata]
            [dendrite.stats :as stats]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayWriter LeveledValue]
           [java.util Date Calendar]
           [java.text SimpleDateFormat])
  (:refer-clojure :exclude [read]))

(def test-target-data-page-length 1000)

(defn write-blocks [column-chunk-writer blocks]
  (reduce write! column-chunk-writer blocks))

(defn write-column-chunk-and-get-reader
  ([column-spec input-blocks]
     (write-column-chunk-and-get-reader column-spec test-target-data-page-length input-blocks))
  ([column-spec target-data-page-length input-blocks]
     (let [w (doto (writer target-data-page-length column-spec)
               (write-blocks input-blocks)
               .finish)
           column-chunk-metadata (metadata w)]
       (-> w
           helpers/get-byte-array-reader
           (reader column-chunk-metadata column-spec)))))

(def simple-date-format (SimpleDateFormat. "yyyy-MM-dd"))

(defn- iterate-calendar-by-day [calendar]
  (lazy-seq (cons (.getTime calendar)
                  (iterate-calendar-by-day (doto calendar (.add Calendar/DATE 1))))))

(defn- days-seq [start-date-str]
  (-> (doto (Calendar/getInstance)
        (.setTime (.parse simple-date-format start-date-str)))
      iterate-calendar-by-day))

(defn- column-spec [value-type encoding compression]
  (metadata/map->ColumnSpec
   {:type value-type
    :encoding encoding
    :compression compression
    :max-definition-level 3
    :max-repetition-level 2}))

(defn- column-spec-no-levels [value-type encoding compression]
  (metadata/map->ColumnSpec
   {:type value-type
    :encoding encoding
    :compression compression
    :max-definition-level 0
    :max-repetition-level 0}))

(defn partition-by-record [leveled-values]
  (lazy-seq
   (when-let [coll (seq leveled-values)]
     (let [fst (first coll)
           [keep remaining] (split-with #(-> ^LeveledValue % .repetitionLevel pos?) (next coll))]
       (cons (cons fst keep) (partition-by-record remaining))))))

(defn- rand-blocks [column-spec coll]
  (->> coll (helpers/leveled column-spec) partition-by-record))

(deftest data-column-chunk
  (let [cs (column-spec :int :plain :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 1100))
        reader (write-column-chunk-and-get-reader cs input-blocks)
        num-pages (-> reader :column-chunk-metadata :num-data-pages)
        output-blocks (read reader)]
    (testing "write/read a data colum-chunk"
      (is (helpers/roughly num-pages 4))
      (is (= input-blocks output-blocks)))
    (testing "value mapping"
        (let [map-fn (partial * 2)
              mapped-reader (write-column-chunk-and-get-reader (assoc cs :map-fn map-fn) input-blocks)]
          (is (= (->> input-blocks flatten (map #(some-> (.value ^LeveledValue %) map-fn)))
                 (->> mapped-reader read flatten (map #(.value ^LeveledValue %)))))))
    (testing "repeatable writes"
      (let [w (doto (writer test-target-data-page-length cs)
                (write-blocks input-blocks))
            baw1 (doto (ByteArrayWriter. 10) (.write w))
            baw2 (doto (ByteArrayWriter. 10) (.write w))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatable reads"
      (is (= (read reader) (read reader))))
    (testing "Page length estimation converges"
      (letfn [(avg-page-length [target-length]
                (let [reader (write-column-chunk-and-get-reader cs target-length input-blocks)
                      num-data-pages (-> reader :column-chunk-metadata :num-data-pages)]
                  (->> (page/read-data-page-headers (:byte-array-reader reader) num-data-pages)
                       rest                      ; the first page is always inaccurate
                       butlast                   ; the last page can have any length
                       (map (comp :length page/stats))
                       helpers/avg)))]
        (is (helpers/roughly 1024 (avg-page-length 1024)))
        (is (helpers/roughly 256 (avg-page-length 256)))))))

(deftest dictionary-column-chunk
  (let [cs (column-spec :int :dictionary :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 10))
        reader (write-column-chunk-and-get-reader cs input-blocks)
        output-blocks (read reader)]
    (testing "write/read a dictionary colum-chunk"
      (is (= input-blocks output-blocks)))
    (testing "value mapping"
      (let [map-fn (partial * 2)
            mapped-reader (write-column-chunk-and-get-reader (assoc cs :map-fn map-fn) input-blocks)]
        (is (= (->> input-blocks flatten (map #(some-> (.value ^LeveledValue %) map-fn)))
               (->> mapped-reader read flatten (map #(.value ^LeveledValue %)))))))
    (testing "repeatable writes"
      (let [w (doto (writer test-target-data-page-length cs)
                (write-blocks input-blocks))
            baw1 (doto (ByteArrayWriter. 10) (.write w))
            baw2 (doto (ByteArrayWriter. 10) (.write w))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatable reads"
      (is (= (read reader) (read reader))))))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [cs (column-spec-no-levels :boolean :plain :none)
          input-blocks (->> helpers/rand-bool repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding reader test-target-data-page-length)))))
  (testing "mostly true booleans"
    (let [cs (column-spec-no-levels :boolean :plain :none)
          input-blocks (->> #(helpers/rand-biased-bool 0.99) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-int-encodings
  (testing "random ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> helpers/rand-int repeatedly (rand-blocks cs) (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding reader test-target-data-page-length)))))
  (testing "random small ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :packed-run-length (find-best-encoding reader test-target-data-page-length)))))
  (testing "increasing ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> (range) (map int) (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding reader test-target-data-page-length)))))
  (testing "small selection of random ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          random-ints (repeatedly 100 helpers/rand-int)
          input-blocks (->> #(rand-nth random-ints) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length)))))
  (testing "small selection of chars"
    (let [cs (column-spec-no-levels :char :plain :none)
          input-blocks (->> #(rand-nth [\c \return \u1111]) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-long-encodings
  (testing "random longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> helpers/rand-long repeatedly (rand-blocks cs) (take 100))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding reader test-target-data-page-length)))))
  (testing "random small longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> #(helpers/rand-long-bits 10) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding reader test-target-data-page-length)))))
  (testing "increasing longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> (range) (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding reader test-target-data-page-length)))))
  (testing "increasing timestamps"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> #(System/nanoTime) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta (find-best-encoding reader test-target-data-page-length)))))
  (testing "incrementing dates as a custom-type"
    (let [cs (column-spec-no-levels :date :plain :none)
          input-blocks (->> (days-seq "2014-01-01") (rand-blocks cs) (take 1000))]
      (encoding/with-custom-types {:date {:base-type :long
                                          :to-base-type-fn #(.getTime %)
                                          :from-base-type-fn #(Date. %)}}
        (let [reader (write-column-chunk-and-get-reader cs input-blocks)]
          (is (= (read reader) input-blocks))
          (is (= :delta (find-best-encoding reader test-target-data-page-length)))))))
  (testing "small selection of random longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          random-ints (repeatedly 100 helpers/rand-long)
          input-blocks (->> #(rand-nth random-ints) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-float-encodings
  (testing "random floats"
    (let [cs (column-spec-no-levels :float :plain :none)
          input-blocks (->> helpers/rand-float repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding reader test-target-data-page-length)))))
  (testing "small selection of random floats"
    (let [cs (column-spec-no-levels :float :plain :none)
          random-floats (repeatedly 100 helpers/rand-float)
          input-blocks (->> #(rand-nth random-floats) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-double-encodings
  (testing "random doubles"
    (let [cs (column-spec-no-levels :double :plain :none)
          input-blocks (->> helpers/rand-double repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :plain (find-best-encoding reader test-target-data-page-length)))))
  (testing "small selection of random doubles"
    (let [cs (column-spec-no-levels :double :plain :none)
          random-doubles (repeatedly 100 helpers/rand-double)
          input-blocks (->> #(rand-nth random-doubles) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-byte-array-encodings
  (testing "random byte arrays"
    (let [cs (column-spec-no-levels :byte-array :plain :none)
          input-blocks (->> #(helpers/rand-byte-array) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read flatten (map #(.value ^LeveledValue %)))
                             (->> input-blocks flatten (map #(.value ^LeveledValue %))))))
      (is (= :delta-length (find-best-encoding reader test-target-data-page-length)))))
  (testing "random big ints"
    (let [cs (column-spec-no-levels :bigint :plain :none)
          input-blocks (->> #(helpers/rand-bigint 100) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :delta-length (find-best-encoding reader test-target-data-page-length)))))
  (testing "random big decimals"
    (let [cs (column-spec-no-levels :bigdec :plain :none)
          input-blocks (->> #(helpers/rand-bigdec 40) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :incremental (find-best-encoding reader test-target-data-page-length)))))
  (testing "incrementing dates"
    (let [cs (column-spec-no-levels :date :plain :none)
          input-blocks (->> (days-seq "2014-01-01") (rand-blocks cs) (take 1000))]
      (encoding/with-custom-types {:date {:base-type :string
                                          :to-base-type-fn #(.format simple-date-format %)
                                          :from-base-type-fn #(.parse simple-date-format %)}}
        (let [reader (write-column-chunk-and-get-reader cs input-blocks)]
          (is (= (read reader) input-blocks))
          (is (= :incremental (find-best-encoding reader test-target-data-page-length)))))))
  (testing "small set of keywords"
    (let [cs (column-spec-no-levels :keyword :plain :none)
          input-blocks (->> #(rand-nth [:foo :bar :baz]) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length)))))
  (testing "small set of symbols"
    (let [cs (column-spec-no-levels :symbol :plain :none)
          input-blocks (->> #(rand-nth ['foo 'bar 'baz]) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (read reader) input-blocks))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-fixed-length-byte-array-encodings
  (testing "random byte arrays"
    (let [cs (column-spec-no-levels :fixed-length-byte-array :plain :none)
          input-blocks (->> #(helpers/rand-byte-array 16) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read flatten (map #(.value ^LeveledValue %)))
                             (->> input-blocks flatten (map #(.value ^LeveledValue %))))))
      (is (= :plain (find-best-encoding reader test-target-data-page-length)))))
  (testing "small selection of random byte arrays"
    (let [cs (column-spec-no-levels :fixed-length-byte-array :plain :none)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          input-blocks (->> #(rand-nth rand-byte-arrays) repeatedly (rand-blocks cs) (take 1000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read flatten (map #(.value ^LeveledValue %)))
                             (->> input-blocks flatten (map #(.value ^LeveledValue %))))))
      (is (= :dictionary (find-best-encoding reader test-target-data-page-length))))))

(deftest find-best-compression-types
  (testing "plain encoded random ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (testing "different compression thresholds"
        (is (= :none (find-best-compression reader test-target-data-page-length {})))
        (is (= :deflate (find-best-compression reader test-target-data-page-length {:lz4 0.95 :deflate 0.6})))
        (is (= :lz4 (find-best-compression reader test-target-data-page-length {:lz4 0.95 :deflate 0.2})))
        (is (= :none (find-best-compression reader test-target-data-page-length {:lz4 0.5 :deflate 0.2}))))
      (testing "unsupported compression types throw proper exceptions"
        (is (thrown-with-msg? IllegalArgumentException #"is not a valid compression-type"
                              (find-best-compression reader test-target-data-page-length {:lzo 0.8})))))))

(deftest find-best-column-types
  (testing "lorem ispum permutations"
    (let [cs (column-spec-no-levels :string :plain :none)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          lorem-ipsum-words (-> helpers/lorem-ipsum (string/split #" ") set)
          lorem-ipsum-shuffles (repeatedly 100 #(->> lorem-ipsum-words shuffle (apply str)))
          input-blocks (->> #(rand-nth lorem-ipsum-shuffles)
                            repeatedly
                            (rand-blocks cs)
                            (take 5000))
          reader (write-column-chunk-and-get-reader cs input-blocks)]
      (is (= (assoc cs
               :encoding :dictionary
               :compression :deflate)
             (find-best-column-spec reader test-target-data-page-length {:lz4 0.8 :deflate 0.5}))))))

(deftest optimization
  (let [cs (column-spec-no-levels :string :plain :none)
        input-blocks (->> #(rand-nth ["foo" "bar" "baz"]) repeatedly (rand-blocks cs) (take 1000))
        w (reduce write! (writer test-target-data-page-length cs) input-blocks)
        optimized-w (optimize! w {:lz4 0.8 :deflate 0.5})]
    (is (= {:type :string :encoding :dictionary :compression :none}
           (-> optimized-w :column-spec (select-keys [:type :encoding :compression]))))
    (is (= input-blocks (read (writer->reader! optimized-w))))))