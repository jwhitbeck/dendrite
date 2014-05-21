(ns dendrite.column-test
  (:require [clojure.string :as string]
            [clojure.test :refer :all]
            [dendrite.common :refer :all]
            [dendrite.column :refer :all]
            [dendrite.encoding :as encoding]
            [dendrite.page :as page]
            [dendrite.schema :as schema]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayWriter]
           [java.util Date Calendar]
           [java.text SimpleDateFormat])
  (:refer-clojure :exclude [read]))

(def target-data-page-size 1000)

(defn write-blocks [column-writer blocks]
  (reduce write column-writer blocks))

(defn write-column-and-get-reader
  [column-spec input-blocks]
  (let [writer (doto (column-writer target-data-page-size column-spec)
                 (write-blocks input-blocks)
                 .finish)
        column-chunk-metadata (metadata writer)]
    (-> writer
        helpers/get-byte-array-reader
        (column-reader column-chunk-metadata column-spec))))

(def simple-date-format (SimpleDateFormat. "yyyy-MM-dd"))

(defn- iterate-calendar-by-day [calendar]
  (lazy-seq (cons (.getTime calendar)
                  (iterate-calendar-by-day (doto calendar (.add Calendar/DATE 1))))))

(defn- days-seq [start-date-str]
  (-> (doto (Calendar/getInstance)
        (.setTime (.parse simple-date-format start-date-str)))
      iterate-calendar-by-day))

(defn- column-spec [value-type encoding compression]
  (schema/val {:type value-type :encoding encoding :compression compression
               :max-definition-level 3 :max-repetition-level 2}))

(defn- column-spec-no-levels [value-type encoding compression]
  (schema/val {:type value-type :encoding encoding :compression compression
               :max-definition-level 0 :max-repetition-level 0}))

(defn- rand-blocks [column-spec coll]
  (->> coll (helpers/leveled column-spec) (helpers/rand-partition 3)))

(deftest data-column
  (let [cs (column-spec :int :plain :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 5000))
        reader (write-column-and-get-reader cs input-blocks)
        num-pages (-> reader :column-chunk-metadata :num-data-pages)
        output-values (read reader)]
    (testing "write/read a colum"
      (is (helpers/roughly num-pages 11))
      (is (= (flatten input-blocks) output-values)))
    (testing "value mapping"
      (is (= (->> input-blocks flatten (map #(some-> % :value (* 2))))
             (map :value (read reader (partial * 2))))))
    (testing "repeatable writes"
      (let [writer (doto (column-writer target-data-page-size cs)
                     (write-blocks input-blocks))
            baw1 (doto (ByteArrayWriter. 10) (.write writer))
            baw2 (doto (ByteArrayWriter. 10) (.write writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatble reads"
      (is (= (read reader) (read reader))))
    (testing "Page size estimation converges"
      (->> (page/read-data-page-headers (:byte-array-reader reader) num-pages)
           rest                         ; the first page is always inaccurate
           butlast                      ; the last page can have any size
           (map data-page-header->partial-column-stats)
           (map #(+ (:header-bytes %) (:repetition-level-bytes %) (:definition-level-bytes %)
                    (:data-bytes %)))
           helpers/avg
           (helpers/roughly target-data-page-size)))))

(deftest dictionary-column
  (let [cs (column-spec :int :dictionary :deflate)
        input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 5000))
        reader (write-column-and-get-reader cs input-blocks)
        output-values (read reader)]
    (testing "write/read a dictionary colum"
      (is (= (flatten input-blocks) output-values)))
    (testing "value mapping"
      (is (= (->> input-blocks flatten (map #(some-> % :value (* 2))))
             (map :value (read reader (partial * 2))))))
    (testing "repeatable writes"
      (let [writer (doto (column-writer target-data-page-size cs)
                     (write-blocks input-blocks))
            baw1 (doto (ByteArrayWriter. 10) (.write writer))
            baw2 (doto (ByteArrayWriter. 10) (.write writer))]
        (is (= (-> baw1 .buffer seq) (-> baw2 .buffer seq)))))
    (testing "repeatble reads"
      (is (= (read reader) (read reader))))))

(deftest find-best-boolean-encodings
  (testing "random booleans"
    (let [cs (column-spec-no-levels :boolean :plain :none)
          input-blocks (->> helpers/rand-bool repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "mostly true booleans"
    (let [cs (column-spec-no-levels :boolean :plain :none)
          input-blocks (->> #(helpers/rand-biased-bool 0.99) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-int-encodings
  (testing "random ints"
    (let [cs (column-spec :int :plain :none)
          input-blocks (->> helpers/rand-int repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "random small ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :packed-run-length (find-best-encoding reader target-data-page-size)))))
  (testing "increasing ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> (range) (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          random-ints (repeatedly 100 helpers/rand-int)
          input-blocks (->> #(helpers/rand-member random-ints) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of chars"
    (let [cs (column-spec-no-levels :char :plain :none)
          input-blocks (->> #(helpers/rand-member [\c \return \u1111]) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-long-encodings
  (testing "random longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> helpers/rand-long repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "random small longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "increasing longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> (range) (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "increasing timestamps"
    (let [cs (column-spec-no-levels :long :plain :none)
          input-blocks (->> #(System/nanoTime) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta (find-best-encoding reader target-data-page-size)))))
  (testing "incrementing dates as a custom-type"
    (let [cs (column-spec-no-levels :date :plain :none)
          input-blocks (->> (days-seq "2014-01-01") (rand-blocks cs) (take 5000))]
      (binding [encoding/*custom-types* {:date {:base-type :long
                                                :to-base-type-fn #(.getTime %)
                                                :from-base-type-fn #(Date. %)}}]
        (let [reader (write-column-and-get-reader cs input-blocks)]
          (is (= (read reader) (flatten input-blocks)))
          (is (= :delta (find-best-encoding reader target-data-page-size)))))))
  (testing "small selection of random longs"
    (let [cs (column-spec-no-levels :long :plain :none)
          random-ints (repeatedly 100 helpers/rand-long)
          input-blocks (->> #(helpers/rand-member random-ints) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-float-encodings
  (testing "random floats"
    (let [cs (column-spec-no-levels :float :plain :none)
          input-blocks (->> helpers/rand-float repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random floats"
    (let [cs (column-spec-no-levels :float :plain :none)
          random-floats (repeatedly 100 helpers/rand-float)
          input-blocks (->> #(helpers/rand-member random-floats) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-double-encodings
  (testing "random doubles"
    (let [cs (column-spec-no-levels :double :plain :none)
          input-blocks (->> helpers/rand-double repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random doubles"
    (let [cs (column-spec-no-levels :double :plain :none)
          random-doubles (repeatedly 100 helpers/rand-double)
          input-blocks (->> #(helpers/rand-member random-doubles) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-byte-array-encodings
  (testing "random byte arrays"
    (let [cs (column-spec-no-levels :byte-array :plain :none)
          input-blocks (->> #(helpers/rand-byte-array) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :delta-length (find-best-encoding reader target-data-page-size)))))
  (testing "random big ints"
    (let [cs (column-spec-no-levels :bigint :plain :none)
          input-blocks (->> #(helpers/rand-big-int 100) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :delta-length (find-best-encoding reader target-data-page-size)))))
  (testing "incrementing dates"
    (let [cs (column-spec-no-levels :date :plain :none)
          input-blocks (->> (days-seq "2014-01-01") (rand-blocks cs) (take 5000))]
      (binding [encoding/*custom-types* {:date {:base-type :string
                                                :to-base-type-fn #(.format simple-date-format %)
                                                :from-base-type-fn #(.parse simple-date-format %)}}]
        (let [reader (write-column-and-get-reader cs input-blocks)]
          (is (= (read reader) (flatten input-blocks)))
          (is (= :incremental (find-best-encoding reader target-data-page-size)))))))
  (testing "small set of keywords"
    (let [cs (column-spec-no-levels :keyword :plain :none)
          input-blocks (->> #(helpers/rand-member [:foo :bar :baz]) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size)))))
  (testing "small set of symbols"
    (let [cs (column-spec-no-levels :symbol :plain :none)
          input-blocks (->> #(helpers/rand-member ['foo 'bar 'baz]) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (read reader) (flatten input-blocks)))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-fixed-length-byte-array-encodings
  (testing "random byte arrays"
    (let [cs (column-spec-no-levels :fixed-length-byte-array :plain :none)
          input-blocks (->> #(helpers/rand-byte-array 16) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :plain (find-best-encoding reader target-data-page-size)))))
  (testing "small selection of random byte arrays"
    (let [cs (column-spec-no-levels :fixed-length-byte-array :plain :none)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          input-blocks (->> #(helpers/rand-member rand-byte-arrays) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (every? true? (map helpers/array=
                             (->> reader read (map :value))
                             (->> input-blocks flatten (map :value)))))
      (is (= :dictionary (find-best-encoding reader target-data-page-size))))))

(deftest find-best-compression-types
  (testing "plain encoded random ints"
    (let [cs (column-spec-no-levels :int :plain :none)
          input-blocks (->> #(helpers/rand-int-bits 10) repeatedly (rand-blocks cs) (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= :none (find-best-compression reader target-data-page-size {})))
      (is (= :deflate (find-best-compression reader target-data-page-size {:lz4 0.9 :deflate 0.5})))
      (is (= :lz4 (find-best-compression reader target-data-page-size {:lz4 0.9 :deflate 0.2})))
      (is (= :none (find-best-compression reader target-data-page-size {:lz4 0.5 :deflate 0.2}))))))

(deftest find-best-column-types
  (testing "lorem ispum permutations"
    (let [cs (column-spec-no-levels :string :plain :none)
          rand-byte-arrays (repeatedly 100 #(helpers/rand-byte-array 16))
          lorem-ipsum-words (-> helpers/lorem-ipsum (string/split #" ") set)
          lorem-ipsum-shuffles (repeatedly 100 #(->> lorem-ipsum-words shuffle (apply str)))
          input-blocks (->>  #(helpers/rand-member lorem-ipsum-shuffles)
                             repeatedly
                             (rand-blocks cs)
                             (take 5000))
          reader (write-column-and-get-reader cs input-blocks)]
      (is (= (assoc cs
               :encoding :dictionary
               :compression :deflate)
             (find-best-column-spec reader target-data-page-size {:lz4 0.8 :deflate 0.5}))))))
