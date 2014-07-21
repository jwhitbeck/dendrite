(ns dendrite.record-group-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.record-group :refer :all]
            [dendrite.schema :as schema]
            [dendrite.striping :as striping]
            [dendrite.test-helpers :as helpers]
            [dendrite.utils :as utils])
  (:import [dendrite.java ByteArrayWriter]
           [java.nio.channels FileChannel])
  (:refer-clojure :exclude [read]))

(def target-data-page-length 1024)

(deftest dremel-write-read
  (let [w (doto (writer target-data-page-length (schema/column-specs dremel-paper-schema))
            (write! dremel-paper-record1-striped)
            (write! dremel-paper-record2-striped)
            .finish)
        record-group-metadata (metadata w)
        bar (helpers/get-byte-array-reader w)]
    (testing "full schema"
      (is (= [dremel-paper-record1-striped dremel-paper-record2-striped]
             (read (byte-array-reader bar record-group-metadata dremel-paper-full-query-schema)))))
    (testing "two fields example"
      (let [two-fields-schema (schema/apply-query dremel-paper-schema
                                                  {:docid '_ :name [{:language [{:country '_}]}]})]
        (is (= [[[(->LeveledValue 0 0 10)]
                 [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil)
                  (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]]
                [[(->LeveledValue 0 0 20)]
                 [(->LeveledValue 0 1 nil)]]]
               (read (byte-array-reader bar record-group-metadata two-fields-schema))))))))

(deftest byte-buffer-random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string schema/parse)
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema) records)
        w (doto (writer target-data-page-length (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        bar (helpers/get-byte-array-reader w)]
    (testing "full schema"
      (is (= striped-records
             (read (byte-array-reader bar record-group-metadata (schema/apply-query test-schema '_))))))))

(deftest file-random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string schema/parse)
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema) records)
        w (doto (writer target-data-page-length (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        tmp-file "target/tmp_file"]
    (with-open [f (utils/file-channel tmp-file :write)]
      (flush-to-file-channel! w f)
      (await-io-completion w))
    (testing "full schema"
      (is (= striped-records
             (with-open [f (utils/file-channel tmp-file :read)]
               (doall
                (read
                 (file-channel-reader f 0 record-group-metadata (schema/apply-query test-schema '_))))))))
    (io/delete-file tmp-file)))

(deftest optimal-column-specs
  (let [test-schema (-> helpers/test-schema-str schema/read-string schema/parse)
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema) records)
        w (doto (writer target-data-page-length (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        bar (helpers/get-byte-array-reader w)
        r (byte-array-reader bar record-group-metadata test-schema)]
    (= [[:long :delta :none]
        [:long :delta :none]
        [:long :delta :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]]
     (->> (find-best-column-specs r target-data-page-length {:lz4 0.8 :deflate 0.5} true)
          (map (juxt :type :encoding :compression))))
    (= [[:long :delta :lz4]
        [:long :delta :none]
        [:long :delta :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]
        [:string :dictionary :none]]
       (->> (find-best-column-specs r target-data-page-length {:lz4 0.8 :deflate 0.5} false)
            (map (juxt :type :encoding :compression))))))
