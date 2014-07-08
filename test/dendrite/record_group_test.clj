(ns dendrite.record-group-test
  (:require [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.record-group :refer :all]
            [dendrite.schema :as schema]
            [dendrite.striping :as striping]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ByteArrayWriter])
  (:refer-clojure :exclude [read]))

(def target-data-page-size 1024)

(deftest dremel-write-read
  (let [w (doto (writer target-data-page-size (schema/column-specs dremel-paper-schema))
            (write! dremel-paper-record1-striped)
            (write! dremel-paper-record2-striped)
            .finish)
        record-group-metadata (metadata w)
        bar (helpers/get-byte-array-reader w)]
    (testing "full schema"
      (is (= [dremel-paper-record1-striped dremel-paper-record2-striped]
             (read (record-group-byte-array-reader bar
                                                   record-group-metadata
                                                   dremel-paper-full-query-schema)))))
    (testing "two fields example"
      (let [two-fields-schema (schema/apply-query dremel-paper-schema
                                                  {:docid '_ :name [{:language [{:country '_}]}]})]
        (is (= [[[(->LeveledValue 0 0 10)]
                 [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil)
                  (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]]
                [[(->LeveledValue 0 0 20)]
                 [(->LeveledValue 0 1 nil)]]]
               (-> (record-group-byte-array-reader bar record-group-metadata two-fields-schema)
                   read)))))))

(deftest random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string schema/parse)
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema) records)
        w (doto (writer target-data-page-size (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        bar (helpers/get-byte-array-reader w)]
    (testing "full schema"
      (is (= striped-records
             (read (record-group-byte-array-reader bar
                                                   record-group-metadata
                                                   (schema/apply-query test-schema '_))))))))
