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
  (:import [dendrite.java ByteArrayWriter Finishable]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(def target-data-page-length 1024)

(deftest dremel-write-read
  (let [w (doto ^Finishable (writer target-data-page-length (schema/column-specs dremel-paper-schema))
            (write! dremel-paper-record1-striped)
            (write! dremel-paper-record2-striped)
            .finish)
        record-group-metadata (metadata w)
        bb (helpers/get-byte-buffer w)]
    (testing "full schema"
      (is (= [dremel-paper-record1-striped dremel-paper-record2-striped]
             (read (byte-buffer-reader bb 0 record-group-metadata dremel-paper-full-query-schema)))))
    (testing "two fields example"
      (let [two-fields-schema (schema/apply-query dremel-paper-schema
                                                  {:docid '_ :name [{:language [{:country '_}]}]})]
        (is (= [[[(->LeveledValue 0 0 10)]
                 [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil)
                  (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]]
                [[(->LeveledValue 0 0 20)]
                 [(->LeveledValue 0 1 nil)]]]
               (read (byte-buffer-reader bb 0 record-group-metadata two-fields-schema))))))))

(deftest byte-buffer-random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string schema/parse)
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema nil) records)
        w (doto ^Finishable (writer target-data-page-length (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        bb (helpers/get-byte-buffer w)]
    (testing "full schema"
      (is (= striped-records
             (read (byte-buffer-reader bb 0 record-group-metadata (schema/apply-query test-schema '_))))))
    (testing "read seq is chunked"
      (is (chunked-seq? (seq (read (byte-buffer-reader bb 0
                                                       record-group-metadata
                                                       (schema/apply-query test-schema '_)))))))
    (testing "read seq is composed of ArraySeqs"
      (is (every? (partial instance? clojure.lang.ArraySeq)
                  (read (byte-buffer-reader bb 0 record-group-metadata
                                            (schema/apply-query test-schema '_))))))))

(deftest file-random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string schema/parse)
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema nil) records)
        w (doto ^Finishable (writer target-data-page-length (schema/column-specs test-schema))
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
                 (byte-buffer-reader (utils/map-file-channel f)
                                     0
                                     record-group-metadata
                                     (schema/apply-query test-schema '_))))))))
    (io/delete-file tmp-file)))
