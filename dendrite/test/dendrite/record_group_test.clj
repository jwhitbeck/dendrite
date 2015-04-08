;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

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
  (:import [dendrite.java IOutputBuffer]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(def target-data-page-length 1024)

(defn- flat-read
  "Returns a list of vectors corresponding to the striped records in the record-group. A record group is a seq
  of columns that are, in turn, seqs of pages."
  [record-group-reader]
  (->> (read record-group-reader)
       (map utils/flatten-1)
       (apply map vector)))

(deftest dremel-write-read
  (let [w (doto ^IOutputBuffer (writer target-data-page-length
                                       helpers/default-type-store
                                       (schema/column-specs dremel-paper-schema))
            (write! dremel-paper-record1-striped)
            (write! dremel-paper-record2-striped)
            .finish)
        record-group-metadata (metadata w)
        bb (helpers/output-buffer->byte-buffer w)]
    (testing "full schema"
      (is (= [dremel-paper-record1-striped dremel-paper-record2-striped]
             (flat-read (byte-buffer-reader bb record-group-metadata
                                            helpers/default-type-store
                                            dremel-paper-full-query-schema)))))
    (testing "two fields example"
      (let [two-fields-schema (schema/apply-query dremel-paper-schema
                                                  {:docid '_ :name [{:language [{:country '_}]}]}
                                                  helpers/default-type-store
                                                  true
                                                  {})]
        (is (= [[10
                 [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil)
                  (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]]
                [20
                 [(->LeveledValue 0 1 nil)]]]
               (flat-read (byte-buffer-reader bb record-group-metadata
                                              helpers/default-type-store two-fields-schema))))))))

(deftest byte-buffer-random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string (schema/parse helpers/default-type-store))
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema helpers/default-type-store nil) records)
        w (doto ^IOutputBuffer (writer target-data-page-length
                                       helpers/default-type-store
                                       (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        bb (helpers/output-buffer->byte-buffer w)
        parsed-query (schema/apply-query test-schema '_ helpers/default-type-store true {})]
    (testing "full schema"
      (is (= striped-records
             (flat-read
              (byte-buffer-reader bb record-group-metadata helpers/default-type-store parsed-query)))))
    (testing "read seq is composed of columns that contain chunked pages"
      (is (every? true? (map (partial every? chunked-seq?)
                             (read (byte-buffer-reader bb
                                                       record-group-metadata helpers/default-type-store
                                                       parsed-query))))))))

(deftest file-random-records-write-read
  (let [test-schema (-> helpers/test-schema-str schema/read-string (schema/parse helpers/default-type-store))
        records (take 1000 (helpers/rand-test-records))
        striped-records (map (striping/stripe-fn test-schema nil helpers/default-type-store) records)
        w (doto ^IOutputBuffer (writer target-data-page-length
                                       helpers/default-type-store
                                       (schema/column-specs test-schema))
            (#(reduce write! % striped-records))
            .finish)
        record-group-metadata (metadata w)
        tmp-file "target/tmp_file"
        parsed-query (schema/apply-query test-schema '_ helpers/default-type-store true {})]
    (with-open [f (utils/file-channel tmp-file :write)]
      (flush-to-file-channel! w f)
      (await-io-completion w))
    (testing "full schema"
      (is (= striped-records
             (with-open [f (utils/file-channel tmp-file :read)]
               (doall
                (flat-read
                 (byte-buffer-reader (utils/map-file-channel f 0 (.size f))
                                     record-group-metadata
                                     helpers/default-type-store
                                     parsed-query)))))))
    (io/delete-file tmp-file)))
