;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.record-groups-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java ReadBundle WriteBundle ChunkedPersistentList LeveledValue RecordGroup
            RecordGroup$Reader RecordGroup$Writer Schema Stripe Utils Types]))

(set! *warn-on-reflection* true)

(def test-target-data-page-length (* 128 1024))

(def ^Types types (Types/create))

(deftest dremel-write-read
  (let [dremel-bundle (->> (map vector dremel-paper-record1-striped dremel-paper-record2-striped)
                           (map helpers/as-chunked-list)
                           (into-array ChunkedPersistentList)
                           WriteBundle.)
        w (doto (RecordGroup$Writer. types
                                     (Schema/getColumns dremel-paper-schema)
                                     test-target-data-page-length
                                     RecordGroup/NONE)
            (.write dremel-bundle)
            .finish)
        record-group-metadata (.metadata w)
        bb (helpers/output-buffer->byte-buffer w)]
    (testing "full schema"
      (let [r (RecordGroup$Reader. types
                                   bb
                                   record-group-metadata
                                   (.columns dremel-paper-full-query-schema))]
        (is (= dremel-bundle (first (.readBundled r 100))))))
    (testing "two fields example"
      (let [two-fields-query (Schema/applyQuery types
                                                true
                                                {}
                                                dremel-paper-schema
                                                {:docid '_ :name [{:language [{:country '_}]}]})
            r (RecordGroup$Reader. types
                                   bb
                                   record-group-metadata
                                   (.columns two-fields-query))]
        (is (= [[10 20]
                [[(LeveledValue. 0 5 "us") (LeveledValue. 2 4 nil)
                  (LeveledValue. 1 2 nil) (LeveledValue. 1 5 "gb")]
                 [(LeveledValue. 0 2 nil)]]]
               (first (.readBundled r 100))))))))

(deftest byte-buffer-random-records-write-read
  (let [test-schema (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types))
        num-columns (count (Schema/getColumns test-schema))
        records (take 1000 (helpers/rand-test-records))
        stripe (Stripe/getFn helpers/default-types test-schema nil)
        bundle (WriteBundle/stripe records stripe num-columns)
        w (doto (RecordGroup$Writer. helpers/default-types
                                     (Schema/getColumns test-schema)
                                     test-target-data-page-length
                                     RecordGroup/NONE)
            (.write bundle)
            .finish)
        record-group-metadata (.metadata w)
        bb (helpers/output-buffer->byte-buffer w)
        query-result (Schema/applyQuery helpers/default-types true {} test-schema '_)
        r (RecordGroup$Reader. helpers/default-types bb record-group-metadata (.columns query-result))]
    (testing "full schema"
      (is (= bundle
             (first (.readBundled r 1000)))))))

(deftest file-random-records-write-read
  (let [test-schema (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types))
        num-columns (count (Schema/getColumns test-schema))
        records (take 1000 (helpers/rand-test-records))
        stripe (Stripe/getFn helpers/default-types test-schema nil)
        bundle (WriteBundle/stripe records stripe num-columns)
        w (doto (RecordGroup$Writer. helpers/default-types
                                     (Schema/getColumns test-schema)
                                     test-target-data-page-length
                                     RecordGroup/ALL)
            (.write bundle)
            (.optimize {'deflate 2.0})
            .finish)
        record-group-metadata (.metadata w)
        tmp-file (io/as-file "target/tmp_file")
        query-result (Schema/applyQuery helpers/default-types true {}
                                        (.withColumns test-schema (.columns w)) '_)]
    (with-open [f (Utils/getWritingFileChannel tmp-file)]
      (.writeTo w f))
    (testing "full schema"
      (is (= bundle
             (with-open [f (Utils/getReadingFileChannel tmp-file)]
               (let [r (RecordGroup$Reader. helpers/default-types
                                            (Utils/mapFileChannel f 0 (.size f))
                                            record-group-metadata
                                            (.columns query-result))]
                 (first (.readBundled r 1000)))))))
    (io/delete-file tmp-file)))
