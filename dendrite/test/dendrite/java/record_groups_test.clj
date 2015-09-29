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
  (:import [dendrite.java Bundle Bundle$Factory LeveledValue RecordGroup RecordGroup$Reader RecordGroup$Writer
            Schema Stripe Utils Types]))

(set! *warn-on-reflection* true)

(def test-target-data-page-length (* 128 1024))

(def ^Types types (Types/create))

(use-fixtures :each helpers/use-in-column-logical-types)

(deftest dremel-write-read
  (let [bundle-factory (Bundle$Factory. (Schema/getColumns dremel-paper-schema))
        dremel-bundle (->> (map list dremel-paper-record1-striped dremel-paper-record2-striped)
                           into-array
                           (.create bundle-factory 2))
        w (doto (RecordGroup$Writer. types
                                     (Schema/getColumns dremel-paper-schema)
                                     test-target-data-page-length
                                     RecordGroup/NONE)
            (.write dremel-bundle)
            .finish)
        record-group-metadata (.getMetadata w)
        bb (helpers/output-buffer->byte-buffer w)]
    (testing "full schema"
      (let [r (RecordGroup$Reader. types
                                   bb
                                   record-group-metadata
                                   (.columns dremel-paper-full-query-schema)
                                   100)]
        (is (= (seq dremel-bundle) (seq (first r))))))
    (testing "two fields example"
      (let [two-fields-query (Schema/applyQuery types
                                                true
                                                {}
                                                dremel-paper-schema
                                                {:docid '_ :name [{:language [{:country '_}]}]})
            r (RecordGroup$Reader. types
                                   bb
                                   record-group-metadata
                                   (.columns two-fields-query)
                                   100)]
        (is (= [[10 20]
                [[(LeveledValue. 0 7 "us") (LeveledValue. 2 6 nil)
                  (LeveledValue. 1 3 nil) (LeveledValue. 1 7 "gb")]
                 [(LeveledValue. 0 3 nil)]]]
               (seq (first r))))))
    (testing "no fields query"
      (let [no-fields-query (Schema/applyQuery types
                                               true
                                               {}
                                               dremel-paper-schema
                                               {:foo '_})
            r (RecordGroup$Reader. types
                                   bb
                                   record-group-metadata
                                   (.columns no-fields-query)
                                   100)]
        (is (not (seq (first r))))))))

(deftest byte-buffer-random-records-write-read
  (let [test-schema (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types))
        bundle-factory (Bundle$Factory. (Schema/getColumns test-schema))
        records (take 1000 (helpers/rand-test-records))
        stripe (Stripe/getFn helpers/default-types test-schema nil nil false)
        bundle (.stripe bundle-factory stripe records)
        w (doto (RecordGroup$Writer. helpers/default-types
                                     (Schema/getColumns test-schema)
                                     test-target-data-page-length
                                     RecordGroup/NONE)
            (.write bundle)
            .finish)
        record-group-metadata (.getMetadata w)
        bb (helpers/output-buffer->byte-buffer w)
        query-result (Schema/applyQuery helpers/default-types true {} test-schema '_)
        r (RecordGroup$Reader. helpers/default-types bb record-group-metadata (.columns query-result) 1000)]
    (testing "full schema"
      (is (= (seq bundle)
             (seq (first r)))))
    (testing "metadata reports correct length"
      (is (= (.length record-group-metadata) (.remaining bb))))))

(deftest file-random-records-write-read
  (let [test-schema (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types))
        bundle-factory (Bundle$Factory. (Schema/getColumns test-schema))
        records (take 1000 (helpers/rand-test-records))
        stripe (Stripe/getFn helpers/default-types test-schema nil nil false)
        bundle (.stripe bundle-factory stripe records)
        w (doto (RecordGroup$Writer. helpers/default-types
                                     (Schema/getColumns test-schema)
                                     test-target-data-page-length
                                     RecordGroup/ALL)
            (.write bundle)
            (.optimize {'deflate 2.0})
            .finish)
        record-group-metadata (.getMetadata w)
        tmp-file (io/as-file "target/tmp_file")
        query-result (Schema/applyQuery helpers/default-types true {}
                                        (.withColumns test-schema (.columns w)) '_)]
    (with-open [f (Utils/getWritingFileChannel tmp-file)]
      (.writeTo w f))
    (testing "full schema"
      (is (= (seq bundle)
             (with-open [f (Utils/getReadingFileChannel tmp-file)]
               (let [r (RecordGroup$Reader. helpers/default-types
                                            (Utils/mapFileChannel f 0 (.size f))
                                            record-group-metadata
                                            (.columns query-result)
                                            1000)]
                 (seq (first r)))))))
    (testing "metadata reports correct length"
      (is (= (.length record-group-metadata) (.length (io/as-file tmp-file)))))
    (io/delete-file tmp-file)))
