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
  (:require [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers]
            [dendrite.utils :as utils])
  (:import [dendrite.java Bundle ChunkedPersistentList LeveledValue RecordGroup RecordGroup$Reader
            RecordGroup$Writer Schema Types]))

(set! *warn-on-reflection* true)

(def test-target-data-page-length (* 128 1024))

(def ^Types types (Types/create nil nil))

(deftest dremel-write-read
  (let [dremel-bundle (->> (map vector dremel-paper-record1-striped2 dremel-paper-record2-striped2)
                           (map helpers/as-chunked-list)
                           (into-array ChunkedPersistentList)
                           Bundle.)
        w (doto (RecordGroup$Writer. types
                                     (Schema/getColumns dremel-paper-schema2)
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
                                   (.columns dremel-paper-full-query-schema2))]
        (is (= dremel-bundle (first (.readBundled r 100))))))
    (testing "two fields example"
      (let [two-fields-query (Schema/applyQuery types
                                                true
                                                {}
                                                dremel-paper-schema2
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
