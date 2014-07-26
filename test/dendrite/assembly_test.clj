(ns dendrite.assembly-test
  (:require [clojure.test :refer :all]
            [dendrite.assembly :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema]
            [dendrite.striping :refer [stripe-record]]
            [dendrite.test-helpers :refer [test-schema-str]]))

(deftest dremel-paper
  (testing "full schema"
    (is (= dremel-paper-record1 (assemble dremel-paper-record1-striped dremel-paper-full-query-schema)))
    (is (= dremel-paper-record2 (assemble dremel-paper-record2-striped dremel-paper-full-query-schema))))
  (testing "two fields example"
    (let [sub-schema (schema/apply-query dremel-paper-schema
                                         {:docid '_ :name [{:language [{:country '_}]}]})]
      (is (= {:docid 10
              :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
             (assemble [[(->LeveledValue 0 0 10)]
                        [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil)
                         (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]]
                       sub-schema)))
      (is (= {:docid 20} (assemble [[(->LeveledValue 0 0 20)]
                                    [(->LeveledValue 0 1 nil)]]
                                   sub-schema))))))

(def test-schema (-> test-schema-str schema/read-string schema/parse))

(def test-record {:docid 10
                  :links {:backward (list 1 2 3)
                          :forward [4 5]}
                  :name [nil {:language [{:code "us" :country "USA"}]
                              :url "http://A"}]
                  :meta {"key1" "value1"
                         "key2" "value2"}
                  :keywords #{"lorem" "ipsum"}})

(def test-record-striped (stripe-record test-record test-schema))

(deftest repetition-types
  (testing "full schema"
    (is (= test-record (assemble test-record-striped (schema/apply-query test-schema '_)))))
  (testing "queries"
    (are [answer query column-indices]
      (let [stripes (mapv (partial get test-record-striped) column-indices)]
        (= answer (assemble stripes (schema/apply-query test-schema query))))
      {:docid 10} {:docid '_} [0]
      {:links {:backward (list 1 2 3) :forward [4 5]}} {:links '_} [1 2]
      {:name [nil {:language [{:code "us" :country "USA"}] :url "http://A"}]} {:name '_} [3 4 5]
      {:meta {"key1" "value1" "key2" "value2"}} {:meta '_} [6 7]
      {:keywords #{"lorem" "ipsum"}} {:keywords '_} [8])))

(deftest tagging
  (let [query {:meta (schema/tag 'foo '_)}
        stripes (mapv (partial get test-record-striped) [6 7])]
    (are [answer reader-fn]
      (= answer (assemble stripes (schema/apply-query test-schema query {:readers {'foo reader-fn}})))
      {:meta 2} count
      {:meta ["key2" "key1"]} keys)))
