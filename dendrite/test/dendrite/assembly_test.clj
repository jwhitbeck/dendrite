;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.assembly-test
  (:require [clojure.test :refer :all]
            [dendrite.assembly :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as schema]
            [dendrite.striping :refer [stripe-fn]]
            [dendrite.test-helpers :refer [default-type-store test-schema-str]]))

(set! *warn-on-reflection* true)

(defn assemble [leveled-values query]
  ((assemble-fn query) (seq (into-array Object leveled-values))))

(deftest dremel-paper
  (testing "full schema"
    (is (= dremel-paper-record1 (assemble dremel-paper-record1-striped dremel-paper-full-query-schema)))
    (is (= dremel-paper-record2 (assemble dremel-paper-record2-striped dremel-paper-full-query-schema))))
  (testing "two fields example"
    (let [sub-schema (schema/apply-query dremel-paper-schema
                                         {:docid '_ :name [{:language [{:country '_}]}]}
                                         default-type-store
                                         true
                                         {})]
      (is (= {:docid 10
              :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
             (assemble [10
                        [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil)
                         (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]]
                       sub-schema)))
      (is (= {:docid 20 :name [nil]} (assemble [20 [(->LeveledValue 0 1 nil)]]
                                   sub-schema))))))

(def test-schema (-> test-schema-str schema/read-string (schema/parse default-type-store)))

(def test-record {:docid 10
                  :is-active false
                  :links {:backward (list 1 2 3)
                          :forward [4 5]}
                  :name [nil {:language [{:code "us" :country "USA"}]
                              :url "http://A"}]
                  :meta {"key1" "value1"
                         "key2" "value2"}
                  :keywords #{"lorem" "ipsum"}})

(def ^clojure.lang.ArraySeq test-record-striped ((stripe-fn test-schema default-type-store nil) test-record))

(deftest repetition-types
  (testing "full schema"
    (is (= test-record (assemble test-record-striped
                                 (schema/apply-query test-schema '_ default-type-store true {})))))
  (testing "queries"
    (are [answer query column-indices]
      (let [stripes (mapv (partial aget (.array test-record-striped)) column-indices)]
        (= answer (assemble stripes (schema/apply-query test-schema query default-type-store true {}))))
      {:docid 10} {:docid '_} [0]
      {:links {:backward (list 1 2 3) :forward [4 5]}} {:links '_} [1 2]
      {:name [nil {:language [{:code "us" :country "USA"}] :url "http://A"}]} {:name '_} [3 4 5]
      {:meta {"key1" "value1" "key2" "value2"}} {:meta '_} [6 7]
      {:keywords #{"lorem" "ipsum"}} {:keywords '_} [8])))

(deftest tagging
  (testing "repeated value"
    (let [query {:links {:backward (schema/tag 'foo '_)}}
          stripes (mapv (partial aget (.array test-record-striped)) [1])]
      (are [answer reader-fn]
        (= answer (assemble stripes
                            (schema/apply-query test-schema query default-type-store true {'foo reader-fn})))
        {:links {:backward 6}} (partial reduce +)
        {:links {:backward ["1" "2" "3"]}} (partial map str))))
  (testing "non-repeated record"
    (let [query {:links (schema/tag 'foo '_)}
          stripes (mapv (partial aget (.array test-record-striped)) [1 2])]
      (are [answer reader-fn]
        (= answer (assemble stripes
                            (schema/apply-query test-schema query default-type-store true {'foo reader-fn})))
        {:links [1 2 3]} :backward
        {:links 2} (comp count keys))))
  (testing "repeated record"
    (let [query {:name (schema/tag 'foo '_)}
          stripes (mapv (partial aget (.array test-record-striped)) [3 4 5])]
      (are [answer reader-fn]
        (= answer (assemble stripes
                            (schema/apply-query test-schema query default-type-store true {'foo reader-fn})))
        {:name 2} count
        {:name [nil]} (partial take 1))))
  (testing "map"
    (let [query {:meta (schema/tag 'foo '_)}
          stripes (mapv (partial aget (.array test-record-striped)) [6 7])]
      (are [answer reader-fn]
        (= answer (assemble stripes
                            (schema/apply-query test-schema query default-type-store true {'foo reader-fn})))
        {:meta 2} count
        {:meta ["key2" "key1"]} keys))))
