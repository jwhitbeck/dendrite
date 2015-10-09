;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.assemble-test
  (:require [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Assemble LeveledValue Schema Schema$QueryResult Stripe]))

(set! *warn-on-reflection* true)

(use-fixtures :each helpers/use-in-column-logical-types)

(defn assemble [leveled-values query-result]
  (.invoke (Assemble/getFn helpers/default-types (.schema ^Schema$QueryResult query-result))
           (helpers/as-list-iterators leveled-values)))

(deftest dremel-paper
  (testing "full schema"
    (is (= dremel-paper-record1 (assemble dremel-paper-record1-striped dremel-paper-full-query-schema)))
    (is (= dremel-paper-record2 (assemble dremel-paper-record2-striped dremel-paper-full-query-schema))))
  (testing "two fields example"
    (let [sub-schema (Schema/applyQuery helpers/default-types
                                        true
                                        {}
                                        dremel-paper-schema
                                        {:docid '_ :name [{:language [{:country '_}]}]})]
      (is (= {:docid 10
              :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
             (assemble [10
                        [(LeveledValue. 0 7 "us") (LeveledValue. 2 5 nil)
                         (LeveledValue. 1 2 nil) (LeveledValue. 1 7 "gb")]]
                       sub-schema)))
      (is (= {:docid 20 :name [nil]} (assemble [20 [(LeveledValue. 0 2 nil)]]
                                               sub-schema))))))

(def test-schema (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types)))

(def test-record {:docid 10
                  :links {:backward (list 1 2 3)
                          :forward [4 5]}
                  :name [nil {:language [{:code "us" :country "USA"}]
                              :url "http://A"}]
                  :meta {"key1" "value1"
                         "key2" "value2"}
                  :keywords #{"lorem" "ipsum"}
                  :internal/is-active false
                  :ngrams [["foo" "bar"]]})

(def test-record-striped
  (let [n (count (Schema/getColumns test-schema))
        a (object-array n)]
    (helpers/with-in-column-logical-types
      (.invoke (Stripe/getFn helpers/default-types test-schema true) test-record a))
    a))

(deftest query-column-indices
  (testing "full schema"
    (is (= test-record (assemble test-record-striped
                                 (Schema/applyQuery helpers/default-types true {} test-schema '_)))))
  (testing "queries"
    (are [answer query column-indices]
      (let [stripes (mapv (partial aget test-record-striped) column-indices)]
        (= answer (assemble stripes (Schema/applyQuery helpers/default-types true {} test-schema query))))
      {:docid 10} {:docid '_} [0]
      {:links {:backward (list 1 2 3) :forward [4 5]}} {:links '_} [1 2]
      {:name [nil {:language [{:code "us" :country "USA"}] :url "http://A"}]} {:name '_} [3 4 5]
      {:meta {"key1" "value1" "key2" "value2"}} {:meta '_} [6 7]
      {:keywords #{"lorem" "ipsum"}} {:keywords '_} [8])))

(deftest tagging
  (testing "repeated value"
    (let [query {:links {:backward (Schema/tag 'foo '_)}}
          stripes (mapv (partial aget test-record-striped) [1])]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'foo f} test-schema query)))
        {:links {:backward 6}} (partial reduce +)
        {:links {:backward ["1" "2" "3"]}} (partial map str))))
  (testing "non-repeated record"
    (let [query {:links (Schema/tag 'foo '_)}
          stripes (mapv (partial aget test-record-striped) [1 2])]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'foo f} test-schema query)))
        {:links [1 2 3]} :backward
        {:links 2} (comp count keys)
        {:links :foo} #(if (empty? %) (throw (IllegalStateException.)) :foo))))
  (testing "repeated record"
    (let [query {:name (Schema/tag 'foo '_)}
          stripes (mapv (partial aget test-record-striped) [3 4 5])]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'foo f} test-schema query)))
        {:name 2} count
        {:name [nil]} (partial take 1))))
  (testing "map"
    (let [query {:meta (Schema/tag 'foo '_)}
          stripes (mapv (partial aget test-record-striped) [6 7])]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'foo f} test-schema query)))
        {:meta 2} count
        {:meta #{"key1" "key2"}} (comp set keys))))
  (testing "missing fields"
    (let [query (Schema/tag 'foo {:foo '_})
          stripes nil]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'foo f} test-schema query)))
        nil identity
        2 (constantly 2)
        0 count))
    (let [query {:links {:foo (Schema/tag 'foo [{:bar '_}])}}
          stripes nil]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'foo f} test-schema query)))
        nil identity
        {:links {:foo 2}} (constantly 2)
        {:links {:foo 0}} count))
    (let [query {:links {:foo {:bar (Schema/tag 'bar [{:baz '_}])}}}
          stripes nil]
      (are [answer f]
        (= answer (assemble stripes
                            (Schema/applyQuery helpers/default-types true {'bar f} test-schema query)))
        nil identity
        {:links {:foo {:bar 2}}} (constantly 2)
        {:links {:foo {:bar 0}}} count))))
