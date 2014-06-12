(ns dendrite.assembly-test
  (:require [clojure.test :refer :all]
            [dendrite.common :refer :all]
            [dendrite.schema :as schema]
            [dendrite.assembly :refer :all]
            [dendrite.dremel-paper-examples :refer :all]))

(deftest dremel-paper
  (testing "full schema"
    (is (= dremel-paper-record1 (assemble dremel-paper-schema dremel-paper-record1-striped)))
    (is (= dremel-paper-record2 (assemble dremel-paper-schema dremel-paper-record2-striped))))
  (testing "two fields example"
    (let [sub-schema (schema/sub-schema-for-query dremel-paper-schema
                                                  {:docid '_ :name [{:language [{:country '_}]}]})]
      (is (= {:docid 10
              :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
             (assemble sub-schema
                       [[(leveled-value 0 0 10)]
                        [(leveled-value 0 3 "us") (leveled-value 2 2 nil)
                         (leveled-value 1 1 nil) (leveled-value 1 3 "gb")]])))
      (is (= {:docid 20} (assemble sub-schema
                                   [[(leveled-value 0 0 20)]
                                    [(leveled-value 0 1 nil)]]))))))
