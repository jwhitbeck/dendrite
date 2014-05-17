(ns dendrite.striping-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer [leveled-value]]
            [dendrite.schema :as schema]
            [dendrite.striping :refer :all]
            [dendrite.test-helpers :refer [dremel-paper-schema-str]]))

(deftest dremel-paper
  (testing "Record striping matches dremel paper"
    (let [schema (-> dremel-paper-schema-str schema/read-string schema/parse schema/annotate)
          stripe-record-fn (stripe-fn schema)]
      (= (stripe-record-fn {:docid 10
                            :links {:forward [20 40 60]}
                            :name [{:language [{:code "en-us" :country "us"}
                                               {:code "en"}]
                                    :url "http://A"}
                                   {:url "http://B"}
                                   {:language [{:code "en-gb" :country "gb"}]}]})
         [[(leveled-value 0 0 10)]
          [(leveled-value 0 1 nil)]
          [(leveled-value 0 2 20) (leveled-value 1 2 40) (leveled-value 1 2 60)]
          [(leveled-value 0 2 "en-us") (leveled-value 2 2 "en") (leveled-value 1 1 nil)
           (leveled-value 1 2 "en-gb")]
          [(leveled-value 0 3 "us") (leveled-value 2 2 nil) (leveled-value 1 1 nil) (leveled-value 1 3 "gb")]
          [(leveled-value 0 2 "http://A") (leveled-value 1 2 "http://B") (leveled-value 1 1 nil)]])
      (= (stripe-record-fn {:docid 20
                            :links {:backward [10 30]
                                    :forward [80]}
                            :name [{:url "http://C"}]})
         [[(leveled-value 0 0 20)]
          [(leveled-value 0 2 10) (leveled-value 1 2 30)]
          [(leveled-value 0 2 80)]
          [(leveled-value 0 1 nil)]
          [(leveled-value 0 1 nil)]
          [(leveled-value 0 2 "http://C")]]))))
