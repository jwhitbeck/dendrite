(ns dendrite.encoding-test
  (:require [clojure.test :refer :all]
            [dendrite.encoding :refer :all]))

(deftest coercions
  (testing "coercions throw exceptions on bad input"
    (is (= 2 ((coercion-fn :int32) 2)))
    (is (thrown? IllegalArgumentException ((coercion-fn :int32) "foo")))))
