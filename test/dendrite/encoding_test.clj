(ns dendrite.encoding-test
  (:require [clojure.test :refer :all]
            [dendrite.encoding :refer :all]))

(deftest coercions
  (testing "coercions throw exceptions on bad input"
    (are [t y] ((coercion-fn t) y)
         :int32 2
         :int64 2
         :float 2
         :double 2
         :byte-array (byte-array 2)
         :fixed-length-byte-array (byte-array 2)
         :string "foo"
         :char \c
         :bigint 2
         :keyword :foo
         :symbol 'foo)
    (are [t y] (thrown? IllegalArgumentException ((coercion-fn t) y))
         :int32 [1 2]
         :int32 "foo"
         :int64 "foo"
         :float "foo"
         :double "foo"
         :byte-array [1 10]
         :fixed-length-byte-array [1 10]
         :char "f"
         :bigint "foo"
         :symbol 2)))
