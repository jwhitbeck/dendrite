(ns dendrite.encoding-test
  (:require [clojure.test :refer :all]
            [dendrite.encoding :refer :all]))

(deftest coercions
  (testing "coercions throw exceptions on bad input"
    (are [t y] ((coercion-fn t) y)
         :int 2
         :long 2
         :float 2
         :double 2
         :byte-array (byte-array 2)
         :fixed-length-byte-array (byte-array 2)
         :string "foo"
         :char \c
         :bigint 2
         :bigdec 2.3
         :keyword :foo
         :symbol 'foo)
    (are [t y] (thrown-with-msg? IllegalArgumentException #"Could not coerce" ((coercion-fn t) y))
         :int [1 2]
         :int "foo"
         :long "foo"
         :float "foo"
         :double "foo"
         :byte-array ["a" 10]
         :fixed-length-byte-array ["a" 10]
         :char "f"
         :bigint "foo"
         :bigdec "foo"
         :symbol 2)))

(deftest validity-checks
  (testing "valid-value-type?"
    (is (valid-value-type? :int))
    (is (not (valid-value-type? :in)))
    (is (valid-value-type? :string))
    (is (binding [*custom-types* {:custom {:base-type :string}}]
          (valid-value-type? :custom)))
    (is (not (binding [*custom-types* {:custom {:base-type :non-existing-type}}]
               (valid-value-type? :custom)))))
  (testing "valid-encoding-for-type?"
    (is (valid-encoding-for-type? :int :plain))
    (is (not (valid-encoding-for-type? :int :incremental)))
    (is (valid-encoding-for-type? :string :incremental))
    (is (not (valid-encoding-for-type? :string :delta)))
    (is (binding [*custom-types* {:custom {:base-type :string}}]
          (valid-encoding-for-type? :custom :incremental)))
    (is (not (binding [*custom-types* {:custom {:base-type :string}}]
               (valid-encoding-for-type? :custom :delta))))))
