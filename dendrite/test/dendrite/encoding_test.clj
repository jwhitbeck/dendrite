(ns dendrite.encoding-test
  (:require [clojure.test :refer :all]
            [dendrite.encoding :refer :all]))

(set! *warn-on-reflection* true)

(def ^:private ts (type-store nil))

(deftest coercions
  (testing "coercions throw exceptions on bad input"
    (are [t y] ((coercion-fn ts t) y)
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
    (are [t y] (thrown-with-msg? IllegalArgumentException #"Could not coerce" ((coercion-fn ts t) y))
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
    (is (valid-value-type? ts :int))
    (is (not (valid-value-type? ts :in)))
    (is (valid-value-type? ts :string))
    (is (valid-value-type? (type-store {:custom {:base-type :string}}) :custom))
    (is (not (valid-value-type? {:custom {:base-type :non-existing-type}} :custom))))
  (testing "valid-encoding-for-type?"
    (is (valid-encoding-for-type? ts :int :plain))
    (is (not (valid-encoding-for-type? ts :int :incremental)))
    (is (valid-encoding-for-type? ts :string :incremental))
    (is (not (valid-encoding-for-type? ts :string :delta)))
    (is (valid-encoding-for-type? (type-store {:custom {:base-type :string}}) :custom :incremental))
    (is (not (valid-encoding-for-type? (type-store {:custom {:base-type :string}}) :custom :delta)))))
