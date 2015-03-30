;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.encoding-test
  (:require [clojure.test :refer :all]
            [dendrite.encoding :refer :all])
  (:import [java.io StringWriter]
           [java.nio ByteBuffer]))

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
         :inst (java.util.Date.)
         :uuid (java.util.UUID/randomUUID)
         :char \c
         :bigint 2
         :bigdec 2.3
         :ratio (/ 2 3)
         :keyword :foo
         :symbol 'foo
         :byte-buffer (ByteBuffer/wrap (byte-array (map byte "foo"))))
    (are [t y] (thrown-with-msg? IllegalArgumentException #"Could not coerce" ((coercion-fn ts t) y))
         :int [1 2]
         :int "foo"
         :long "foo"
         :float "foo"
         :double "foo"
         :byte-array ["a" 10]
         :fixed-length-byte-array ["a" 10]
         :inst "date"
         :uuid "uuid"
         :char "f"
         :bigint "foo"
         :bigdec "foo"
         :ratio "foo"
         :symbol 2
         :byte-buffer "foo")))

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

(deftest derived-types
  (testing "derived-type"
    (testing "throws exception on invalid keys"
      (is (thrown-with-msg?
           IllegalArgumentException #"Key :invalid-key is not a valid derived-type key."
           (derived-type :my-type {:base-type :int :invalid-key :bar}))))
    (testing "throws exception when function was expected but not passed"
      (is (thrown-with-msg?
           IllegalArgumentException #":coercion-fn expects a function for type 'my-type'."
           (derived-type :my-type {:base-type :int :coercion-fn 2}))))
    (testing "warns on missing keys"
      (binding [*err* (StringWriter.)]
        (derived-type :my-type {:base-type :int})
        (is (re-find #":coercion-fn is not defined for type 'my-type', defaulting to clojure.core/identity."
                     (str *err*)))))))
