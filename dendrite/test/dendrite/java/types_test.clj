;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.types-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java CustomType Options Types]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(def ^Types types (Types/create))

(deftest primitive-types
  (are [x y] (and (= (.getType types x) y)
                  (nil? (.getToBaseTypeFn types y))
                  (nil? (.getFromBaseTypeFn types y)))
       'boolean Types/BOOLEAN
       'int Types/INT
       'long Types/LONG
       'float Types/FLOAT
       'double Types/DOUBLE
       'byte-array Types/BYTE_ARRAY
       'fixed-length-byte-array Types/FIXED_LENGTH_BYTE_ARRAY))

(deftest built-in-logical-types
  (are [x y z] (and (= (.getType types x) y)
                    (.getToBaseTypeFn types y)
                    (.getFromBaseTypeFn types y)
                    (= z (.getPrimitiveType types y)))
       'string Types/STRING Types/BYTE_ARRAY
       'inst Types/INST Types/LONG
       'uuid Types/UUID Types/FIXED_LENGTH_BYTE_ARRAY
       'char Types/CHAR Types/INT
       'bigint Types/BIGINT Types/BYTE_ARRAY
       'ratio Types/RATIO Types/BYTE_ARRAY
       'keyword Types/KEYWORD Types/BYTE_ARRAY
       'symbol Types/SYMBOL Types/BYTE_ARRAY
       'byte-buffer Types/BYTE_BUFFER Types/BYTE_ARRAY))

(deftest encodings
  (are [x y] (= (.getEncoding types x) y)
       'plain Types/PLAIN
       'dictionary Types/DICTIONARY
       'frequency Types/FREQUENCY
       'vlq Types/VLQ
       'zig-zag Types/ZIG_ZAG
       'packed-run-length Types/PACKED_RUN_LENGTH
       'delta Types/DELTA
       'incremental Types/INCREMENTAL
       'delta-length Types/DELTA_LENGTH))

(deftest compression
  (are [x y] (= (.getCompression types x) y)
       'none Types/NONE
       'deflate Types/DEFLATE))

(deftest coercions
  (testing "coercions throw exceptions on bad input"
    (are [t y] ((.getCoercionFn types (.getType types t)) y)
         'int 2
         'long 2
         'float 2
         'double 2
         'byte-array (byte-array 2)
         'fixed-length-byte-array (byte-array 2)
         'string "foo"
         'inst (java.util.Date.)
         'uuid (java.util.UUID/randomUUID)
         'char \c
         'bigint 2
         'bigdec 2.3
         'ratio (/ 2 3)
         'keyword :foo
         'symbol 'foo
         'byte-buffer (ByteBuffer/wrap (byte-array (map byte "foo"))))
    (are [t y] (thrown-with-msg? IllegalArgumentException #"Could not coerce"
                                 ((.getCoercionFn types (.getType types t)) y))
         'int [1 2]
         'int "foo"
         'long "foo"
         'float "foo"
         'double "foo"
         'byte-array ["a" 10]
         'fixed-length-byte-array ["a" 10]
         'inst "date"
         'uuid "uuid"
         'char "f"
         'bigint "foo"
         'bigdec "foo"
         'ratio "foo"
         'symbol 2
         'byte-buffer "foo")))

(deftest base-type-function-compositions
  (testing "one step to primitive type"
    (let [f (.getToBaseTypeFn types Types/STRING)]
      (is (= [102 111 111] (seq (f "foo")))))
    (let [f (.getFromBaseTypeFn types Types/STRING)]
      (is (= "foo" (f (byte-array [102 111 111]))))))
  (testing "two steps to primitive type"
    (let [f (.getToBaseTypeFn types Types/KEYWORD)]
      (is (= [102 111 111] (seq (f :foo)))))
    (let [f (.getFromBaseTypeFn types Types/SYMBOL)]
      (is (= 'foo (f (byte-array [102 111 111])))))))

(deftest validity-checks
  (testing "invalid types"
    (is (thrown-with-msg? IllegalArgumentException #"Unknown type: 'in'"
                          (.getType types 'in))))
  (testing "invalid encodings"
    (is (thrown-with-msg? IllegalArgumentException #"Unknown encoding: 'foo'"
                          (.getEncoding types 'foo)))
    (is (thrown-with-msg? IllegalArgumentException #"Unknown encoding: 'foo'"
                          (.getEncoding types Types/INT 'foo)))
    (is (thrown-with-msg? IllegalArgumentException #"Unsupported encoding 'delta-length' for type 'int'"
                          (.getEncoding types Types/INT 'delta-length))))
  (testing "invalid compressions"
    (is (thrown-with-msg? IllegalArgumentException #"Unknown compression: 'foo'"
                          (.getCompression types 'foo)))))

(deftest custom-types
  (testing "invalid configurations"
    (are [custom-types regex]
      (thrown-with-msg? IllegalArgumentException regex
                        (Types/create (Options/getCustomTypeDefinitions {:custom-types custom-types})))
      {'foo {:base-type 'bar} 'bar {:base-type 'foo}} #"Loop detected for custom-type 'foo'"
      {'foo {:base-type 'foo}} #"Loop detected for custom-type 'foo'"
      {'foo {:base-type 'in}} #"Unknown base type 'in'"))
  (testing "custom-types work"
    (let [my-types (Types/create (Options/getCustomTypeDefinitions
                                  {:custom-types {'foo {:base-type 'int
                                                        :coercion-fn int
                                                        :to-base-type-fn inc
                                                        :from-base-type-fn dec}
                                                  'bar {:base-type 'foo
                                                        :coercion-fn double
                                                        :to-base-type-fn (partial * 2)
                                                        :from-base-type-fn #(quot % 2)}}}))
          foo (.getType my-types 'foo)
          bar (.getType my-types 'bar)]
      (testing "new types are incrementally assigned"
        (is (= Types/FIRST_CUSTOM_TYPE foo))
        (is (= (inc Types/FIRST_CUSTOM_TYPE) bar)))
      (testing "primitive types are correct"
        (is (= Types/INT (.getPrimitiveType my-types foo)))
        (is (= Types/INT (.getPrimitiveType my-types bar))))
      (testing "coercion-fns are correct"
        (is (= 2 ((.getCoercionFn my-types foo) 2.0)))
        (is (= 2.0 ((.getCoercionFn my-types bar) 2))))
      (testing "toBaseTypeFns are correct"
        (is (= 3 ((.getToBaseTypeFn my-types foo) 2)))
        (is (= 5 ((.getToBaseTypeFn my-types bar) 2))))
      (testing "fromBaseTypeFns are correct"
        (is (= 2 ((.getFromBaseTypeFn my-types foo) 3)))
        (is (= 2 ((.getFromBaseTypeFn my-types bar) 5))))
      (testing "custom-types array is correct"
        (is (= (sort-by #(.type ^CustomType %) (.getCustomTypes my-types))
               [(CustomType. (.getType my-types 'foo) Types/INT 'foo)
                (CustomType. (.getType my-types 'bar) (.getType my-types 'foo) 'bar)])))))
  (testing "reading from file with defined custom-types"
    (let [my-types (Types/create
                       (Options/getCustomTypeDefinitions
                        {:custom-types
                         {'foo {:base-type 'int
                                :coercion-fn int
                                :to-base-type-fn inc
                                :from-base-type-fn dec}
                          'bar {:base-type 'foo
                                :coercion-fn double
                                :to-base-type-fn (partial * 2)
                                :from-base-type-fn #(quot % 2)}}})
                     (into-array [(CustomType. 20 Types/INT 'foo)]))
          foo (.getType my-types 'foo)
          bar (.getType my-types 'bar)]
      (testing "new types are incrementally assigned"
        (is (= 20 foo))
        (is (= 21 bar)))))
  (testing "reading from older file with defined custom-types that override newer logical types"
    (let [my-types (Types/create
                       (Options/getCustomTypeDefinitions
                        {:custom-types
                         {'foo {:base-type 'int
                                :coercion-fn int
                                :to-base-type-fn inc
                                :from-base-type-fn dec}
                          'bar {:base-type 'foo
                                :coercion-fn double
                                :to-base-type-fn (partial * 2)
                                :from-base-type-fn #(quot % 2)}}})
                     (into-array [(CustomType. Types/BYTE_BUFFER Types/INT 'foo)]))
          foo (.getType my-types 'foo)
          bar (.getType my-types 'bar)]
      (testing "new types are incrementally assigned"
        (is (= Types/BYTE_BUFFER foo))
        (is (= Types/FIRST_CUSTOM_TYPE bar)))
      (testing "logical type is overriden"
        (is (= 'foo (.getTypeSymbol my-types Types/BYTE_BUFFER) (.getTypeSymbol my-types foo)))
        (is (= 'bar (.getTypeSymbol my-types bar)))))))
