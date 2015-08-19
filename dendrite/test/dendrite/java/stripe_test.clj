;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.stripe-test
  (:require [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java LeveledValue Schema Stripe Stripe$Fn]))

(set! *warn-on-reflection* true)

(use-fixtures :each helpers/use-in-column-logical-types)

(defn stripe-record [record schema]
  (let [n (count (Schema/getColumns schema))
        a (object-array n)]
    (.invoke (Stripe/getFn helpers/default-types schema nil nil) record a)
    (seq a)))

(deftest dremel-paper
  (testing "record striping matches dremel paper"
    (is (= dremel-paper-record1-striped (stripe-record dremel-paper-record1 dremel-paper-schema)))
    (is (= dremel-paper-record2-striped (stripe-record dremel-paper-record2 dremel-paper-schema)))))

(def test-schema* (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types)))

(deftest test-schema
  (testing "record striping works on test-schema"
    (let [test-record {:docid 0
                       :internal/is-active false
                       :links {:forward [1 2] :backward [3]}
                       :name [{:url "http://P" :language [{:code "us"}
                                                          {:code "gb" :country "Great Britain"}]}]
                       :keywords #{"commodo"}
                       :meta {"adipisicing" "laboris" "commodo" "elit"}
                       :ngrams [["foo" "bar"]]}]
      (is (= (stripe-record test-record test-schema*)
             [0
              [(LeveledValue. 0 3 3)]
              [(LeveledValue. 0 3 1) (LeveledValue. 1 3 2)]
              [(LeveledValue. 0 4 "us") (LeveledValue. 2 4 "gb")]
              [(LeveledValue. 0 4 nil) (LeveledValue. 2 5 "Great Britain")]
              [(LeveledValue. 0 3 "http://P")]
              [(LeveledValue. 0 1 "adipisicing") (LeveledValue. 1 1 "commodo")]
              [(LeveledValue. 0 1 "laboris") (LeveledValue. 1 1 "elit")]
              [(LeveledValue. 0 1 "commodo")]
              false
              [(LeveledValue. 0 2 "foo") (LeveledValue. 2 2 "bar")]]))))
  (testing "nil values in repeated records are preserved"
    (is (= (stripe-record {:docid 0 :internal/is-active false :name [nil]} test-schema*)
           [0
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 2 nil)]
            [(LeveledValue. 0 2 nil)]
            [(LeveledValue. 0 2 nil)]
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 0 nil)]
            false
            [(LeveledValue. 0 0 nil)]]))))

(deftest invalid-records
  (testing "missing required field"
    (let [schema (Schema/parse helpers/default-types
                               (Schema/req {:docid (Schema/req 'long)
                                            :name [{:language (Schema/req {:country 'string
                                                                           :code (Schema/req 'string)})
                                                    :url 'string}]
                                            :keywords #{(Schema/req 'string)}}))]
      (are [x] (stripe-record x schema)
           {:docid 10}
           {:docid 10 :name []}
           {:docid 10 :name [{:language {:code "en-us"}}]}
           {:docid 10 :name [{:language {:code "en-us"}}] :keywords #{"foo" "bar"}})
      (are [x re] (thrown-with-msg? IllegalArgumentException re
                                    (helpers/throw-cause (stripe-record x schema)))
           nil #"Required value at path '\[:docid\]' is missing"
           {:docid 10 :name [{:url "http://A"}]}
           #"Required record at path '\[:name nil :language\]' is missing"
           {:docid 10 :name [{}]} #"Required record at path '\[:name nil :language\]' is missing"
           {:docid 10 :name [{:language {}}]}
           #"Required value at path '\[:name nil :language :code\]' is missing"
           {:docid 10 :keywords #{nil}}
           #"Required value at path '\[:keywords nil\]' is missing")))
  (testing "incompatible value types"
    (let [schema (Schema/parse helpers/default-types
                               {:boolean 'boolean
                                :int 'int
                                :long 'long
                                :float 'float
                                :double 'double
                                :string 'string
                                :fixed-length-byte-array 'fixed-length-byte-array
                                :byte-array 'byte-array
                                :char 'char
                                :bigint 'bigint
                                :keyword 'keyword
                                :symbol 'symbol
                                :repeated-int ['int]})]
      (are [x] (stripe-record x schema)
           {:boolean true}
           {:boolean nil}
           {:int 2}
           {:long 2}
           {:float 2}
           {:double 2}
           {:string "foo"}
           {:fixed-length-byte-array (byte-array 2)}
           {:byte-array (byte-array 2)}
           {:string "foo"}
           {:char \c}
           {:bigint 2}
           {:keyword :foo}
           {:symbol 'foo}
           {:repeated-int [1 2]}
           {:repeated-int [1 nil]})
      (are [x] (thrown-with-msg? IllegalArgumentException #"Could not coerce value"
                                 (helpers/throw-cause (stripe-record x schema)))
           {:int [1 2]}
           {:int "2"}
           {:long "2"}
           {:float "2"}
           {:double "2"}
           {:fixed-length-byte-array ["a" 2 3]}
           {:byte-array ["a" 2 3]}
           {:char "f"}
           {:bigint "foo"}
           {:symbol 2}
           {:repeated-int ["foo"]}))))
