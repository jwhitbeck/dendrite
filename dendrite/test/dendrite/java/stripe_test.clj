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

(defn stripe-record
  ([record schema] (stripe-record record schema true))
  ([record schema ignore-extra-fields?]
   (let [n (count (Schema/getColumns schema))
         a (object-array n)]
     (.invoke (Stripe/getFn helpers/default-types schema ignore-extra-fields?) record a)
     (seq a))))

(deftest dremel-paper
  (testing "record striping matches dremel paper"
    (is (= dremel-paper-record1-striped (stripe-record dremel-paper-record1 dremel-paper-schema)))
    (is (= dremel-paper-record2-striped (stripe-record dremel-paper-record2 dremel-paper-schema)))))

(def test-schema* (->> helpers/test-schema-str Schema/readString (Schema/parse helpers/default-types)))

(deftest test-schema
  (testing "record striping works on test-schema"
    (let [test-record {:docid 0
                       :links {:backward [3] :forward [1 2]}
                       :name [{:url "http://P" :language [{:code "us"}
                                                          {:code "gb" :country "Great Britain"}]}]
                       :meta {"adipisicing" "laboris" "commodo" "elit"}
                       :keywords #{"commodo"}
                       :internal/is-active false
                       :ngrams [["foo" "bar"]]}]
      (is (= (stripe-record test-record test-schema*)
             [0
              [(LeveledValue. 0 4 3)]
              [(LeveledValue. 0 3 1) (LeveledValue. 1 3 2)]
              [(LeveledValue. 0 6 "us") (LeveledValue. 2 6 "gb")]
              [(LeveledValue. 0 6 nil) (LeveledValue. 2 7 "Great Britain")]
              [(LeveledValue. 0 4 "http://P")]
              [(LeveledValue. 0 2 "adipisicing") (LeveledValue. 1 2 "commodo")]
              [(LeveledValue. 0 2 "laboris") (LeveledValue. 1 2 "elit")]
              [(LeveledValue. 0 2 "commodo")]
              false
              [(LeveledValue. 0 4 "foo") (LeveledValue. 2 4 "bar")]]))))
  (testing "nil values in repeated records are preserved"
    (is (= (stripe-record {:docid 0 :internal/is-active false :name [nil]} test-schema*)
           [0
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 3 nil)]
            [(LeveledValue. 0 3 nil)]
            [(LeveledValue. 0 3 nil)]
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 0 nil)]
            [(LeveledValue. 0 0 nil)]
            false
            [(LeveledValue. 0 0 nil)]]))))

(deftest invalid-records
  (testing "missing required field"
    (let [schema (Schema/parse helpers/default-types
                               (Schema/req {:docid (Schema/req 'long)
                                            :links {:forward (Schema/req ['long])}
                                            :name [{:language (Schema/req {:country 'string
                                                                           :code (Schema/req 'string)})
                                                    :url 'string}]
                                            :meta (Schema/req {'string 'string})
                                            :keywords #{(Schema/req 'string)}}))]
      (are [x] (stripe-record x schema)
        {:docid 10 :meta {}}
        {:docid 10 :name [] :meta {}}
        {:docid 10 :name [{:language {:code "en-us"}}] :meta {"foo" "bar"}}
        {:docid 10 :name [{:language {:code "en-us"}}] :meta {} :keywords #{"foo" "bar"}})
      (are [x re] (thrown-with-msg? IllegalArgumentException re (stripe-record x schema))
        nil #"Required value at path '\[:docid\]' is missing"
        {:docid 10} #"Required collection at path '\[:meta\]' is missing"
        {:docid 10 :links {} :meta {}} #"Required collection at path '\[:links :forward\]' is missing"
        {:docid 10 :name [{:url "http://A"}] :meta {}}
        #"Required record at path '\[:name nil :language\]' is missing"
        {:docid 10 :name [{}] :meta {}} #"Required record at path '\[:name nil :language\]' is missing"
        {:docid 10 :name [{:language {}}] :meta {}}
        #"Required value at path '\[:name nil :language :code\]' is missing"
        {:docid 10 :keywords #{nil} :meta {}}
        #"Required value at path '\[:keywords nil\]' is missing")))
  (testing "extra-fields"
    (let [schema (Schema/parse helpers/default-types {:docid 'int})]
      (is (stripe-record {:docid 2} schema false))
      (is (stripe-record {:docid 2 :extra-field 3} schema))
      (is (thrown-with-msg? IllegalArgumentException #"Field ':extra-field' at path '\[\]' is not in schema"
                            (stripe-record {:docid 2 :extra-field 3} schema false)))))
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
      (are [x] (thrown-with-msg? IllegalArgumentException #"Could not coerce value" (stripe-record x schema))
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
