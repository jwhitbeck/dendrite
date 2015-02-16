;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.striping-test
  (:require [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as s]
            [dendrite.striping :refer :all]
            [dendrite.test-helpers :as helpers]))

(set! *warn-on-reflection* true)

(defn stripe-record [record schema]
  ((stripe-fn schema helpers/default-type-store nil) record))

(deftest dremel-paper
  (testing "record striping matches dremel paper"
    (is (= dremel-paper-record1-striped (stripe-record dremel-paper-record1 dremel-paper-schema)))
    (is (= dremel-paper-record2-striped (stripe-record dremel-paper-record2 dremel-paper-schema)))))

(deftest test-schema
  (testing "striping returns ArraySeqs"
    (let [test-schema (-> helpers/test-schema-str s/read-string (s/parse helpers/default-type-store))]
      (is (instance? clojure.lang.ArraySeq
                     (stripe-record {:docid 0 :is-active false :name [nil]} test-schema)))))
  (testing "record striping works on test-schema"
    (let [test-schema (-> helpers/test-schema-str s/read-string (s/parse helpers/default-type-store))
          test-record {:docid 0
                       :is-active false
                       :links {:forward [1 2] :backward [3]}
                       :name [{:url "http://P" :language [{:code "us"}
                                                          {:code "gb" :country "Great Britain"}]}]
                       :keywords #{"commodo"}
                       :meta {"adipisicing" "laboris" "commodo" "elit"}}]
      (is (= (stripe-record test-record test-schema)
             [0
              [(->LeveledValue 0 2 3)]
              [(->LeveledValue 0 2 1) (->LeveledValue 1 2 2) ]
              [(->LeveledValue 0 2 "us") (->LeveledValue 2 2 "gb")]
              [(->LeveledValue 0 2 nil) (->LeveledValue 2 3 "Great Britain")]
              [(->LeveledValue 0 2 "http://P")]
              [(->LeveledValue 0 1 "adipisicing") (->LeveledValue 1 1 "commodo")]
              [(->LeveledValue 0 1 "laboris") (->LeveledValue 1 1 "elit")]
              [(->LeveledValue 0 1 "commodo")]
              false]))))
  (testing "nil values in repeated records are preserved"
    (let [test-schema (-> helpers/test-schema-str s/read-string (s/parse helpers/default-type-store))]
      (is (= (stripe-record {:docid 0 :is-active false :name [nil]} test-schema)
             [0
              [(->LeveledValue 0 0 nil)]
              [(->LeveledValue 0 0 nil)]
              [(->LeveledValue 0 1 nil)]
              [(->LeveledValue 0 1 nil)]
              [(->LeveledValue 0 1 nil)]
              [(->LeveledValue 0 0 nil)]
              [(->LeveledValue 0 0 nil)]
              [(->LeveledValue 0 0 nil)]
              false])))))

(deftest invalid-records
  (testing "missing required field"
    (let [schema (s/parse {:docid (s/req 'long)
                           :name [{:language (s/req {:country 'string
                                                     :code (s/req 'string)})
                                   :url 'string}]}
                          helpers/default-type-store)]
      (are [x] (stripe-record x schema)
           {:docid 10}
           {:docid 10 :name []}
           {:docid 10 :name [{}]}
           {:docid 10 :name [{:language {:code "en-us"}}]})
      (are [x msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                     (helpers/throw-cause (stripe-record x schema)))
           {} "Empty record!"
           {:docid 10 :name [{:url "http://A"}]} "Required field \\[:name :language\\] is missing"
           {:docid 10 :name [{:language {}}]} "Required field \\[:name :language\\] is missing"
           {:docid 10 :name [{:language {:country "us"}}]}
           "Required field \\[:name :language :code\\] is missing")))
  (testing "incompatible value types"
    (let [schema (s/parse {:boolean 'boolean
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
                           :repeated-int ['int]}
                          helpers/default-type-store)]
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
           {:repeated-int []})
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
           {:repeated-int [nil]}))))
