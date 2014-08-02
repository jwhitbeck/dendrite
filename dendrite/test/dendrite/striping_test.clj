(ns dendrite.striping-test
  (:require [clojure.test :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.leveled-value :refer [->LeveledValue]]
            [dendrite.schema :as s]
            [dendrite.striping :refer :all]
            [dendrite.test-helpers :as helpers]))

(set! *warn-on-reflection* true)

(deftest dremel-paper
  (testing "record striping matches dremel paper"
    (is (= dremel-paper-record1-striped (stripe-record dremel-paper-record1 dremel-paper-schema)))
    (is (= dremel-paper-record2-striped (stripe-record dremel-paper-record2 dremel-paper-schema)))))

(deftest test-schema
  (testing "record striping works on test-schema"
    (let [test-schema (-> helpers/test-schema-str s/read-string s/parse)
          test-record {:docid 0
                       :is-active false
                       :links {:forward [1 2] :backward [3]}
                       :name [{:url "http://P" :language [{:code "us"}
                                                          {:code "gb" :country "Great Britain"}]}]
                       :keywords #{"commodo"}
                       :meta {"adipisicing" "laboris" "commodo" "elit"}}]
      (is (= (stripe-record test-record test-schema)
             [[(->LeveledValue 0 0 0)]
              [(->LeveledValue 0 2 3)]
              [(->LeveledValue 0 2 1) (->LeveledValue 1 2 2) ]
              [(->LeveledValue 0 2 "us") (->LeveledValue 2 2 "gb")]
              [(->LeveledValue 0 2 nil) (->LeveledValue 2 3 "Great Britain")]
              [(->LeveledValue 0 2 "http://P")]
              [(->LeveledValue 0 1 "adipisicing") (->LeveledValue 1 1 "commodo")]
              [(->LeveledValue 0 1 "laboris") (->LeveledValue 1 1 "elit")]
              [(->LeveledValue 0 1 "commodo")]
              [(->LeveledValue 0 0 false)]])))))

(deftest invalid-records
  (testing "missing required field"
    (let [schema (s/parse {:docid (s/req 'long)
                           :name [{:language (s/req {:country 'string
                                                     :code (s/req 'string)})
                                   :url 'string}]})]
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
                           :symbol 'symbol})]
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
           {:symbol 'foo})
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
           {:symbol 2}))))
