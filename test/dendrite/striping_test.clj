(ns dendrite.striping-test
  (:require [clojure.test :refer :all]
            [dendrite.common :refer :all]
            [dendrite.schema :as s]
            [dendrite.striping :refer :all]))

(def dremel-paper-schema
  (s/parse {:docid (s/req 'long)
            :links {:forward ['long]
                    :backward ['long]}
            :name [{:language [{:code (s/req 'string)
                                :country 'string}]
                    :url 'string}]}))

(deftest dremel-paper
  (testing "record striping matches dremel paper"
    (= (stripe-record {:docid 10
                       :links {:forward [20 40 60]}
                       :name [{:language [{:code "en-us" :country "us"}
                                          {:code "en"}]
                               :url "http://A"}
                              {:url "http://B"}
                              {:language [{:code "en-gb" :country "gb"}]}]}
                      dremel-paper-schema)
       [[(leveled-value 0 0 10)]
        [(leveled-value 0 2 20) (leveled-value 1 2 40) (leveled-value 1 2 60)]
        [(leveled-value 0 1 nil)]
        [(leveled-value 0 2 "en-us") (leveled-value 2 2 "en") (leveled-value 1 1 nil)
         (leveled-value 1 2 "en-gb")]
        [(leveled-value 0 3 "us") (leveled-value 2 2 nil) (leveled-value 1 1 nil) (leveled-value 1 3 "gb")]
        [(leveled-value 0 2 "http://A") (leveled-value 1 2 "http://B") (leveled-value 1 1 nil)]])
    (= (stripe-record {:docid 20
                       :links {:backward [10 30]
                               :forward [80]}
                       :name [{:url "http://C"}]}
                      dremel-paper-schema)
       [[(leveled-value 0 0 20)]
        [(leveled-value 0 2 80)]
        [(leveled-value 0 2 10) (leveled-value 1 2 30)]
        [(leveled-value 0 1 nil)]
        [(leveled-value 0 1 nil)]
        [(leveled-value 0 2 "http://C")]])))

(deftest invalid-records
  (testing "missing required field"
    (let [schema (s/parse {:docid (s/req 'long)
                           :name [{:language (s/req {:country 'string
                                                     :code (s/req 'string)})}
                                  :url 'string]})]
      (are [x] (stripe-record x schema)
           {:docid 10}
           {:docid 10 :name []}
           {:docid 10 :name [{}]}
           {:docid 10 :name [{:language {:code "en-us"}}]})
      (are [x] (thrown? IllegalArgumentException (stripe-record x schema))
           {}
           {:docid 10 :name [{:url "http://A"}]}
           (:docid 10 :name [{:language {}}])
           {:docid 10 :name [{:language {:country "us"}}]})))
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
      (are [x] (thrown? IllegalArgumentException (stripe-record x schema))
           {:int [1 2]}
           {:int "2"}
           {:long "2"}
           {:float "2"}
           {:double "2"}
           {:fixed-length-byte-array [1 2 3]}
           {:byte-array [1 2 3]}
           {:char "f"}
           {:bigint "foo"}
           {:symbol 2}))))
