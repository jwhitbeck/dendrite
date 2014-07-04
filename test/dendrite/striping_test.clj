(ns dendrite.striping-test
  (:require [clojure.test :refer :all]
            [dendrite.common :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.schema :as s]
            [dendrite.striping :refer :all]))

(deftest dremel-paper
  (testing "record striping matches dremel paper"
    (is (= dremel-paper-record1-striped (stripe-record dremel-paper-record1 dremel-paper-schema)))
    (is (= dremel-paper-record2-striped (stripe-record dremel-paper-record2 dremel-paper-schema)))))

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
