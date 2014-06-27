(ns dendrite.schema-test
  (:require [clojure.test :refer :all]
            [dendrite.schema :refer :all]
            [dendrite.test-helpers :refer [test-schema-str]])
  (:refer-clojure :exclude [read-string val]))

(deftest parse-human-readable-schema-str
  (testing "write/read human-readable schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string str read-string))))
  (testing "parse human-reable schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string parse human-readable)))))

(deftest schema-annotation
  (testing "value types are properly annotated"
    (let [schema (-> test-schema-str read-string parse)]
      (is (= :required (:repetition schema)))
      (are [ks m] (let [cs  (-> schema (sub-field-in ks) :column-spec)]
                    (and (= (:column-index cs) (:column-index m))
                         (= (:max-repetition-level cs) (:max-repetition-level m))
                         (= (:max-definition-level cs) (:max-definition-level m))))
           [:docid] {:column-index 0 :max-repetition-level 0 :max-definition-level 0}
           [:links :backward] {:column-index 1 :max-repetition-level 1 :max-definition-level 2}
           [:links :forward] {:column-index 2 :max-repetition-level 1 :max-definition-level 2}
           [:name :language :code] {:column-index 3 :max-repetition-level 2 :max-definition-level 2}
           [:name :language :country] {:column-index 4 :max-repetition-level 2 :max-definition-level 3}
           [:name :url] {:column-index 5 :max-repetition-level 1 :max-definition-level 2}
           [:meta :key] {:column-index 6 :max-repetition-level 1 :max-definition-level 1}
           [:meta :value] {:column-index 7 :max-repetition-level 1 :max-definition-level 1}
           [:keywords] {:column-index 8 :max-repetition-level 1 :max-definition-level 1}))))

(deftest invalid-schemas
  (testing "unsupported types"
    (is (thrown? IllegalArgumentException (parse {:foo 'invalid})))
    (is (thrown? IllegalArgumentException (parse {:foo {:bar 'invalid}}))))
  (testing "mismatched type and encodings"
    (is (thrown? IllegalArgumentException (parse {:foo (col {:type :int :encoding :incremental})})))
    (is (thrown? IllegalArgumentException (parse {:foo {:var (col {:type :string :encoding :delta})}}))))
  (testing "unsupported compression types"
    (is (thrown? IllegalArgumentException (parse {:foo (col {:type :int :compression :snappy})}))))
  (testing "marking a field as both reapeated and required"
    (is (thrown? IllegalArgumentException (parse {:foo (req ['int])})))))

(deftest queries
  (let [schema (-> test-schema-str read-string parse)]
    (testing "read from string"
      (is (= (read-query-string "{:docid _ :links #foo {:backward (long)}}")
             {:docid '_ :links (tag 'foo {:backward (list 'long)})})))
    (testing "select sub-schema from query"
      (are [query sub-schema] (= (human-readable (apply-query schema query)) sub-schema)
           '_ (human-readable schema)
           {:docid '_} {:docid (req (col {:type :long :encoding :delta :compression :lz4}))}
           {:links '_} {:links {:backward (list 'long)
                                :forward [(col {:type :long :encoding :delta})]}}
           {:links {:backward ['long]}} {:links {:backward ['long]}}
           {:name [{:language [{:country '_}]}]} {:name [{:language [{:country 'string}]}]}
           {:name [{:language (list {:code 'string})}]} {:name [{:language (list {:code (req 'string)})}]}
           {:meta '_ :name [{:url '_}]} {:name [{:url 'string}] :meta {'string 'string}}
           {:keywords ['_]} {:keywords ['string]}
           {:meta ['_]} {:meta [{:key (req 'string) :value (req 'string)}]}))
    (testing "tagging"
      (let [bogus-fn (fn [])]
        (is (= bogus-fn (-> (apply-query schema {:docid (tag 'foo '_)} :readers {'foo bogus-fn})
                            (sub-field :docid)
                            :reader-fn)))
        (is (= bogus-fn (-> (apply-query schema {:name [{:language [{:code (tag 'foo '_)}]}]}
                                         :readers {'foo bogus-fn})
                            (sub-field-in [:name :language :code])
                            :reader-fn)))
        (is (= bogus-fn (-> (apply-query schema {:name (tag 'foo '_)} :readers {'foo bogus-fn})
                            (sub-field :name)
                            :reader-fn)))
        (is (nil? (-> (apply-query schema {:docid (tag 'foo '_)})
                      (sub-field :docid)
                      :reader-fn)))))
    (testing "missing fields throw errors if enforced"
      (is (thrown? IllegalArgumentException (apply-query schema {:docid '_ :missing '_}
                                                         :missing-fields-as-nil? false))))
    (testing "bad queries"
      (are [query] (thrown? IllegalArgumentException (apply-query schema query))
           (Object.)
           {:docid 'int}
           {:docid '_ :links {:backward ['int]}}
           {:links 'int}
           {:docid {:foo '_}}
           {:docid (list 'long)}
           {:docid ['long]}
           {:name #{{:url '_}}}
           {:name {'string '_}}))))
