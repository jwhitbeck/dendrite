(ns dendrite.schema-test
  (:require [clojure.test :refer :all]
            [dendrite.schema :refer :all]
            [dendrite.test-helpers :refer [test-schema-str throw-cause]])
  (:refer-clojure :exclude [read-string val record?]))

(deftest parse-unparsed-schema-str
  (testing "write/read unparsed schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string str read-string))))
  (testing "parse unparsed schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string parse unparse)))))

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
           [:keywords] {:column-index 8 :max-repetition-level 1 :max-definition-level 1}))))<

(deftest invalid-schemas
  (testing "unsupported types"
    (is (thrown-with-msg?
         IllegalArgumentException #"Unsupported type ':invalid' for column \[:foo\]"
         (throw-cause (parse {:foo 'invalid}))))
    (is (thrown-with-msg?
         IllegalArgumentException #"Unsupported type ':invalid' for column \[:foo :bar\]"
         (throw-cause (parse {:foo {:bar 'invalid}})))))
  (testing "mismatched type and encodings"
    (is (thrown-with-msg?
         IllegalArgumentException #"Mismatched type ':int' and encoding ':incremental' for column \[:foo\]"
         (throw-cause (parse {:foo (col 'int 'incremental)}))))
    (is (thrown-with-msg?
         IllegalArgumentException #"Mismatched type ':string' and encoding ':delta' for column \[:foo :var\]"
         (throw-cause (parse {:foo {:var (col 'string 'delta)}})))))
  (testing "unsupported compression types"
    (is (thrown-with-msg?
         IllegalArgumentException #"Unsupported compression type ':snappy' for column \[:foo\]"
         (throw-cause (parse {:foo (col 'int 'delta 'snappy)})))))
  (testing "marking a field as both reapeated and required"
    (is (thrown-with-msg?
         IllegalArgumentException #"Field \[:foo\] is marked both required and repeated"
         (throw-cause (parse {:foo (req ['int])}))))))

(deftest queries
  (let [schema (-> test-schema-str read-string parse)]
    (testing "read from string"
      (is (= (read-query-string "{:docid _ :links #foo {:backward (long)}}")
             {:docid '_ :links (tag 'foo {:backward (list 'long)})})))
    (testing "select sub-schema from query"
      (are [query sub-schema] (= (unparse (apply-query schema query)) sub-schema)
           '_ (unparse schema)
           {:docid '_} {:docid (req (col 'long 'delta 'lz4))}
           {:links '_} {:links {:backward (list 'long)
                                :forward [(col 'long 'delta)]}}
           {:links {:backward ['long]}} {:links {:backward ['long]}}
           {:name [{:language [{:country '_}]}]} {:name [{:language [{:country 'string}]}]}
           {:name [{:language (list {:code 'string})}]} {:name [{:language (list {:code (req 'string)})}]}
           {:meta '_ :name [{:url '_}]} {:name [{:url 'string}] :meta {'string 'string}}
           {:keywords ['_]} {:keywords ['string]}
           {:meta ['_]} {:meta [{:key (req 'string) :value (req 'string)}]}))
    (testing "tagging"
      (let [bogus-fn (fn [])]
        (is (= bogus-fn (-> (apply-query schema {:docid (tag 'foo '_)} {:readers {'foo bogus-fn}})
                            (sub-field :docid)
                            :reader-fn)))
        (is (= bogus-fn (-> (apply-query schema {:name [{:language [{:code (tag 'foo '_)}]}]}
                                         {:readers {'foo bogus-fn}})
                            (sub-field-in [:name :language :code])
                            :reader-fn)))
        (is (= bogus-fn (-> (apply-query schema {:name (tag 'foo '_)} {:readers {'foo bogus-fn}})
                            (sub-field :name)
                            :reader-fn)))
        (is (nil? (-> (apply-query schema {:docid (tag 'foo '_)})
                      (sub-field :docid)
                      :reader-fn)))))
    (testing "missing fields throw errors if enforced"
      (is (thrown-with-msg?
           IllegalArgumentException #"the following sub-fields don't exist: ':missing'"
           (throw-cause (apply-query schema {:docid '_ :missing '_} {:missing-fields-as-nil? false})))))
    (testing "bad queries"
      (are [query msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                         (throw-cause (apply-query schema query)))
           (Object.) "Unable to parse query element"
           {:docid 'int} (str "Mismatched column types for field \\[:docid\\]. "
                              "Asked for 'int' but schema defines 'long'")
           {:docid '_ :links {:backward ['int]}} (str "Mismatched column types for field "
                                                      "\\[:links :backward\\]. Asked for 'int' but schema "
                                                      "defines 'long'.")
           {:links 'int} "Field \\[:links\\] is a record field in schema, not a value"
           {:docid {:foo '_}} "Field \\[:docid\\] is a value field in schema, not a record."
           {:docid (list 'long)} (str "Field \\[:docid\\] contains a required in the schema, "
                                      "cannot be read as a list")
           {:docid ['long]} "Field \\[:docid\\] contains a required in the schema, cannot be read as a vector"
           {:name #{{:url '_}}} "Field \\[:name\\] contains a vector in the schema, cannot be read as a set."
           {:name {'string '_}} (str "Field \\[:name\\] contains a vector in the schema, "
                                     "cannot be read as a map.")))))
