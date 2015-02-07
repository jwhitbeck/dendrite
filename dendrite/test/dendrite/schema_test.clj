;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.schema-test
  (:require [clojure.test :refer :all]
            [dendrite.schema :refer :all]
            [dendrite.test-helpers :refer [default-type-store test-schema-str throw-cause]])
  (:refer-clojure :exclude [read-string val record?]))

(set! *warn-on-reflection* true)

(deftest parse-unparsed-schema-str
  (testing "write/read unparsed schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string pr-str read-string))))
  (testing "parse unparsed schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string (parse default-type-store) unparse)))))

(deftest schema-annotation
  (testing "value types are properly annotated"
    (let [schema (-> test-schema-str read-string (parse default-type-store))]
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
           [:keywords] {:column-index 8 :max-repetition-level 1 :max-definition-level 1}
           [:is-active] {:column-index 9 :max-repetition-level 0 :max-definition-level 0}))))

(deftest nested-map-schemas
  (are [unparsed-schema key-rep val-rep]
    (let [s (parse unparsed-schema default-type-store)]
      (and (= (-> s :sub-fields first :repetition) key-rep)
           (= (-> s :sub-fields second :repetition) val-rep)))
    {'string 'int} :optional :optional
    {'string (req 'int)} :optional :required
    {(req 'string) 'int} :required :optional
    {(req 'string) (req 'int)} :required :required
    {'string ['int]} :optional :vector
    {'string {:foo 'int}} :optional :optional
    {'string (req {:foo 'int})} :optional :required
    {'string #{{:foo 'int}}} :optional :set))

(deftest flat-schemas
  (testing "base types"
    (is (= 'int (-> "int" read-string str read-string)))
    (is (= 'int (-> "int" read-string (parse default-type-store) unparse)))
    (is (= (req (col 'int 'plain 'lz4)) (-> "#req #col [int plain lz4]" read-string pr-str read-string)))
    (is (= 'byte-array (-> "byte-array" read-string str read-string))))
  (testing "repeated"
    (is (= ['int] (-> "[int]" read-string str read-string)))
    (is (= ['int] (-> "[int]" read-string (parse default-type-store) unparse)))
    (is (= [(col 'int 'plain 'lz4)] (-> "[#col [int plain lz4]]" read-string pr-str read-string)))))

(deftest invalid-schemas
  (testing "unsupported types"
    (is (thrown-with-msg?
         IllegalArgumentException #"Unsupported type 'invalid' for column \[:foo\]"
         (throw-cause (parse {:foo 'invalid} default-type-store))))
    (is (thrown-with-msg?
         IllegalArgumentException #"Unsupported type 'invalid' for column \[:foo :bar\]"
         (throw-cause (parse {:foo {:bar 'invalid}} default-type-store)))))
  (testing "mismatched type and encodings"
    (is (thrown-with-msg?
         IllegalArgumentException #"Mismatched type 'int' and encoding 'incremental' for column \[:foo\]"
         (throw-cause (parse {:foo (col 'int 'incremental)} default-type-store))))
    (is (thrown-with-msg?
         IllegalArgumentException #"Mismatched type 'string' and encoding 'delta' for column \[:foo :var\]"
         (throw-cause (parse {:foo {:var (col 'string 'delta)}} default-type-store)))))
  (testing "unsupported compression types"
    (is (thrown-with-msg?
         IllegalArgumentException #"Unsupported compression type 'snappy' for column \[:foo\]"
         (throw-cause (parse {:foo (col 'int 'delta 'snappy)} default-type-store)))))
  (testing "marking a field as both repeated and required"
    (is (thrown-with-msg?
         IllegalArgumentException #"Field \[:foo\] is marked both required and repeated"
         (throw-cause (parse {:foo (req ['int])} default-type-store)))))
  (testing "marking a field as both repeated and required in a map"
    (is (thrown-with-msg?
         IllegalArgumentException #"Field \[:value\] is marked both required and repeated"
         (throw-cause (parse {'string (req ['int])} default-type-store)))))
  (testing "nesting required elements"
    (is (thrown-with-msg?
         IllegalArgumentException #"Cannot mark a field as required multiple times"
         (parse {:foo (req (req 'int))} default-type-store))))
  (testing "required repeated fields"
    (is (thrown-with-msg?
         IllegalArgumentException #"Repeated field \[:foo\] cannot be marked as required"
         (throw-cause (parse {:foo [(req 'int)]} default-type-store))))))

(deftest queries
  (let [schema (-> test-schema-str read-string (parse default-type-store))]
    (testing "read from string"
      (is (= (read-query-string "{:docid _ :links #foo {:backward (long)}}")
             {:docid '_ :links (tag 'foo {:backward (list 'long)})})))
    (testing "select sub-schema from query"
      (are [query sub-schema] (= (unparse (apply-query schema query default-type-store true {})) sub-schema)
           '_ (unparse schema)
           {:docid '_} {:docid (req (col 'long 'delta 'lz4))}
           {:links '_} {:links {:backward (list 'long)
                                :forward [(col 'long 'delta)]}}
           {:links {:backward ['long]}} {:links {:backward ['long]}}
           {:name [{:language [{:country '_}]}]} {:name [{:language [{:country 'string}]}]}
           {:name [{:language (list {:code 'string})}]} {:name [{:language (list {:code (req 'string)})}]}
           {:meta '_ :name [{:url '_}]} {:name [{:url 'string}] :meta {(req 'string) (req 'string)}}
           {:keywords ['_]} {:keywords ['string]}
           {:meta ['_]} {:meta [{:key (req 'string) :value (req 'string)}]}))
    (testing "tagging"
      (let [bogus-fn (fn [])]
        (is (= bogus-fn (-> (apply-query schema {:docid (tag 'foo '_)}
                                         default-type-store true {'foo bogus-fn})
                            (sub-field :docid)
                            :column-spec
                            :map-fn)))
        (is (= bogus-fn (-> (apply-query schema {:name [{:language [{:code (tag 'foo '_)}]}]}
                                         default-type-store true {'foo bogus-fn})
                            (sub-field-in [:name :language :code])
                            :column-spec
                            :map-fn)))
        (is (= bogus-fn (-> (apply-query schema {:links {:backward (tag 'foo '_)}}
                                         default-type-store true {'foo bogus-fn})
                            (sub-field-in [:links :backward])
                            :reader-fn)))
        (is (= bogus-fn (-> (apply-query schema {:links {:backward [(tag 'foo '_)]}}
                                         default-type-store true {'foo bogus-fn})
                            (sub-field-in [:links :backward])
                            :column-spec
                            :map-fn)))
        (is (= bogus-fn (-> (apply-query schema {:name (tag 'foo '_)} default-type-store true {'foo bogus-fn})
                            (sub-field :name)
                            :reader-fn)))
        (is (thrown-with-msg? IllegalArgumentException #"No reader function was provided for tag 'foo'"
                              (throw-cause (-> (apply-query schema {:docid (tag 'foo '_)}
                                                            default-type-store true {})
                                               (sub-field :docid)
                                               :reader-fn))))
        (is (thrown-with-msg? IllegalArgumentException #"Cannot tag an element multiple times"
                              (-> (apply-query schema {:docid (tag 'bar (tag 'foo '_))}
                                               default-type-store true {})
                                  (sub-field :docid)
                                  :reader-fn)))))
    (testing "missing fields throw errors if enforced"
      (is (thrown-with-msg?
           IllegalArgumentException #"The following fields don't exist: \[:missing\]"
           (throw-cause (apply-query schema {:docid '_ :missing '_} default-type-store false nil)))))
    (testing "bad queries"
      (are [query msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                         (throw-cause (apply-query schema query default-type-store true {})))
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

(deftest entrypoints
  (let [unparsed-schema (-> test-schema-str read-string)
        schema (parse unparsed-schema default-type-store)]
    (is (= unparsed-schema (-> schema (sub-schema-in nil) unparse)))
    (is (= (:links unparsed-schema) (-> schema (sub-schema-in [:links]) unparse)))
    (is (= (get-in unparsed-schema [:links :backward])
           (-> schema (sub-schema-in [:links :backward]) unparse)))
    (is (thrown-with-msg?
         IllegalArgumentException #"Entrypoint '\[:name :language\]' contains repeated field ':name'"
         (sub-schema-in schema [:name :language])))))
