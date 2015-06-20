;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.schema-test
  (:require [clojure.test :refer :all]
            [dendrite.core] ; for the print-methods
            [dendrite.test-helpers :refer [test-schema-str throw-cause]])
  (:import [dendrite.java Col Schema Schema$Column Schema$Collection Schema$Record Schema$Field Types]))

(set! *warn-on-reflection* true)

(deftest read-write-unparsed-schema-str
  (testing "write/read unparsed schema"
    (is (= (Schema/readString test-schema-str)
           (-> test-schema-str Schema/readString pr-str Schema/readString))))
  (testing "badly tagged schema strings"
    (are [s regex] (thrown-with-msg? IllegalArgumentException regex (Schema/readString s))
         "#col[] " #"Invalid col"
         "#col [int plain none none]" #"Invalid col"
         "#req #req int" #"Cannot mark a field as required multiple times")))

(def ^Types types (Types/create))

(deftest parse-unparse-schemas
  (are [s] (= s (->> s (Schema/parse types) (Schema/unparse types)))
       'int
       (Schema/req 'int)
       ['int]
       #{'int}
       [['int]]
       [[['int]]]
       {'string 'int}
       {['string] 'int}
       {{'int 'int} [{:foo 'byte-array}]}
       {'string #{'int}}
       (Col. 'int 'vlq 'deflate)
       [(Col. 'int 'vlq 'deflate)]
       {:foo {:bar {:baz 'int}}}
       {:foo (Schema/req 'int)}
       {:foo 'int :bar (Col. 'long 'vlq)}
       [{:foo 'int}]
       {:foo #{'long}}
       {:foo #{{'int 'string}}}))

(deftest test-schema-str-stress-test
  (is (= (Schema/readString test-schema-str)
         (->> test-schema-str Schema/readString (Schema/parse types) (Schema/unparse types)))))

(def test-unparsed-schema (Schema/readString test-schema-str))
(def test-schema (Schema/parse types test-unparsed-schema))

(defn sub-schema ^Schema [schema k]
  (if (instance? Schema$Collection schema)
    (.repeatedSchema ^Schema$Collection schema)
    (.get ^Schema$Record schema k)))

(defn sub-schema-in ^Schema [schema [k & ks :as aks]]
  (if (empty? aks)
    schema
    (if (empty? ks)
      (sub-schema schema k)
      (sub-schema-in (sub-schema schema k) ks))))

(deftest schema-configuration
  (testing "schemas are properly configured in test schema"
    (are [ks rep rep-lvl def-lvl] (let [^Schema schema (sub-schema-in test-schema ks)]
                                    (and (= rep (.repetition schema))
                                         (= rep-lvl (.repetitionLevel schema))
                                         (= def-lvl (.definitionLevel schema))))
         [] Schema/REQUIRED 0 0
         [:docid] Schema/REQUIRED 0 0
         [:links] Schema/OPTIONAL 0 1
         [:links :backward] Schema/LIST 1 2
         [:links :backward nil] Schema/OPTIONAL 1 3
         [:links :forward] Schema/VECTOR 1 2
         [:links :forward nil] Schema/OPTIONAL 1 3
         [:name] Schema/VECTOR 1 1
         [:name nil] Schema/OPTIONAL 1 2
         [:name nil :language] Schema/VECTOR 2 3
         [:name nil :language nil] Schema/OPTIONAL 2 4
         [:name nil :language nil :code] Schema/REQUIRED 2 4
         [:name nil :language nil :country] Schema/OPTIONAL 2 5
         [:name nil :url] Schema/OPTIONAL 1 3
         [:meta] Schema/MAP 1 1
         [:meta nil] Schema/REQUIRED 1 1
         [:meta nil :key] Schema/REQUIRED 1 1
         [:meta nil :val] Schema/REQUIRED 1 1
         [:keywords] Schema/SET 1 1
         [:keywords nil] Schema/REQUIRED 1 1
         [:is-active] Schema/REQUIRED 0 0
         [:ngrams] Schema/VECTOR 1 1
         [:ngrams nil] Schema/VECTOR 2 2
         [:ngrams nil nil] Schema/REQUIRED 2 2)))
  (testing "columns are properly configured in test schema"
    (are [ks t enc com col-idx enc-def-lvl]
      (let [^Schema$Column column (sub-schema-in test-schema ks)]
        (and (= t (.type column))
             (= enc (.encoding column))
             (= com (.compression column))
             (= col-idx (.columnIndex column))
             (= enc-def-lvl (.enclosingCollectionMaxDefinitionLevel column))))
         [:docid] Types/LONG Types/DELTA Types/DEFLATE 0 0
         [:links :backward nil] Types/LONG Types/PLAIN Types/NONE 1 2
         [:links :forward nil] Types/LONG Types/DELTA Types/NONE 2 2
         [:name nil :language nil :code] Types/STRING Types/PLAIN Types/NONE 3 3
         [:name nil :language nil :country] Types/STRING Types/PLAIN Types/NONE 4 3
         [:name nil :url] Types/STRING Types/PLAIN Types/NONE 5 1
         [:meta nil :key] Types/STRING Types/PLAIN Types/NONE 6 1
         [:meta nil :val] Types/STRING Types/PLAIN Types/NONE 7 1
         [:keywords nil] Types/STRING Types/PLAIN Types/NONE 8 1
         [:is-active] Types/BOOLEAN Types/PLAIN Types/NONE 9 0
         [:ngrams nil nil] Types/STRING Types/PLAIN Types/NONE 10 2))
  (testing "records have the proper leaf-column-index"
    (are [ks leaf-column-idx] (let [^Schema$Record record (sub-schema-in test-schema ks)]
                                (= leaf-column-idx (.leafColumnIndex record)))
         [] 10
         [:links] 2
         [:name nil] 5
         [:name nil :language nil] 4))

(deftest invalid-schemas
  (are [schema regex] (thrown-with-msg? IllegalArgumentException regex
                                        (throw-cause (Schema/parse types schema)))
       'invalid #"Unknown type: 'invalid'"
       {:bar 'invalid} #"Unknown type: 'invalid'"
       (Col. 'int 'invalid) #"Unknown encoding: 'invalid'"
       (Col. 'int 'plain 'invalid) #"Unknown compression: 'invalid'"
       (Col. 'int 'incremental) #"Unsupported encoding 'incremental' for type 'int'"
       (Col. 'string 'delta) #"Unsupported encoding 'delta' for type 'string'"
       (Col. 'int 'incremental) #"Unsupported encoding 'incremental' for type 'int'"
       (Col. 'string 'delta) #"Unsupported encoding 'delta' for type 'string'"
       (Schema/req ['int]) #"Repeated element cannot also be required"
       {:foo (Schema/req [{:foo 'int}])} #"Repeated element cannot also be required"
       {:foo (Schema/req {'int 'int})} #"Repeated element cannot also be required"
       {:foo (byte-array [1 2 3])} #"Unsupported schema element"
       ['int {:foo :bar}] #"Repeated field can only contain a single schema element"
       {'string 'int 'long 'long} #"Map field can only contain a single key/value schema element"))

(deftest plain-schema
  (is (= {:is-active (Schema/req 'boolean)
          :keywords #{'string}
          :meta {(Schema/req 'string) (Schema/req 'string)}
          :name [{:url 'string :language [{:country 'string, :code (Schema/req 'string)}]}]
          :links {:forward ['long] :backward '(long)}
          :docid 'long
          :ngrams [['string]]}
         (Schema/unparsePlain types test-schema)))
  (is (= {:foo (Schema/req 'int)}
         (Schema/unparsePlain types (Schema/parse types {:foo (Schema/req (Col. 'int 'vlq))})))))

(deftest entrypoints
  (is (= test-unparsed-schema (->> test-schema (Schema/getSubSchema []) (Schema/unparse types))))
  (is (= (:links test-unparsed-schema) (->> test-schema (Schema/getSubSchema [:links]) (Schema/unparse types))))
  (is (= (get-in test-unparsed-schema [:links :backward])
         (->> test-schema (Schema/getSubSchema [:links :backward]) (Schema/unparse types))))
  (are [entrypoint regex] (thrown-with-msg? IllegalArgumentException regex
                                            (Schema/getSubSchema entrypoint test-schema))
       [:name :language] #"Entrypoint '\[:name :language\]' contains repeated field ':name'"
       [:docid :foo] #"Entrypoint '\[:docid :foo\]' contains column node at ':docid'"))

(deftest queries
  (testing "read from string"
    (is (= (Schema/readQueryString "{:docid _ :links #foo {:backward (long)}}")
           {:docid '_ :links (Schema/tag 'foo {:backward (list 'long)})})))
  (testing "select sub-schema from query"
    (are [query sub-schema] (= (Schema/unparse types
                                               (.schema (Schema/applyQuery types true {} test-schema query)))
                               sub-schema)
         '_ (Schema/unparse types test-schema)
         {:docid '_} {:docid (Schema/req (Col. 'long 'delta 'deflate))}
         {:links '_} {:links {:backward (list 'long)
                              :forward [(Col. 'long 'delta)]}}
         {:links {:backward ['long]}} {:links {:backward ['long]}}
         {:name [{:language [{:country '_}]}]} {:name [{:language [{:country 'string}]}]}
         {:name [{:language (list {:code 'string})}]} {:name [{:language (list {:code (Schema/req 'string)})}]}
         {:meta '_ :name [{:url '_}]} {:name [{:url 'string}] :meta {(Schema/req 'string)
                                                                     (Schema/req 'string)}}
         {:keywords ['_]} {:keywords ['string]}
         {:meta ['_]} {:meta [{:key (Schema/req 'string) :val (Schema/req 'string)}]}))
  (testing "query-column-indices are set correctly"
    (let [query-result (Schema/applyQuery types true {} test-schema '_)]
      (are [query-column-index sub-schema]
        (let [^Schema$Column col (-> query-result .schema (sub-schema-in sub-schema))]
          (is (= query-column-index (.queryColumnIndex col))))
        0 [:docid]
        1 [:links :backward nil]
        2 [:links :forward nil]
        3 [:name nil :language nil :code]
        4 [:name nil :language nil :country]
        5 [:name nil :url]
        6 [:meta nil :key]
        7 [:meta nil :val]
        8 [:keywords nil]
        9 [:is-active]
        10 [:ngrams nil nil])
      (is (= (range 11) (map #(.queryColumnIndex ^Schema$Column %) (.columns query-result)))))
    (let [query-result (Schema/applyQuery types true {} test-schema {:is-active '_ :name '_})]
      (are [query-column-index sub-schema]
        (let [^Schema$Column col (-> query-result .schema (sub-schema-in sub-schema))]
          (is (= query-column-index (.queryColumnIndex col))))
        0 [:name nil :language nil :code]
        1 [:name nil :language nil :country]
        2 [:name nil :url]
        3 [:is-active])
      (is (= (range 4) (map #(.queryColumnIndex ^Schema$Column %) (.columns query-result))))))
  (testing "tagging"
    (let [bogus-fn (fn [])]
      (are [query path] (= bogus-fn (-> (Schema/applyQuery types true {'foo bogus-fn} test-schema query)
                                        .schema
                                        (sub-schema-in path)
                                        .fn))
           {:docid (Schema/tag 'foo '_)} [:docid]
           {:name [{:language [{:code (Schema/tag 'foo '_)}]}]} [:name nil :language nil :code]
           {:links {:backward (Schema/tag 'foo '_)}} [:links :backward]
           {:links {:backward [(Schema/tag 'foo '_)]}} [:links :backward nil]
           {:name (Schema/tag 'foo '_)} [:name]
           {:ngrams (Schema/tag 'foo '_)} [:ngrams]
           {:ngrams [(Schema/tag 'foo '_)]} [:ngrams nil]
           {:ngrams [[(Schema/tag 'foo '_)]]} [:ngrams nil nil])
      (is (thrown-with-msg? IllegalArgumentException #"No reader function was provided for tag 'foo'"
                            (throw-cause (Schema/applyQuery types true {} test-schema
                                                            {:docid (Schema/tag 'foo '_)}))))
      (is (thrown-with-msg? IllegalArgumentException #"Cannot tag an element multiple times"
                            (Schema/applyQuery types true {} test-schema
                                               {:docid (Schema/tag 'bar (Schema/tag 'foo '_))})))))
  (testing "missing fields throw errors if enforced"
    (is (thrown-with-msg?
         IllegalArgumentException #"The following fields don't exist: \[:missing\]"
         (throw-cause (Schema/applyQuery types false {} test-schema {:docid '_ :missing '_})))))
  (testing "missing fields are marked as such and don't cross repetition levels"
    (are [repetition query path]
      (let [applied-query (.schema (Schema/applyQuery types true {} test-schema query))]
                                   (= repetition (.repetition (sub-schema-in applied-query path))))
         Schema/MISSING {:foo '_} [:foo]
         Schema/MISSING {:links {:foo '_}} [:links :foo]
         Schema/MISSING {:links {:foo ['_]}} [:links :foo]
         Schema/MISSING {:links {:foo #{'_}}} [:links :foo]
         Schema/MISSING {:links {:foo [{:bar '_}]}} [:links :foo]))
  (testing "tags on missing fields"
    (let [bogus-fn (fn [])]
      (are [query path] (= bogus-fn (-> (Schema/applyQuery types true {'foo bogus-fn} test-schema query)
                                        .schema
                                        (sub-schema-in path)
                                        .fn))
           {:foo (Schema/tag 'foo '_)} [:foo]
           {:links {:foo (Schema/tag 'foo '_)}} [:links :foo]
           {:links {:foo (Schema/tag 'foo ['_])}} [:links :foo])
      (is (nil? (-> (Schema/applyQuery types true {'foo bogus-fn} test-schema
                                       {:links {:foo [(Schema/tag 'foo '_)]}})
                    .schema
                    (sub-schema-in [:links :foo])
                    .fn)))))
  (testing "bad queries"
    (are [query regex] (thrown-with-msg? IllegalArgumentException regex
                                         (throw-cause (Schema/applyQuery types true {} test-schema query)))
         (Object.)
         #"Unable to parse query element"
         {:docid 'int}
         #"Mismatched column types at path \[:docid\]. Asked for 'int' but schema defines 'long'"
         {:docid '_ :links {:backward ['int]}}
         #"Mismatched column types at path \[:links :backward nil\]. Asked for 'int' but schema defines 'long'"
         {:links 'int}
         #"Element at path \[:links\] is a record, not a value"
         {:docid {:foo '_}}
         #"Element at path \[:docid\] is not a record in schema"
         {:docid (list 'long)}
         #"Element at path \[:docid\] contains a required in the schema, cannot be read as a list"
         {:docid ['long]}
         #"Element at path \[:docid\] contains a required in the schema, cannot be read as a vector"
         {:name #{{:url '_}}}
         #"Element at path \[:name\] contains a vector in the schema, cannot be read as a set."
         {:name {'string '_}}
         #"Element at path \[:name\] contains a vector in the schema, cannot be read as a map.")))
