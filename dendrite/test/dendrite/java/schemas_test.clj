;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.schemas-test
  (:require [clojure.test :refer :all]
            [dendrite.core2] ; for the print-method
            [dendrite.test-helpers :refer [test-schema-str throw-cause]])
  (:import [dendrite.java Col SchemaNode SchemaNode$Leaf SchemaNode$Collection SchemaNode$Record Schemas
            Types]))

(set! *warn-on-reflection* true)

(deftest read-write-unparsed-schema-str
  (testing "write/read unparsed schema"
    (is (= (Schemas/readString test-schema-str)
           (-> test-schema-str Schemas/readString pr-str Schemas/readString))))
  (testing "badly tagged schema strings"
    (are [s regex] (thrown-with-msg? IllegalArgumentException regex (Schemas/readString s))
         "#col[] " #"Invalid col"
         "#col [int plain none none]" #"Invalid col"
         "#req #req int" #"Cannot mark a field as required multiple times")))

(def ^Types types (Types/create nil nil))

(deftest parse-unparse-schemas
  (are [s] (= s (->> s (Schemas/parse types) (Schemas/unparse types)))
       'int
       (Schemas/req 'int)
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
       {:foo (Schemas/req 'int)}
       {:foo 'int :bar (Col. 'long 'vlq)}
       [{:foo 'int}]
       {:foo #{'long}}
       {:foo #{{'int 'string}}}))

(deftest test-schema-str-stress-test
  (is (= (Schemas/readString test-schema-str)
         (->> test-schema-str Schemas/readString (Schemas/parse types) (Schemas/unparse types)))))

(def test-unparsed-schema (Schemas/readString test-schema-str))
(def test-schema (Schemas/parse types test-unparsed-schema))

(defn sub-node [node k]
  (if (instance? SchemaNode$Collection node)
    (.repeatedNode ^SchemaNode$Collection node)
    (->> (.fields ^SchemaNode$Record node)
         (filter #(= (.name ^SchemaNode %) k))
         first)))

(defn sub-node-in [node [k & ks :as aks]]
  (if (empty? aks)
    node
    (if (empty? ks)
      (sub-node node k)
      (sub-node-in (sub-node node k) ks))))

(deftest schema-configuration
  (testing "schema nodes are properly configured in test schema"
    (are [ks n rep rep-lvl def-lvl] (let [^SchemaNode schema-node (sub-node-in test-schema ks)]
                                      (and (= n (.name schema-node))
                                           (= rep (.repetition schema-node))
                                           (= rep-lvl (.repetitionLevel schema-node))
                                           (= def-lvl (.definitionLevel schema-node))))
         [] nil SchemaNode/REQUIRED 0 0
         [:docid] :docid SchemaNode/REQUIRED 0 0
         [:links] :links SchemaNode/OPTIONAL 0 1
         [:links :backward] :backward SchemaNode/LIST 1 2
         [:links :forward] :forward SchemaNode/VECTOR 1 2
         [:name] :name SchemaNode/VECTOR 1 1
         [:name :language] :language SchemaNode/VECTOR 2 2
         [:name :language :code] :code SchemaNode/REQUIRED 2 2
         [:name :language :country] :country SchemaNode/OPTIONAL 2 3
         [:name :url] :url SchemaNode/OPTIONAL 1 2
         [:meta] :meta SchemaNode/MAP 1 1
         [:meta :key] :key SchemaNode/REQUIRED 1 1
         [:meta :val] :val SchemaNode/REQUIRED 1 1
         [:keywords] :keywords SchemaNode/SET 1 1
         [:is-active] :is-active SchemaNode/REQUIRED 0 0))
  (testing "leaf nodes are properly configured in test schema"
    (are [ks t enc com col-idx] (let [^SchemaNode$Leaf leaf-node (sub-node-in test-schema ks)]
                                  (and (= t (.type leaf-node))
                                       (= enc (.encoding leaf-node))
                                       (= com (.compression leaf-node))
                                       (= col-idx (.columnIndex leaf-node))))
         [:docid] Types/LONG Types/DELTA Types/DEFLATE 0
         [:links :backward] Types/LONG Types/PLAIN Types/NONE 1
         [:links :forward] Types/LONG Types/DELTA Types/NONE 2
         [:name :language :code] Types/STRING Types/PLAIN Types/NONE 3
         [:name :language :country] Types/STRING Types/PLAIN Types/NONE 4
         [:name :url] Types/STRING Types/PLAIN Types/NONE 5
         [:meta :key] Types/STRING Types/PLAIN Types/NONE 6
         [:meta :val] Types/STRING Types/PLAIN Types/NONE 7
         [:keywords] Types/STRING Types/PLAIN Types/NONE 8
         [:is-active] Types/BOOLEAN Types/PLAIN Types/NONE 9))
  (testing "record nodes have the proper leaf-column-index"
    (are [ks leaf-column-idx] (let [^SchemaNode$Record schema-record (sub-node-in test-schema ks)]
                                (= leaf-column-idx (.leafColumnIndex schema-record)))
         [] 9
         [:links] 2
         [:name] 5
         [:name :language] 4))
  (testing "schema with collection nodes"
    (let [schema (Schemas/parse types {:foo [[{:bar 'int :baz (Schemas/req 'long)}]]})]
      (are [ks rep rep-lvl def-lvl] (let [^SchemaNode schema-node (sub-node-in schema ks)]
                                      (and (= rep (.repetition schema-node))
                                           (= rep-lvl (.repetitionLevel schema-node))
                                           (= def-lvl (.definitionLevel schema-node))))
           [] SchemaNode/REQUIRED 0 0
           [:foo] SchemaNode/VECTOR 1 1
           [:foo nil] SchemaNode/VECTOR 2 2
           [:foo nil :bar] SchemaNode/OPTIONAL 2 3
           [:foo nil :baz] SchemaNode/REQUIRED 2 2)
      (is (= 1 (.leafColumnIndex ^SchemaNode$Collection (sub-node schema :foo))))
      (is (= 1 (.leafColumnIndex ^SchemaNode$Record (sub-node-in schema [:foo nil])))))))

(deftest invalid-schemas
  (are [schema regex] (thrown-with-msg? IllegalArgumentException regex
                                        (throw-cause (Schemas/parse types schema)))
       'invalid #"Unknown type: 'invalid'"
       {:bar 'invalid} #"Unknown type: 'invalid'"
       (Col. 'int 'invalid) #"Unknown encoding: 'invalid'"
       (Col. 'int 'plain 'invalid) #"Unknown compression: 'invalid'"
       (Col. 'int 'incremental) #"Unsupported encoding 'incremental' for type 'int'"
       (Col. 'string 'delta) #"Unsupported encoding 'delta' for type 'string'"
       (Col. 'int 'incremental) #"Unsupported encoding 'incremental' for type 'int'"
       (Col. 'string 'delta) #"Unsupported encoding 'delta' for type 'string'"
       [(Schemas/req 'int)] #"Repeated field cannot be marked as required"
       (Schemas/req #{'int}) #"Repeated field cannot be marked as required"
       {:foo (Schemas/req [{:foo 'int}])} #"Repeated field cannot be marked as required"
       {:foo [(Schemas/req {:foo 'int})]} #"Repeated field cannot be marked as required"
       {'int (Schemas/req ['int])} #"Repeated field cannot be marked as required"
       {:foo (Schemas/req {'int 'int})} #"Map field cannot be marked as required"
       {:foo (byte-array [1 2 3])} #"Unsupported schema element"
       ['int {:foo :bar}] #"Repeated field can only contain a single schema element"
       {'string 'int 'long 'long} #"Map field can only contain a single key/value schema element"))

(deftest plain-schema
  (is (= {:is-active (Schemas/req 'boolean)
          :keywords #{'string}
          :meta {(Schemas/req 'string) (Schemas/req 'string)}
          :name [{:url 'string :language [{:country 'string, :code (Schemas/req 'string)}]}]
          :links {:forward ['long] :backward '(long)}
          :docid 'long}
         (Schemas/plain types test-unparsed-schema)))
  (is (= {:foo (Schemas/req 'int)} (Schemas/plain types {:foo (Schemas/req (Col. 'int 'vlq))}))))

(deftest entrypoints
  (is (= test-unparsed-schema (->> test-schema (Schemas/subSchema nil) (Schemas/unparse types))))
  (is (= (:links test-unparsed-schema) (->> test-schema (Schemas/subSchema [:links]) (Schemas/unparse types))))
  (is (= (get-in test-unparsed-schema [:links :backward])
         (->> test-schema (Schemas/subSchema [:links :backward]) (Schemas/unparse types))))
  (are [entrypoint regex] (thrown-with-msg? IllegalArgumentException regex
                                            (Schemas/subSchema entrypoint test-schema))
       [:name :language] #"Entrypoint '\[:name :language\]' contains repeated field ':name'"
       [:docid :foo] #"Entrypoint '\[:docid :foo\]' contains leaf node at ':docid'"))
