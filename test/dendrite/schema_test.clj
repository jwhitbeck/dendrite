(ns dendrite.schema-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [dendrite.schema :refer :all])
  (:refer-clojure :exclude [read-string]))

(def human-readable-schema-str
  "{:docid #req (int64 :delta :lz4)
    :links {:backward [(int64)]
            :forward [(int64)]}
    :name [{:language [{:code #req (string)
                        :country (string)}]
           :url (string)}]
    :meta {(string) (string)}
    :keywords #{(string)}}")

(deftest parse-human-readable-schema-str
  (testing "Write/read human-readable schema"
    (is (= (read-string human-readable-schema-str)
           (-> human-readable-schema-str read-string str read-string))))
  (testing "Parse human-reable schema"
    (is (= (read-string human-readable-schema-str)
           (-> human-readable-schema-str read-string parse human-readable)))))

(deftest schema-serialization
  (testing "Fressian serialization"
    (let [schema (-> human-readable-schema-str read-string parse)]
      (is (= schema
             (-> (fressian/write schema) fressian/read))))))
