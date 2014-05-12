(ns dendrite.schema-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [dendrite.schema :refer :all]
            [dendrite.test-helpers :refer [test-schema-str]])
  (:refer-clojure :exclude [read-string]))

(deftest parse-human-readable-schema-str
  (testing "Write/read human-readable schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string str read-string))))
  (testing "Parse human-reable schema"
    (is (= (read-string test-schema-str)
           (-> test-schema-str read-string parse human-readable)))))

(deftest schema-serialization
  (testing "Fressian serialization"
    (let [schema (-> test-schema-str read-string parse)]
      (is (= schema
             (-> (fressian/write schema) fressian/read))))))
