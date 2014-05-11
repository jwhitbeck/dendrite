(ns dendrite.schema-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [dendrite.schema :refer :all]))

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
    (is (= (read-schema-str human-readable-schema-str)
           (-> human-readable-schema-str read-schema-str str read-schema-str))))
  (testing "Pars human-reable schema"
    (is (= (read-schema-str human-readable-schema-str)
           (-> human-readable-schema-str read-schema-str parse-schema human-readable)))))

(deftest schema-serialization
  (testing "Fressian serialization"
    (let [schema (-> human-readable-schema-str read-schema-str parse-schema)]
      (is (= schema
             (-> (fressian/write schema) fressian/read))))))
