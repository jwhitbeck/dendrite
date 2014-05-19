(ns dendrite.schema-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [dendrite.schema :refer :all]
            [dendrite.test-helpers :refer [test-schema-str]])
  (:refer-clojure :exclude [read-string val]))

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

(deftest schema-annotation
  (testing "Value types are properly annotated"
    (let [schema (-> test-schema-str read-string parse)]
      (is (= :required (:repetition schema)))
      (are [ks m] (let [field (sub-field-in schema ks)
                        vt (:value field)]
                    (and (= (:column-index vt) (:column-index m))
                         (= (:repetition-level field) (:repetition-level m))
                         (= (:definition-level vt) (:definition-level m))
                         (= (:nested? vt) (:nested? m))))
           [:docid] {:column-index 0 :repetition-level 0 :definition-level 0 :nested? false}
           [:links :backward] {:column-index 1 :repetition-level 1 :definition-level 2 :nested? true}
           [:links :forward] {:column-index 2 :repetition-level 1 :definition-level 2 :nested? true}
           [:name :language :code] {:column-index 3 :repetition-level 2 :definition-level 2 :nested? true}
           [:name :language :country] {:column-index 4 :repetition-level 2 :definition-level 3 :nested? true}
           [:name :url] {:column-index 5 :repetition-level 1 :definition-level 2 :nested? true}
           [:meta :key] {:column-index 6 :repetition-level 1 :definition-level 1 :nested? true}
           [:meta :value] {:column-index 7 :repetition-level 1 :definition-level 1 :nested? true}
           [:keywords] {:column-index 8 :repetition-level 1 :definition-level 1 :nested? true}))))

(deftest invalid-schemas
  (testing "unsupported types"
    (is (thrown? IllegalArgumentException (parse {:foo 'invalid})))
    (is (thrown? IllegalArgumentException (parse {:foo {:bar 'invalid}}))))
  (testing "mismatched type and encodings"
    (is (thrown? IllegalArgumentException (parse {:foo (val {:type :int :encoding :incremental})})))
    (is (thrown? IllegalArgumentException (parse {:foo {:var (val {:type :string :encoding :delta})}}))))
  (testing "unsupported compression types"
    (is (thrown? IllegalArgumentException (parse {:foo (val {:type :int :compression :snappy})}))))
  (testing "marking a field as both reapeated and required"
    (is (thrown? IllegalArgumentException (parse {:foo (req ['int])})))))
