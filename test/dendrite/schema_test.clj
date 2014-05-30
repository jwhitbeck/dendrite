(ns dendrite.schema-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
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

(deftest schema-serialization
  (testing "fressian serialization"
    (let [schema (-> test-schema-str read-string parse)]
      (is (= schema
             (-> (fressian/write schema) fressian/read))))))

(defn sub-field [field k] (->> field :sub-fields (filter #(= (:name %) k)) first))

(defn sub-field-in [field [k & ks]]
  (if (empty? ks)
    (sub-field field k)
    (sub-field-in (sub-field field k) ks)))

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
