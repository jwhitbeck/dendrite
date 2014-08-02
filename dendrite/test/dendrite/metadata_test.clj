(ns dendrite.metadata-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [dendrite.metadata :refer :all]
            [dendrite.schema :as schema]
            [dendrite.test-helpers :refer [test-schema-str]])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(deftest schema-serialization
  (testing "fressian schema serialization"
    (let [s (-> test-schema-str schema/read-string schema/parse)]
      (is (= s (read (write s)))))))
