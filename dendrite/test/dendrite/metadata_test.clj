;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.metadata-test
  (:require [clojure.data.fressian :as fressian]
            [clojure.test :refer :all]
            [dendrite.metadata :refer :all]
            [dendrite.schema :as schema]
            [dendrite.test-helpers :refer [default-type-store test-schema-str]])
  (:refer-clojure :exclude [read]))

(set! *warn-on-reflection* true)

(deftest schema-serialization
  (testing "fressian schema serialization"
    (let [s (-> test-schema-str schema/read-string (schema/parse default-type-store))]
      (is (= s (read (write s)))))))
