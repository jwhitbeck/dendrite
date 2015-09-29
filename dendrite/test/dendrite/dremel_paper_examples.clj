;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.dremel-paper-examples
  (:require [dendrite.test-helpers :as helpers])
  (:import [dendrite.java LeveledValue Schema Schema$QueryResult]))

(def dremel-paper-schema-str
  "#req {:docid #req long
         :links {:forward [long]
                 :backward [long]}
         :name [{:language [{:code #req string
                             :country string}]
                 :url string}]}")

(def dremel-paper-schema
  (->> dremel-paper-schema-str Schema/readString (Schema/parse helpers/default-types)))

(def ^Schema$QueryResult dremel-paper-full-query-schema
  (Schema/applyQuery helpers/default-types true {} dremel-paper-schema '_))

(def dremel-paper-record1
  {:docid 10
   :links {:forward [20 40 60]}
   :name [{:language [{:code "en-us" :country "us"}
                      {:code "en"}]
           :url "http://A"}
          {:url "http://B"}
          {:language [{:code "en-gb" :country "gb"}]}]})

(def dremel-paper-record1-striped
  [10
   [(LeveledValue. 0 4 20) (LeveledValue. 1 4 40) (LeveledValue. 1 4 60)]
   [(LeveledValue. 0 1 nil)]
   [(LeveledValue. 0 6 "en-us") (LeveledValue. 2 6 "en") (LeveledValue. 1 3 nil)
    (LeveledValue. 1 6 "en-gb")]
   [(LeveledValue. 0 7 "us") (LeveledValue. 2 6 nil) (LeveledValue. 1 3 nil) (LeveledValue. 1 7 "gb")]
   [(LeveledValue. 0 4 "http://A") (LeveledValue. 1 4 "http://B") (LeveledValue. 1 3 nil)]])

(def dremel-paper-record2
  {:docid 20
   :links {:backward [10 30]
           :forward [80]}
   :name [{:url "http://C"}]})

(def dremel-paper-record2-striped
  [20
   [(LeveledValue. 0 4 80)]
   [(LeveledValue. 0 4 10) (LeveledValue. 1 4 30)]
   [(LeveledValue. 0 3 nil)]
   [(LeveledValue. 0 3 nil)]
   [(LeveledValue. 0 4 "http://C")]])
