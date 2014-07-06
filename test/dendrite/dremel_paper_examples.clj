(ns dendrite.dremel-paper-examples
  (:require [dendrite.common :refer :all]
            [dendrite.schema :as schema]))

(def dremel-paper-schema-str
  "{:docid #req long
    :links {:forward [long]
            :backward [long]}
    :name [{:language [{:code #req string
                        :country string}]
            :url string}]}")

(def dremel-paper-schema (-> dremel-paper-schema-str schema/read-string schema/parse))

(def dremel-paper-full-query-schema (schema/apply-query dremel-paper-schema '_))

(def dremel-paper-record1
  {:docid 10
   :links {:forward [20 40 60]}
   :name [{:language [{:code "en-us" :country "us"}
                      {:code "en"}]
           :url "http://A"}
          {:url "http://B"}
          {:language [{:code "en-gb" :country "gb"}]}]})

(def dremel-paper-record1-striped
  [[(->LeveledValue 0 0 10)]
   [(->LeveledValue 0 2 20) (->LeveledValue 1 2 40) (->LeveledValue 1 2 60)]
   [(->LeveledValue 0 1 nil)]
   [(->LeveledValue 0 2 "en-us") (->LeveledValue 2 2 "en") (->LeveledValue 1 1 nil)
    (->LeveledValue 1 2 "en-gb")]
   [(->LeveledValue 0 3 "us") (->LeveledValue 2 2 nil) (->LeveledValue 1 1 nil) (->LeveledValue 1 3 "gb")]
   [(->LeveledValue 0 2 "http://A") (->LeveledValue 1 2 "http://B") (->LeveledValue 1 1 nil)]])

(def dremel-paper-record2
  {:docid 20
   :links {:backward [10 30]
           :forward [80]}
   :name [{:url "http://C"}]})

(def dremel-paper-record2-striped
  [[(->LeveledValue 0 0 20)]
   [(->LeveledValue 0 2 80)]
   [(->LeveledValue 0 2 10) (->LeveledValue 1 2 30)]
   [(->LeveledValue 0 1 nil)]
   [(->LeveledValue 0 1 nil)]
   [(->LeveledValue 0 2 "http://C")]])
