(ns dendrite.common
  (:require [clojure.string :as string]))

(set! *warn-on-reflection* true)

(defrecord LeveledValue [repetition-level definition-level value])

(def leveled-value ->LeveledValue)

(defn format-ks [ks] (format "[%s]" (string/join " " ks)))
