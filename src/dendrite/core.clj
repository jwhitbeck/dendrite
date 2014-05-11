(ns dendrite.core)

(set! *warn-on-reflection* true)

(defrecord LeveledValue [repetition-level definition-level value])

(def leveled-value ->LeveledValue)
