(ns dendrite.leveled-value)

(set! *warn-on-reflection* true)

(defrecord LeveledValue [repetition-level definition-level value])
