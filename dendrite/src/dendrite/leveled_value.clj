(ns dendrite.leveled-value
  (:import [dendrite.java LeveledValue]))

(set! *warn-on-reflection* true)

(defmacro ->LeveledValue [repetition-level definition-level value]
  `(LeveledValue. (int ~repetition-level) (int ~definition-level) ~value))
