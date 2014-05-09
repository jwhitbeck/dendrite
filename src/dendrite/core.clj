(ns dendrite.core)

(set! *warn-on-reflection* true)

(defrecord WrappedValue [repetition-level definition-level value])

(def wrap-value ->WrappedValue)
