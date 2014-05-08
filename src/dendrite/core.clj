(ns dendrite.core)

(defrecord WrappedValue [repetition-level definition-level value])

(def wrap-value ->WrappedValue)
