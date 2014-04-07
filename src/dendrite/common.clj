(ns dendrite.common)

(defrecord WrappedValue [repetition-level definition-level value])

(defn wrap-value [repetition-level definition-level value]
  (WrappedValue. repetition-level definition-level value))
