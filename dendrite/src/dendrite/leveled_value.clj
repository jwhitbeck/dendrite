(ns dendrite.leveled-value
  (:import [dendrite.java LeveledValue]
           [java.io Writer]))

(set! *warn-on-reflection* true)

(definline ->LeveledValue [repetition-level definition-level value]
  `(LeveledValue. (int ~repetition-level) (int ~definition-level) ~value))

(defmethod print-method LeveledValue
  [^LeveledValue lv ^Writer w]
  (.write w (format "#<LeveledValue[r:%d, d:%d, v:%s]>"
                    (.repetitionLevel lv) (.definitionLevel lv) (.value lv))))

(defn apply-fn [^LeveledValue leveled-value f]
  (let [v (.value leveled-value)]
    (if (nil? v)
      leveled-value
      (.apply leveled-value f))))
