;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

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
