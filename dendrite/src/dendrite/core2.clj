;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.core2
  (:require [clojure.pprint :as pprint])
  (:import [dendrite.java Col Schemas]
           [java.io Writer]))

(set! *warn-on-reflection* true)

(defn col
  "Returns a column specification. Takes one to three arguments:
  - type:        the column type symbol (e.g. int)
  - encoding:    the column encoding symbol (default: plain)
  - compression: the column compression symbol (default: none)

  See README for all supported encoding/compression types."
  ([type] (Col. type))
  ([type encoding] (Col. type encoding))
  ([type encoding compression] (Col. type encoding compression)))

(defmethod print-method Col
  [v ^Writer w]
  (.write w (str v)))

(defmethod pprint/simple-dispatch Col
  [v]
  (.write *out* (str v)))

(defn req
  "Marks the enclosed schema element as required."
  [x]
  (Schemas/req x))

(defmethod print-method Schemas/REQUIRED
  [v ^Writer w]
  (.write w "#req ")
  (print-method (Schemas/unreq v) w))

(defmethod pprint/simple-dispatch Schemas/REQUIRED
  [v]
  (.write *out* "#req ")
  (pprint/simple-dispatch (Schemas/unreq v)))

(defn tag
  "Tags the enclosed query element with the provided tag. Meant to be used in combination with the :readers
  option."
  [tag elem]
  (Schemas/tag tag elem))

(defmethod print-method Schemas/TAGGED
  [v ^Writer w]
  (.write w (format "#%s " (-> v Schemas/getTag name)))
  (print-method (Schemas/untag v) w))

(defn read-schema-string
  "Parse an edn-formatted dendrite schema string."
  [s]
  (Schemas/readString s))
