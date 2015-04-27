;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.compression
  (:import [dendrite.java Deflate$Compressor Deflate$Decompressor]))

(set! *warn-on-reflection* true)

(defn- invalid-compression-type-exception [x]
  (IllegalArgumentException. (str x " is not a valid compression-type. "
                                  "Supported types are: :none, :deflate.")))

(defn compressor [compression-type]
  (case compression-type
    :none nil
    :deflate (Deflate$Compressor.)
    (throw (invalid-compression-type-exception compression-type))))

(defn decompressor-ctor [compression-type]
  (case compression-type
    :none (constantly nil)
    :deflate #(Deflate$Decompressor.)
    (throw (invalid-compression-type-exception compression-type))))
