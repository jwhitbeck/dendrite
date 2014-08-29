;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.core
  (:require [dendrite.impl :as impl]
            [dendrite.schema :as schema]
            [dendrite.utils :refer [defalias]])
  (:refer-clojure :exclude [read pmap]))

(defalias byte-buffer-writer impl/byte-buffer-writer)
(defalias byte-buffer-reader impl/byte-buffer-reader)

(defalias file-writer impl/file-writer)
(defalias file-reader impl/file-reader)

(defalias write! impl/write!)
(defalias set-metadata! impl/set-metadata!)
(defalias update-metadata! impl/update-metadata!)
(defalias byte-buffer! impl/byte-buffer!)

(defalias read impl/read)
(defalias pmap impl/pmap)
(defalias stats impl/stats)
(defalias metadata impl/metadata)
(defalias schema impl/schema)

(defalias read-schema-string schema/read-string)
(defalias pretty schema/pretty)
(defalias col schema/col)
(defalias req schema/req)
(defalias tag schema/tag)
