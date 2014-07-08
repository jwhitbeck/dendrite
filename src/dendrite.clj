(ns dendrite
  (:require [dendrite.core :as core]
            [dendrite.schema :as s]
            [dendrite.utils :refer [defalias]])
  (:refer-clojure :exclude [read]))

(defalias byte-buffer-writer core/byte-buffer-writer)
(defalias byte-buffer-reader core/byte-buffer-reader)

(defalias write! core/write!)
(defalias set-metadata! core/set-metadata!)
(defalias byte-buffer! core/byte-buffer!)

(defalias read core/read)
(defalias stats core/stats)
(defalias metadata core/metadata)
(defalias schema core/schema)

(defalias col s/col)
(defalias req s/req)
(defalias tag s/tag)
