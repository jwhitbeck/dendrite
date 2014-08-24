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
