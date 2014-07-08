(ns dendrite.utils
  (:require [clojure.string :as string]))

(defmacro defalias
  [alias target]
  `(do
     (-> (def ~alias ~target)
         (alter-meta! merge (meta (var ~target))))
     (var ~alias)))

(defn format-ks [ks] (format "[%s]" (string/join " " ks)))
