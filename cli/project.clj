(defproject dendrite.cli "0.1.0-SNAPSHOT"
  :description "CLI tool for dendrite, a Dremel-like columnar storage format for Clojure."
  :url "https://github.com/jwhitbeck/dendrite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main dendrite.cli.core
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [dendrite "0.5.9"]
                 [org.clojure/tools.cli "0.3.1"]]
  :profiles {:uberjar {:aot :all}})
