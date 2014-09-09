(defproject dendrite "0.1.1-SNAPSHOT"
  :description "A Dremel-like columnar storage format for Clojure."
  :url "https://github.com/jwhitbeck/dendrite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [net.jpountz.lz4/lz4 "1.2.0"]
                 [org.clojure/data.fressian "0.2.0"]]
  :java-source-paths ["java-src"])
