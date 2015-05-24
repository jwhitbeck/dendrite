(defproject dendrite.benchmarks "0.1.0-SNAPSHOT"
  :description "Benchmark code for dendrite, a Dremel-like columnar storage format for Clojure."
  :url "https://github.com/jwhitbeck/dendrite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-opts ["-server" "-Xms4g" "-Xmx8g"]
  :main dendrite.benchmarks.core
  :plugins [[lein-protobuf "0.4.1"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cheshire "5.4.0"]
                 [com.damballa/abracad "0.4.12"]
                 [com.google.protobuf/protobuf-java "2.6.1"]
                 [com.taoensso/nippy "2.8.0"]
                 [dendrite "0.3.0-SNAPSHOT"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.flatland/protobuf "0.8.1"]
                 [prismatic/plumbing "0.4.4"]
                 [net.jpountz.lz4/lz4 "1.3.0"]
                 [clj-http "1.1.2"]
                 [ring/ring-codec "1.0.0"]
                 [org.clojure/data.fressian "0.2.0"]])
