(defproject dendrite.benchmarks "0.1.0-SNAPSHOT"
  :description "Benchmark code for dendrite, a Dremel-like columnar storage format for Clojure."
  :url "https://github.com/jwhitbeck/dendrite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-opts ["-server" "-Xmx4g"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cheshire "5.3.1"]
                 [dendrite "0.1.0-SNAPSHOT"]
                 [http-kit "2.1.16"]
                 [ring/ring-codec "1.0.0"]])
