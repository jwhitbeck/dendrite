(defproject dendrite.benchmarks "0.1.0-SNAPSHOT"
  :description "Benchmark code for dendrite, a Dremel-like columnar storage format for Clojure."
  :url "https://github.com/jwhitbeck/dendrite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :java-opts ["-server" "-Xmx4g"]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cheshire "5.3.1"]
                 [com.taoensso/nippy "2.7.0-RC1"]
                 [dendrite "0.1.0-SNAPSHOT"]
                 [net.jpountz.lz4/lz4 "1.2.0"]
                 [http-kit "2.1.16"]
                 [ring/ring-codec "1.0.0"]
                 [org.clojure/data.fressian "0.2.0"]])
