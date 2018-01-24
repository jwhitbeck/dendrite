(defproject dendrite "0.5.9"
  :description "A Dremel-like columnar storage format for Clojure."
  :url "https://github.com/jwhitbeck/dendrite"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]]
  :java-source-paths ["java-src"]
  :scm {:name "git"
        :url "https://github.com/jwhitbeck/dendrite"}
  :profiles {:dev {:java-opts ["-server"]
                   :plugins [[codox "0.8.13"]]
                   :codox {:output-dir "../docs/api"}}})
