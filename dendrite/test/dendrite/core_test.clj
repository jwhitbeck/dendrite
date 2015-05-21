;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.core-test
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [dendrite.core2 :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Options Schema])
  (:refer-clojure :exclude [read pmap]))

(set! *warn-on-reflection* true)

(def tmp-filename "target/foo.dend")

(deftest invalid-options-are-caught
  (testing "writer options"
    (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                      (Options/getWriterOptions opts))
         {:record-group-length (inc Integer/MAX_VALUE)}
         ":record-group-length expects a positive int but got '2147483648'"
         {:record-group-length "foo"}
         ":record-group-length expects a positive int but got 'foo'"
         {:record-group-length nil}
         ":record-group-length expects a positive int but got 'null'"
         {:record-group-length -1.5}
         ":record-group-length expects a positive int but got '-1.5'"
         {:data-page-length "foo"}
         ":data-page-length expects a positive int but got 'foo'"
         {:data-page-length nil}
         ":data-page-length expects a positive int but got 'null'"
         {:data-page-length -1.5}
         ":data-page-length expects a positive int but got '-1.5'"
         {:optimize-columns? "foo"}
         ":optimize-columns\\? expects one of :all, :none, or :default but got 'foo'"
         {:compression-thresholds :deflate}
         ":compression-thresholds expects a map."
         {:compression-thresholds {:deflate -0.2}}
         ":compression-thresholds expects its keys to be symbols but got ':deflate'"
         {:compression-thresholds {'deflate -0.2}}
         ":compression-thresholds expects its values to be positive"
         {:custom-types "foo"}
         ":custom-types expects a map but got 'foo'"
         {:invalid-input-handler "foo"}
         ":invalid-input-handler expects a function"
         {:invalid-option "foo"}
         ":invalid-option is not a supported writer option"))
  (testing "reader options"
    (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                      (Options/getReaderOptions opts))
         {:custom-types "foo"}
         ":custom-types expects a map but got 'foo'"
         {:invalid-option "foo"}
         ":invalid-option is not a supported reader option."))
  (testing "read options"
    (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                      (Options/getReadOptions opts))
         {:entrypoint :foo}
         ":entrypoint expects a seqable object but got ':foo'"
         {:missing-fields-as-nil? nil}
         ":missing-fields-as-nil\\? expects a boolean but got 'null'"
         {:missing-fields-as-nil? "foo"}
         ":missing-fields-as-nil\\? expects a boolean but got 'foo'"
         {:readers "foo"}
         ":readers expects a map but got 'foo'"
         {:invalid-option "foo"}
         ":invalid-option is not a supported read option.")))

(defn- dremel-paper-writer ^java.io.Closeable []
  (doto (writer (Schema/readString dremel-paper-schema-str) tmp-filename)
    (.write dremel-paper-record1)
    (.write dremel-paper-record2)))

(deftest dremel-paper
  (.close (dremel-paper-writer))
  (with-open [r (reader tmp-filename)]
    (testing "full schema"
      (is (= [dremel-paper-record1 dremel-paper-record2] (read r))))
    (testing "two fields example"
      (is (= [{:docid 10
               :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
              {:docid 20 :name [nil]}]
             (read {:query {:docid '_ :name [{:language [{:country '_}]}]}} r)))))
  (io/delete-file tmp-filename))

(deftest file-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (writer (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (testing "full schema"
      (is (= records (with-open [r (reader tmp-filename)]
                       (doall (read r))))))
    (testing "one field"
      (is (= (map #(select-keys % [:docid]) records)
             (with-open [r (reader tmp-filename)]
               (doall (read {:query {:docid '_}} r))))))
    (io/delete-file tmp-filename)))

(deftest empty-file-write-read
  (.close (writer (Schema/readString helpers/test-schema-str) tmp-filename))
  (with-open [r (reader tmp-filename)]
    (is (empty? (read r)))
    (is (zero? (-> r stats :global :data-length)))
    (is (pos? (-> r stats :global :length))))
  (io/delete-file tmp-filename))

(deftest flat-base-type-write-read
  (testing "required"
    (let [records (repeatedly 100 #(rand-int 100))]
      (with-open [w (writer (Schema/req 'int) tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (reader tmp-filename)]
                       (doall (read r)))))))
  (testing "optional"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-map 0.1 (constantly nil)) (take 100))]
      (with-open [w (writer 'int tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (reader tmp-filename)]
                       (doall (read r)))))))
  (io/delete-file tmp-filename))

(deftest flat-repeated-type-write-read
  (testing "repeated optional base type"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-map 0.1 (constantly nil))
                       (partition 5) (take 20))]
      (with-open [w (writer ['int] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (reader tmp-filename)]
                       (doall (read r)))))))
  (testing "repeated required base type"
    (let [records (->> (repeatedly #(rand-int 100)) (partition 5) (take 20))]
      (with-open [w (writer [(Schema/req 'int)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (reader tmp-filename)]
                       (doall (read r)))))))
  (testing "repeated records"
    (let [records (->> (helpers/rand-test-records) (partition 5) (take 20))]
      (with-open [w (writer [(Schema/readString helpers/test-schema-str)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (reader tmp-filename)]
                       (doall (read r)))))))
  (io/delete-file tmp-filename))
