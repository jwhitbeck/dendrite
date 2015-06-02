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
            [dendrite.core :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Schema]
           [java.util Date Calendar])
  (:refer-clojure :exclude [read pmap]))

(set! *warn-on-reflection* true)

(def tmp-filename "target/foo.dend")
(def tmp-filename2 "target/bar.dend")

(use-fixtures :each (fn [f]
                      (f)
                      (io/delete-file tmp-filename true)
                      (io/delete-file tmp-filename2 true)))

(defn- dremel-paper-writer
  (^dendrite.java.FileWriter [] (dremel-paper-writer tmp-filename))
  (^dendrite.java.FileWriter [filename]
    (doto (file-writer (Schema/readString dremel-paper-schema-str) filename)
      (.write dremel-paper-record1)
      (.write dremel-paper-record2))))

(deftest dremel-paper
  (.close (dremel-paper-writer))
  (with-open [r (file-reader tmp-filename)]
    (testing "full schema"
      (is (= [dremel-paper-record1 dremel-paper-record2] (read r))))
    (testing "two fields example"
      (is (= [{:docid 10
               :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
              {:docid 20 :name [nil]}]
             (read {:query {:docid '_ :name [{:language [{:country '_}]}]}} r))))))

(deftest file-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (testing "full schema"
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r))))))
    (testing "one field"
      (is (= (map #(select-keys % [:docid]) records)
             (with-open [r (file-reader tmp-filename)]
               (doall (read {:query {:docid '_}} r))))))))

(deftest empty-file-write-read
  (.close (file-writer (Schema/readString helpers/test-schema-str) tmp-filename))
  (with-open [r (file-reader tmp-filename)]
    (is (empty? (read r)))
    (is (zero? (-> r stats :global :data-length)))
    (is (pos? (-> r stats :global :length)))))

(deftest flat-base-type-write-read
  (testing "required"
    (let [records (repeatedly 100 #(rand-int 100))]
      (with-open [w (file-writer (Schema/req 'int) tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r)))))))
  (testing "optional"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-map 0.1 (constantly nil)) (take 100))]
      (with-open [w (file-writer 'int tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r))))))))

(deftest flat-repeated-type-write-read
  (testing "repeated optional base type"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-map 0.1 (constantly nil))
                       (partition 5) (take 20))]
      (with-open [w (file-writer ['int] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r)))))))
  (testing "repeated required base type"
    (let [records (->> (repeatedly #(rand-int 100)) (partition 5) (take 20))]
      (with-open [w (file-writer [(Schema/req 'int)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r)))))))
  (testing "repeated records"
    (let [records (->> (helpers/rand-test-records) (partition 5) (take 20))]
      (with-open [w (file-writer [(Schema/readString helpers/test-schema-str)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r))))))))

(deftest automatic-schema-optimization
  (let [records (take 100 (helpers/rand-test-records))
        test-schema (Schema/readString helpers/test-schema-str)]
    (with-open [w (file-writer {:optimize-columns? :all :compression-thresholds {}}
                               test-schema
                               tmp-filename)]
      (.writeAll w records))
    (with-open [r (file-reader tmp-filename)]
      (testing "schema is indeed optimized"
        (is (= (str "#req "
                    "{:docid #req #col [long delta],"
                    " :links {:backward (long), :forward [long]},"
                    " :name [{:language [{:code #req #col [string dictionary],"
                    " :country #col [string dictionary]}],"
                    " :url #col [string incremental]}],"
                    " :meta {#req #col [string dictionary] #req #col [string dictionary]},"
                    " :keywords #{#req #col [string dictionary]},"
                    " :is-active #req boolean,"
                    " :ngrams [[#req #col [string dictionary]]]}")
               (str (schema r)))))
      (testing "stats"
        (is (not (nil? (stats r)))))
      (testing "full schema"
        (is (= records (read r))))
      (testing "one field"
        (is (= (map #(select-keys % [:is-active]) records)
               (read {:query {:is-active '_}} r)))))))

(deftest user-defined-metadata
  (let [test-metadata {:foo {:bar "test"} :baz [1 2 3]}]
    (with-open [w (dremel-paper-writer)]
      (set-metadata! w test-metadata))
    (is (= test-metadata (with-open [r (file-reader tmp-filename)]
                           (metadata r))))))

(deftest corrupt-data
  (testing "corrupt file"
    (spit tmp-filename "random junk")
    (is (thrown-with-msg? IllegalStateException #"File is not a valid dendrite file."
                          (with-open [f (file-reader tmp-filename)]))))
  (testing "interrupted write"
    (spit tmp-filename "den1")
    (is (thrown-with-msg? IllegalStateException #"File is not a valid dendrite file."
                          (with-open [f (file-reader tmp-filename)])))))


(deftest record-group-lengths
  (let [records (take 10000 (helpers/rand-test-records))]
    (letfn [(avg-record-group-length [target-length]
              (with-open [w (file-writer {:record-group-length target-length}
                                         (Schema/readString helpers/test-schema-str)
                                         tmp-filename)]
                (.writeAll w records))
              (with-open [r (file-reader tmp-filename)]
                (->> r stats :record-groups butlast (map :length) helpers/avg)))]
      (testing "record-group lengths are approximately equal to record-group-length"
        (is (helpers/roughly (* 300 1024) (avg-record-group-length (* 300 1024))))
        (is (helpers/roughly (* 100 1024) (avg-record-group-length (* 100 1024))))
        (is (= records (with-open [r (file-reader tmp-filename)]
                         (doall (read r)))))))))

(defn- throw-foo-fn [& args] (throw (Exception. "foo")))

(deftest errors
  (testing "exceptions in the writing thread are caught in the main thread"
    (is (thrown? IllegalStateException
         (with-open [w (dremel-paper-writer)]
           (.write w nil)))))
  (testing "exceptions in the reading threads are caught in the main thread"
    (.close (dremel-paper-writer))
    (is (thrown-with-msg?
         Exception #"foo"
         (with-open [r (file-reader tmp-filename)]
           (pmap throw-foo-fn r)))))
  (testing "missing file exception"
    (is (thrown-with-msg?
         java.nio.file.NoSuchFileException #"/no/such/file"
         (with-open [r (file-reader "/no/such/file")]
           (read r))))))

(deftest invalid-records
  (let [bad-record {:docid "not-a-number"}]
    (testing "invalid records trigger an exception while writing"
      (is (thrown-with-msg?
           IllegalArgumentException #"Failed to stripe record '\{:docid \"not-a-number\"\}"
           (helpers/throw-cause
            (helpers/throw-cause
             (helpers/throw-cause
              (helpers/throw-cause
               (with-open [w (dremel-paper-writer)]
                 (.write w bad-record)))))))))
    (testing "invalid-input-handler can process exceptions"
      (let [error-atom (atom nil)]
        (with-open [w (file-writer {:invalid-input-handler (fn [record e] (reset! error-atom record))}
                                   (Schema/readString dremel-paper-schema-str)
                                   tmp-filename)]
          (.write w bad-record)
          (.write w dremel-paper-record1)
          (.write w dremel-paper-record2))
        (is (= @error-atom bad-record))
        (is (= [dremel-paper-record1 dremel-paper-record2]
               (with-open [r (file-reader tmp-filename)]
                 (doall (read r)))))))))

(deftest strict-queries
  (testing "queries fail when missing-fields-as-nil? is false and we query a missing field."
    (.close (dremel-paper-writer))
    (with-open [r (file-reader tmp-filename)]
      (is (= [nil nil] (read {:query {:foo '_}} r)))
      (is (= [nil nil] (r/reduce conj [] (read {:query {:foo '_}} r))))
      (is (thrown-with-msg?
           IllegalArgumentException #"The following fields don't exist: \[:foo\]"
           (helpers/throw-cause (read {:query {:foo '_} :missing-fields-as-nil? false} r)))))))

(deftest entrypoints
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (testing "sub records"
      (is (= (map :links records) (with-open [r (file-reader tmp-filename)]
                                    (doall (read {:entrypoint [:links]} r))))))
    (testing "access a column directly"
      (is (= (map :docid records) (with-open [r (file-reader tmp-filename)]
                                    (doall (read {:entrypoint [:docid]} r))))))
    (testing "access a repeated column"
      (is (= (map #(get-in % [:links :backward]) records)
             (with-open [r (file-reader tmp-filename)]
               (doall (read {:entrypoint [:links :backward]} r))))))))

(deftest readers
  (testing "readers functions transform output"
    (.close (dremel-paper-writer))
    (with-open [r (file-reader tmp-filename)]
      (is (= [{:name 3, :docid 10} {:name 1, :docid 20}]
             (read {:query {:docid '_ :name (Schema/tag 'foo '_)} :readers {'foo count}} r)))
      (is (thrown-with-msg?
           IllegalArgumentException #"No reader function was provided for tag 'foo'."
           (helpers/throw-cause
            (read {:query {:docid '_ :name (Schema/tag 'foo '_)}} r))))))
  (testing "reader functions behave properly on missing fields"
    (with-open [r (file-reader tmp-filename)]
      (is (= [true true]
             (read {:query (Schema/tag 'foo {:foo '_}) :readers {'foo empty?}} r)))
      (is (= [{:docid 10 :foo {:bar 0}} {:docid 20 :foo {:bar 0}}]
             (read {:query {:docid '_ :foo {:bar (Schema/tag 'bar [{:baz '_}])}} :readers {'bar count}} r))))))

(deftest custom-types
  (testing "write/read custom types"
    (let [t1 (.getTime (Calendar/getInstance))
          t2 (-> (doto (Calendar/getInstance) (.add Calendar/DATE 1)) .getTime)
          records [{:docid 1 :at t1} {:docid 2 :at t2}]
          records-with-timestamps [{:docid 1 :at (.getTime t1)} {:docid 2 :at (.getTime t2)}]
          custom-types {'test-type {:base-type 'long
                                    :coercion-fn identity
                                    :to-base-type-fn #(.getTime ^Date %)
                                    :from-base-type-fn #(Date. (long %))}}]
      (testing "throw error when the writer is not passed the :custom types option"
        (is (thrown-with-msg?
             IllegalArgumentException #"Unknown type: 'test-type'"
             (helpers/throw-cause (with-open [w (file-writer {:docid 'long :at 'test-type} tmp-filename)]
                                    (.writeAll w records))))))
      (testing "throw error when invalid field is defined in custom-types"
        (is (thrown-with-msg?
             IllegalArgumentException #":invalid is not a valid custom type definition key"
             (helpers/throw-cause
              (.close (file-writer {:custom-types {'test-type {:invalid 'bar}}}
                                   {:docid 'long :at 'test-type}
                                   tmp-filename))))))
      (with-open [w (file-writer {:custom-types custom-types} {:docid 'long :at 'test-type} tmp-filename)]
        (.writeAll w records))
      (testing "read as derived type when :custom-types option is passed"
        (is (= records (with-open [r (file-reader {:custom-types custom-types} tmp-filename)]
                         (doall (read r))))))
      (testing "read as base type when :custom-types option is not passed"
        (is (= records-with-timestamps (with-open [r (file-reader tmp-filename)]
                                         (doall (read r)))))))))

(deftest pmap-convenience-function
  (.close (dremel-paper-writer))
  (is (= [3 1] (with-open [r (file-reader tmp-filename)]
                 (doall (pmap (comp count :name) r))))))

(deftest chunkiness
  (.close (dremel-paper-writer))
  (is (chunked-seq? (with-open [r (file-reader tmp-filename)]
                      (doall (next (read r)))))))

(deftest folding
  (.close (dremel-paper-writer))
  (with-open [r (file-reader tmp-filename)]
    (is (= 30 (->> (read r) (r/map :docid) (r/fold +))))
    (is (= 30 (->> (read r) (r/map :docid) (r/reduce +))))
    (is (= 40 (->> (read r) (r/map :docid) (r/reduce + 10))))))

(deftest multiple-files
  (.close (doto (dremel-paper-writer tmp-filename)
            (set-metadata! :foo)))
  (.close (doto (dremel-paper-writer tmp-filename2)
            (set-metadata! :bar)))
  (testing "reads"
    (is (= (concat (with-open [r (file-reader tmp-filename)]
                     (doall (read r)))
                   (with-open [r (file-reader tmp-filename2)]
                     (doall (read r))))
           (with-open [r (files-reader [tmp-filename tmp-filename2])]
             (doall (read r))))))
  (testing "stats"
    (is (= {(io/as-file tmp-filename) (with-open [r (file-reader tmp-filename)]
                                          (stats r))
            (io/as-file tmp-filename2) (with-open [r (file-reader tmp-filename2)]
                                         (stats r))}
           (with-open [r (files-reader [tmp-filename tmp-filename2])]
             (stats r)))))
  (testing "metadata"
    (is (= {(io/as-file tmp-filename) (with-open [r (file-reader tmp-filename)]
                                        (metadata r))
            (io/as-file tmp-filename2) (with-open [r (file-reader tmp-filename2)]
                                         (metadata r))}
           (with-open [r (files-reader [tmp-filename tmp-filename2])]
             (metadata r)))))
  (testing "schema"
    (is (= {(io/as-file tmp-filename) (with-open [r (file-reader tmp-filename)]
                                        (schema r))
            (io/as-file tmp-filename2) (with-open [r (file-reader tmp-filename2)]
                                         (schema r))}
           (with-open [r (files-reader [tmp-filename tmp-filename2])]
             (schema r))))))

(deftest writer-with-map-fn
  (let [records (take 100 (helpers/rand-test-records))
        f #(select-keys % [:docid :is-active])]
    (with-open [w (file-writer {:map-fn f} (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (is (= (map f records)
           (with-open [r (file-reader tmp-filename)]
             (doall (read r)))))))
