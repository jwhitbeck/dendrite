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
  (:import [dendrite.java Options Schema]
           [java.util Date Calendar])
  (:refer-clojure :exclude [read pmap]))

(set! *warn-on-reflection* true)

(def tmp-filename "target/foo.dend")

(use-fixtures :each (fn [f] (f) (io/delete-file tmp-filename true)))

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
         {:readers {:foo "foo"}}
         "reader key should be a symbol but got ':foo'"
         {:readers {'foo "foo"}}
         "reader value for tag 'foo' should be a function but got 'foo'"
         {:invalid-option "foo"}
         ":invalid-option is not a supported read option.")))

(defn- dremel-paper-writer ^dendrite.java.Writer []
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
             (read {:query {:docid '_ :name [{:language [{:country '_}]}]}} r))))))

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
               (doall (read {:query {:docid '_}} r))))))))

(deftest empty-file-write-read
  (.close (writer (Schema/readString helpers/test-schema-str) tmp-filename))
  (with-open [r (reader tmp-filename)]
    (is (empty? (read r)))
    (is (zero? (-> r stats :global :data-length)))
    (is (pos? (-> r stats :global :length)))))

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
                       (doall (read r))))))))

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
                       (doall (read r))))))))

(deftest automatic-schema-optimization
  (let [records (take 100 (helpers/rand-test-records))
        test-schema (Schema/readString helpers/test-schema-str)]
    (with-open [w (writer {:optimize-columns? :all :compression-thresholds {}}
                          test-schema
                          tmp-filename)]
      (.writeAll w records))
    (with-open [r (reader tmp-filename)]
      (testing "schema is indeed optimized"
        (is (= (str "#req "
                    "{:docid #req #col [long delta],"
                    " :links {:backward (long), :forward [long]},"
                    " :name [{:language [{:code #req #col [string dictionary],"
                    " :country #col [string dictionary]}],"
                    " :url #col [string incremental]}],"
                    " :meta {#req #col [string dictionary] #req #col [string dictionary]},"
                    " :keywords #{#col [string dictionary]},"
                    " :is-active #req boolean}")
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
    (is (= test-metadata (with-open [r (reader tmp-filename)]
                           (metadata r))))))

(deftest corrupt-data
  (testing "corrupt file"
    (spit tmp-filename "random junk")
    (is (thrown-with-msg? IllegalStateException #"File is not a valid dendrite file."
                          (with-open [f (reader tmp-filename)]))))
  (testing "interrupted write"
    (spit tmp-filename "den1")
    (is (thrown-with-msg? IllegalStateException #"File is not a valid dendrite file."
                          (with-open [f (reader tmp-filename)])))))


(deftest record-group-lengths
  (let [records (take 10000 (helpers/rand-test-records))]
    (letfn [(avg-record-group-length [target-length]
              (with-open [w (writer {:record-group-length target-length}
                                    (Schema/readString helpers/test-schema-str)
                                    tmp-filename)]
                (.writeAll w records))
              (with-open [r (reader tmp-filename)]
                (->> r stats :record-groups butlast (map :length) helpers/avg)))]
      (testing "record-group lengths are approximately equal to record-group-length"
        (is (helpers/roughly (* 300 1024) (avg-record-group-length (* 300 1024))))
        (is (helpers/roughly (* 100 1024) (avg-record-group-length (* 100 1024))))
        (is (= records (with-open [r (reader tmp-filename)]
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
         (with-open [r (reader tmp-filename)]
           (pmap throw-foo-fn r)))))
  (testing "missing file exception"
    (is (thrown-with-msg?
         java.nio.file.NoSuchFileException #"/no/such/file"
         (with-open [r (reader "/no/such/file")]
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
        (with-open [w (writer {:invalid-input-handler (fn [record e] (reset! error-atom record))}
                              (Schema/readString dremel-paper-schema-str)
                              tmp-filename)]
          (.write w bad-record)
          (.write w dremel-paper-record1)
          (.write w dremel-paper-record2))
        (is (= @error-atom bad-record))
        (is (= [dremel-paper-record1 dremel-paper-record2]
               (with-open [r (reader tmp-filename)]
                 (doall (read r)))))))))

(deftest strict-queries
  (testing "queries fail when missing-fields-as-nil? is false and we query a missing field."
    (.close (dremel-paper-writer))
    (with-open [r (reader tmp-filename)]
      (is (= [nil nil] (read {:query {:foo '_}} r)))
      (is (= [nil nil] (r/reduce conj [] (read {:query {:foo '_}} r))))
      (is (thrown-with-msg?
           IllegalArgumentException #"The following fields don't exist: \[:foo\]"
           (helpers/throw-cause (read {:query {:foo '_} :missing-fields-as-nil? false} r)))))))

(deftest entrypoints
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (writer (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (testing "sub records"
      (is (= (map :links records) (with-open [r (reader tmp-filename)]
                                    (doall (read {:entrypoint [:links]} r))))))
    (testing "access a column directly"
      (is (= (map :docid records) (with-open [r (reader tmp-filename)]
                                    (doall (read {:entrypoint [:docid]} r))))))
    (testing "access a repeated column"
      (is (= (map #(get-in % [:links :backward]) records)
             (with-open [r (reader tmp-filename)]
               (doall (read {:entrypoint [:links :backward]} r))))))))

(deftest readers
  (testing "readers functions transform output"
    (.close (dremel-paper-writer))
    (with-open [r (reader tmp-filename)]
      (is (= [{:name 3, :docid 10} {:name 1, :docid 20}]
             (read {:query {:docid '_ :name (Schema/tag 'foo '_)} :readers {'foo count}} r)))
      (is (thrown-with-msg?
           IllegalArgumentException #"No reader function was provided for tag 'foo'."
           (helpers/throw-cause
            (read {:query {:docid '_ :name (Schema/tag 'foo '_)}} r))))))
  (testing "reader functions behave properly on missing fields"
    (with-open [r (reader tmp-filename)]
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
             (helpers/throw-cause (with-open [w (writer {:docid 'long :at 'test-type} tmp-filename)]
                                    (.writeAll w records))))))
      (testing "throw error when invalid field is defined in custom-types"
        (is (thrown-with-msg?
             IllegalArgumentException #"Key ':invalid' is not a valid custom-type field"
             (helpers/throw-cause
              (.close (writer {:custom-types {'test-type {:invalid 'bar}}}
                              {:docid 'long :at 'test-type}
                              tmp-filename))))))
      (with-open [w (writer {:custom-types custom-types} {:docid 'long :at 'test-type} tmp-filename)]
        (.writeAll w records))
      (testing "read as derived type when :custom-types option is passed"
        (is (= records (with-open [r (reader {:custom-types custom-types} tmp-filename)]
                         (doall (read r))))))
      #_(testing "read as base type and warn when :custom-types option is not passed"
        (binding [*err* (StringWriter.)]
          (let [records-read (-> bb byte-buffer-reader read)]
            (is (= records-with-timestamps records-read))
            (is (every? #(re-find (re-pattern (str % " is not defined for type 'test-type', "
                                                   "defaulting to clojure.core/identity."))
                                  (str *err*))
                        [:coercion-fn :to-base-type-fn :from-base-type-fn]))))))))

(deftest pmap-convenience-function
  (.close (dremel-paper-writer))
  (is (= [3 1] (with-open [r (reader tmp-filename)]
                 (doall (pmap (comp count :name) r))))))

(deftest chunkiness
  (.close (dremel-paper-writer))
  (is (chunked-seq? (with-open [r (reader tmp-filename)]
                      (doall (next (read r)))))))

(deftest folding
  (.close (dremel-paper-writer))
  (with-open [r (reader tmp-filename)]
    (is (= 30 (->> (read r) (r/map :docid) (r/fold +))))
    (is (= 30 (->> (read r) (r/map :docid) (r/reduce +))))
    (is (= 40 (->> (read r) (r/map :docid) (r/reduce + 10))))))
