;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.impl-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [dendrite.impl :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.schema :as schema]
            [dendrite.test-helpers :as helpers])
  (:import [java.io StringWriter]
           [java.util Date Calendar])
  (:refer-clojure :exclude [read pmap]))

(set! *warn-on-reflection* true)

(def tmp-filename "target/foo.dend")

(defn- dremel-paper-writer ^java.io.Closeable []
  (doto (byte-buffer-writer (schema/read-string dremel-paper-schema-str))
    (write! dremel-paper-record1)
    (write! dremel-paper-record2)))

(deftest dremel-paper
  (let [byte-buffer (byte-buffer! (dremel-paper-writer))]
    (testing "full schema"
      (is (= [dremel-paper-record1 dremel-paper-record2] (-> byte-buffer byte-buffer-reader read))))
    (testing "two fields example"
      (is (= [{:docid 10
               :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
              {:docid 20 :name [nil]}]
             (->> byte-buffer
                  byte-buffer-reader
                  (read {:query {:docid '_ :name [{:language [{:country '_}]}]}})))))))

(deftest byte-buffer-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))
        writer (->> (byte-buffer-writer (-> helpers/test-schema-str schema/read-string))
                    (#(reduce write! % records)))
        byte-buffer (byte-buffer! writer)]
    (testing "full schema"
      (is (= records (read (byte-buffer-reader byte-buffer)))))
    (testing "schema"
      (is (not (nil? (schema (byte-buffer-reader byte-buffer))))))
    (testing "stats"
      (is (not (nil? (stats (byte-buffer-reader byte-buffer))))))))

(deftest file-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (file-writer (-> helpers/test-schema-str schema/read-string) tmp-filename)]
      (reduce write! w records))
    (testing "full schema"
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r))))))
    (testing "one field"
      (is (= (map #(select-keys % [:docid]) records)
             (with-open [r (file-reader tmp-filename)]
               (doall (read {:query {:docid '_}} r))))))
    (io/delete-file tmp-filename)))

(deftest automatic-schema-optimization
  (let [records (take 100 (helpers/rand-test-records))
        test-schema (-> helpers/test-schema-str schema/read-string)]
    (with-open [w (file-writer {:optimize-columns? true
                                :compression-thresholds {}}
                               test-schema
                               tmp-filename)]
      (reduce write! w records))
    (testing "schema is indeed optimized"
      (is (= (str "{:docid #req #col [long delta],"
                  " :links {:backward (long), :forward [long]},"
                  " :name [{:language [{:code #req #col [string dictionary],"
                  " :country #col [string dictionary]}],"
                  " :url #col [string incremental]}],"
                  " :meta {#req #col [string dictionary] #req #col [string dictionary]},"
                  " :keywords #{#col [string dictionary]},"
                  " :is-active #req boolean}")
             (str (schema (file-reader tmp-filename))))))
    (testing "stats"
      (is (not (nil? (stats (file-reader tmp-filename))))))
    (testing "full schema"
      (is (= records (read (file-reader tmp-filename)))))
    (testing "one field"
      (is (= (map #(select-keys % [:is-active]) records)
             (read {:query {:is-active '_}} (file-reader tmp-filename)))))
    (io/delete-file tmp-filename)))

(deftest custom-metadata
  (let [test-custom-metadata {:foo {:bar "test"} :baz [1 2 3]}
        writer (doto (dremel-paper-writer)
                 (set-metadata! test-custom-metadata)
                 (swap-metadata! assoc :more "foo"))
        byte-buffer (byte-buffer! writer)]
    (is (= (assoc test-custom-metadata :more "foo") (-> byte-buffer byte-buffer-reader metadata)))))

(deftest corrupt-data
  (let [byte-buffer (byte-buffer! (dremel-paper-writer))]
    (testing "corrupt magic bytes at start"
      (let [bad-byte-pos 2
            tmp-byte (.get byte-buffer bad-byte-pos)]
        (is (thrown-with-msg? IllegalArgumentException #"does not contain a valid dendrite serialization"
                     (byte-buffer-reader (doto byte-buffer (.put bad-byte-pos (byte 0))))))
        (.put byte-buffer bad-byte-pos tmp-byte)))
    (testing "corrupt magic bytes at end"
      (let [bad-byte-pos (- (.limit byte-buffer) 2)
            tmp-byte (.get byte-buffer bad-byte-pos)]
        (is (thrown-with-msg? IllegalArgumentException #"does not contain a valid dendrite serialization"
                     (byte-buffer-reader (doto byte-buffer (.put bad-byte-pos (byte 0))))))
        (.put byte-buffer bad-byte-pos tmp-byte))))
  (testing "corrupt file"
    (spit tmp-filename "random junk")
    (is (thrown-with-msg? IllegalArgumentException #"File is not a valid dendrite file."
                          (with-open [f (file-reader tmp-filename)]
                            (metadata f))))
    (io/delete-file tmp-filename)))

(deftest record-group-lengths
  (letfn [(avg-record-group-length [target-length]
            (let [records (take 1000 (helpers/rand-test-records))
                  writer (->> (byte-buffer-writer {:record-group-length target-length}
                                                   (-> helpers/test-schema-str schema/read-string))
                              (#(reduce write! % records)))
                  byte-buffer (byte-buffer! writer)]
              (->> (byte-buffer-reader byte-buffer)
                   stats
                   :record-groups
                   rest
                   butlast
                   (map :length)
                   helpers/avg)))]
    (testing "record-group lengths are approximately equal to record-group-length"
      (is (helpers/roughly (* 3 1024) (avg-record-group-length (* 3 1024))))
      (is (helpers/roughly 1024 (avg-record-group-length 1024))))))

(defn- throw-foo-fn [& args] (throw (Exception. "foo")))

(deftest errors
  (testing "exceptions in the writing thread are caught in the main thread"
    (testing "byte-buffer-writer"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.impl/complete-record-group! throw-foo-fn]
             (.close (dremel-paper-writer))))))
    (testing "file-writer"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/write-byte-buffer (constantly (Exception. "foo"))]
             (with-open [w (file-writer (-> dremel-paper-schema-str schema/read-string) tmp-filename)]
               (write! w dremel-paper-record1)))))
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/flush-column-chunks-to-byte-buffer throw-foo-fn]
             (with-open [w (file-writer (-> dremel-paper-schema-str schema/read-string) tmp-filename)]
               (write! w dremel-paper-record1))))))
    (io/delete-file tmp-filename))
  (testing "exceptions in the reading thread are caught in the main thread"
    (testing "byte-buffer-reader"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/byte-buffer-reader throw-foo-fn]
             (-> (dremel-paper-writer) byte-buffer! byte-buffer-reader read)))))
    (testing "file-reader"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/byte-buffer-reader throw-foo-fn]
             (with-open [w (file-writer (-> dremel-paper-schema-str schema/read-string) tmp-filename)]
               (write! w dremel-paper-record1))
             (with-open [r (file-reader tmp-filename)]
               (read r)))))
      (io/delete-file tmp-filename)
      (is (thrown-with-msg?
           java.nio.file.NoSuchFileException #"target/foo.dend"
           (with-open [r (file-reader tmp-filename)]
             (read r)))))))

(deftest invalid-records
  (let [bad-record {:docid "not-a-number"}]
    (testing "invalid records trigger an exception while writing"
      (is (thrown-with-msg?
           IllegalArgumentException #"Failed to stripe record '\{:docid \"not-a-number\"\}"
           (helpers/throw-cause (helpers/throw-cause (with-open [w (dremel-paper-writer)]
                                                       (write! w bad-record)))))))
    (testing "invalid-input-handler can process exceptions"
      (let [error-atom (atom nil)
            w (doto (byte-buffer-writer {:invalid-input-handler (fn [record e] (reset! error-atom record))}
                                        (schema/read-string dremel-paper-schema-str))
                (write! bad-record)
                (write! dremel-paper-record1)
                (write! dremel-paper-record2)
                .close)]
        (is (= @error-atom bad-record))
        (is (= [dremel-paper-record1 dremel-paper-record2]
               (read (-> w byte-buffer! byte-buffer-reader))))))))

(deftest strict-queries
  (testing "queries fail when missing-fields-as-nil? is false and we query a missing fields"
    (let [bb (byte-buffer! (dremel-paper-writer))]
      (is (= [nil nil] (read {:query {:foo '_}} (byte-buffer-reader bb))))
      (is (thrown-with-msg?
           IllegalArgumentException #"The following fields don't exist: \[:foo\]"
           (helpers/throw-cause (read {:query {:foo '_}
                                       :missing-fields-as-nil? false}
                                      (byte-buffer-reader bb))))))))

(deftest readers
  (testing "readers functions transform output"
    (let [bb (byte-buffer! (dremel-paper-writer))]
      (is (= [{:name 3, :docid 10} {:name 1, :docid 20}]
             (read {:query {:docid '_ :name (schema/tag 'foo '_)}
                    :readers {'foo count}}
                   (byte-buffer-reader bb))))
      (is (thrown-with-msg?
           IllegalArgumentException #"No reader function was provided for tag 'foo'."
           (helpers/throw-cause
            (read {:query {:docid '_ :name (schema/tag 'foo '_)}} (byte-buffer-reader bb)))))
      (is (thrown-with-msg?
           IllegalArgumentException #":reader key should be a symbol but got ':foo'."
           (read {:query {:docid '_ :name (schema/tag 'foo '_)} :readers {:foo "foo"}}
                 (byte-buffer-reader bb))))
      (is (thrown-with-msg?
           IllegalArgumentException #":reader value for tag 'foo' should be a function."
           (read {:query {:docid '_ :name (schema/tag 'foo '_)} :readers {'foo "foo"}}
                 (byte-buffer-reader bb)))))))

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
             IllegalArgumentException #"Unsupported type 'test-type' for column \[:at\]"
             (helpers/throw-cause (with-open [w (byte-buffer-writer {:docid 'long :at 'test-type})]
                                    (reduce write! w records))))))
      (testing "throw error when invalid field is defined in custom-types"
        (is (thrown-with-msg?
             IllegalArgumentException #"Key :invalid is not a valid derived-type key. "
             (byte-buffer-writer {:custom-types {'test-type {:invalid 'bar}}} {:docid 'long :at 'test-type}))))
      (let [w (with-open [w (byte-buffer-writer {:custom-types custom-types} {:docid 'long :at 'test-type})]
                (reduce write! w records))
            bb (byte-buffer! w)]
        (testing "read as derived type when :custom-types option is passed"
          (is (= records (->> bb (byte-buffer-reader {:custom-types custom-types}) read))))
        (testing "read as base type and warn when :custom-types option is not passed"
          (binding [*err* (StringWriter.)]
            (let [records-read (-> bb byte-buffer-reader read)]
              (is (= records-with-timestamps records-read))
              (is (every? #(re-find (re-pattern (str % " is not defined for type 'test-type', "
                                                     "defaulting to clojure.core/identity."))
                                    (str *err*))
                          [:coercion-fn :to-base-type-fn :from-base-type-fn])))))))))

(deftest pmap-convenience-function
  (let [rdr (-> (dremel-paper-writer) byte-buffer! byte-buffer-reader)]
    (is (= [3 1] (pmap (comp count :name) rdr)))))

(deftest invalid-options-are-caught
  (testing "writer options"
    (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                      (#'dendrite.impl/parse-writer-options opts))
         {:record-group-length "foo"}
         ":record-group-length expects a positive integer but got 'foo'."
         {:record-group-length nil}
         ":record-group-length expects a positive integer but got 'null'."
         {:record-group-length -1.5}
         ":record-group-length expects a positive integer but got '-1.5'."
         {:data-page-length "foo"}
         ":data-page-length expects a positive integer but got 'foo'."
         {:data-page-length nil}
         ":data-page-length expects a positive integer but got 'null'."
         {:data-page-length -1.5}
         ":data-page-length expects a positive integer but got '-1.5'."
         {:optimize-columns? "foo"}
         ":optimize-columns\\? expects either true, false, or nil but got 'foo'"
         {:compression-thresholds :lz4}
         ":compression-thresholds expects a map."
         {:compression-thresholds {:lz4 -0.2}}
         ":compression-thresholds expects compression-type/compression-threshold map entries"
         {:invalid-input-handler "foo"}
         ":invalid-input-handler expects a function."
         {:invalid-option "foo"}
         ":invalid-option is not a supported writer option."))
  (testing "reader options"
    (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                      (#'dendrite.impl/parse-reader-options opts))
         {:invalid-option "foo"}
         ":invalid-option is not a supported reader option."))
  (testing "read options"
    (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                      (#'dendrite.impl/parse-read-options opts))
         {:missing-fields-as-nil? nil}
         ":missing-fields-as-nil\\? expects a boolean but got 'null'"
         {:missing-fields-as-nil? "foo"}
         ":missing-fields-as-nil\\? expects a boolean but got 'foo'"
         {:pmap-fn "foo"}
         ":pmap-fn expects a function."
         {:invalid-option "foo"}
         ":invalid-option is not a supported read option.")))
