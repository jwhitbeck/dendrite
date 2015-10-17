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
            [dendrite.core :as d]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Schema]
           [java.util Date Calendar]))

(set! *warn-on-reflection* true)

(def tmp-filename "target/foo.den")
(def tmp-filename2 "target/bar.den")

(use-fixtures :each (fn [f]
                      (f)
                      (io/delete-file tmp-filename true)
                      (io/delete-file tmp-filename2 true)))

(defn- dremel-paper-writer
  (^dendrite.java.FileWriter [] (dremel-paper-writer tmp-filename))
  (^dendrite.java.FileWriter [filename]
    (doto (d/file-writer (Schema/readString dremel-paper-schema-str) filename)
      (.write dremel-paper-record1)
      (.write dremel-paper-record2))))

(deftest dremel-paper
  (.close (dremel-paper-writer))
  (with-open [r (d/file-reader tmp-filename)]
    (testing "full schema"
      (is (= [dremel-paper-record1 dremel-paper-record2] (d/read r))))
    (testing "two fields example"
      (is (= [{:docid 10
               :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
              {:docid 20 :name [nil]}]
             (d/read {:query {:docid '_ :name [{:language [{:country '_}]}]}} r))))))

(deftest file-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (d/file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (testing "full schema"
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r))))))
    (testing "one field"
      (is (= (map #(select-keys % [:docid]) records)
             (with-open [r (d/file-reader tmp-filename)]
               (doall (d/read {:query {:docid '_}} r))))))))

(deftest empty-file-write-read
  (.close (d/file-writer (Schema/readString helpers/test-schema-str) tmp-filename))
  (with-open [r (d/file-reader tmp-filename)]
    (is (empty? (d/read r)))
    (is (zero? (-> r d/stats :global :data-length)))
    (is (pos? (-> r d/stats :global :length)))))

(deftest flat-base-type-write-read
  (testing "required"
    (let [records (repeatedly 100 #(rand-int 100))]
      (with-open [w (d/file-writer (d/req 'int) tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r)))))))
  (testing "optional"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-map 0.1 (constantly nil)) (take 100))]
      (with-open [w (d/file-writer 'int tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r))))))))

(deftest flat-repeated-type-write-read
  (testing "repeated optional base type"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-map 0.1 (constantly nil))
                       (partition 5) (take 20))]
      (with-open [w (d/file-writer ['int] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r)))))))
  (testing "repeated required base type"
    (let [records (->> (repeatedly #(rand-int 100)) (partition 5) (take 20))]
      (with-open [w (d/file-writer [(d/req 'int)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r)))))))
  (testing "repeated records"
    (let [records (->> (helpers/rand-test-records) (partition 5) (take 20))]
      (with-open [w (d/file-writer [(Schema/readString helpers/test-schema-str)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r))))))))

(deftest empty-and-missing-collection
  (testing "required collection, required value"
    (let [records (->> (repeatedly #(rand-int 100)) (helpers/rand-partition 3) (take 100))]
      (with-open [w (d/file-writer (d/req [(d/req 'int)]) tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r)))))))
  (testing "required collection, optional value"
    (let [records (->> (repeatedly #(when (helpers/rand-bool) (rand-int 100)))
                       (helpers/rand-partition 3) (take 100))]
      (with-open [w (d/file-writer (d/req ['int]) tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r)))))))
  (testing "optional collection, required value"
    (let [records (->> (repeatedly #(rand-int 100))
                       (helpers/rand-partition 3)
                       (helpers/rand-map 0.5 seq)
                       (take 100))]
      (with-open [w (d/file-writer [(d/req 'int)] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r)))))))
  (testing "optional collection, optional value"
    (let [records (->> (repeatedly #(when (helpers/rand-bool) (rand-int 100)))
                       (helpers/rand-partition 3)
                       (helpers/rand-map 0.5 seq)
                       (take 100))]
      (with-open [w (d/file-writer ['int] tmp-filename)]
        (.writeAll w records))
      (is (= records (with-open [r (d/file-reader tmp-filename)]
                       (doall (d/read r))))))))

(deftest automatic-schema-optimization
  (let [records (take 100 (helpers/rand-test-records))
        test-schema (Schema/readString helpers/test-schema-str)]
    (with-open [w (d/file-writer {:optimize-columns? :all :compression-thresholds {}}
                                 test-schema
                                 tmp-filename)]
      (.writeAll w records))
    (with-open [r (d/file-reader tmp-filename)]
      (testing "schema is indeed optimized"
        (is (= (str "#req "
                    "{:docid #req #col [long delta],"
                    " :links {:backward (long), :forward #req [long]},"
                    " :name [{:language [{:code #req #col [string dictionary],"
                    " :country #col [string dictionary]}],"
                    " :url #col [string incremental]}],"
                    " :meta {#req #col [string dictionary] #req #col [string dictionary]},"
                    " :keywords #{#req #col [string dictionary]},"
                    " :internal/is-active #req boolean,"
                    " :ngrams [[#req #col [string dictionary]]]}")
               (str (d/full-schema r)))))
      (testing "stats"
        (is (not (nil? (d/stats r)))))
      (testing "full schema"
        (is (= records (d/read r))))
      (testing "one field"
        (is (= (map #(select-keys % [:internal/is-active]) records)
               (d/read {:query {:internal/is-active '_}} r)))))))

(defrecord Foo [foo])

(deftest user-defined-metadata
  (testing "untagged"
    (let [test-metadata {:foo {:bar "test"} :baz [1 2 3]}]
      (with-open [w (dremel-paper-writer)]
        (d/set-metadata! w test-metadata))
      (is (= test-metadata (with-open [r (d/file-reader tmp-filename)]
                             (d/metadata r))))))
  (testing "tagged, read without tags"
    (let [test-metadata (map->Foo {:foo {:bar "test"}})]
      (with-open [w (dremel-paper-writer)]
        (d/set-metadata! w test-metadata))
      (is (= {:foo {:bar "test"}} (with-open [r (d/file-reader tmp-filename)]
                                    (d/metadata r))))))
  (testing "tagged, read with tags"
    (let [test-metadata (map->Foo {:foo {:bar "test"}})]
      (with-open [w (dremel-paper-writer)]
        (d/set-metadata! w test-metadata))
      (is (= test-metadata (with-open [r (d/file-reader tmp-filename)]
                             (d/metadata r {:readers {'dendrite.core_test.Foo map->Foo}})))))))

(deftest corrupt-data
  (testing "corrupt file"
    (spit tmp-filename "random junk")
    (is (thrown-with-msg? IllegalStateException #"File is not a valid dendrite file."
                          (with-open [f (d/file-reader tmp-filename)]))))
  (testing "interrupted write"
    (spit tmp-filename "den1")
    (is (thrown-with-msg? IllegalStateException #"File is not a valid dendrite file."
                          (with-open [f (d/file-reader tmp-filename)])))))

(deftest record-group-lengths
  (let [records (take 15000 (helpers/rand-test-records))]
    (letfn [(avg-record-group-length [target-length]
              (with-open [w (d/file-writer {:record-group-length target-length}
                                           (Schema/readString helpers/test-schema-str)
                                           tmp-filename)]
                (.writeAll w records))
              (with-open [r (d/file-reader tmp-filename)]
                (->> r d/stats :record-groups butlast (map :length) helpers/avg)))]
      (testing "record-group lengths are approximately equal to record-group-length"
        (is (helpers/roughly (* 300 1024) (avg-record-group-length (* 300 1024))))
        (is (helpers/roughly (* 100 1024) (avg-record-group-length (* 100 1024))))
        (is (= records (with-open [r (d/file-reader tmp-filename)]
                         (doall (d/read r)))))))))

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
         (with-open [r (d/file-reader tmp-filename)]
           (doall (d/eduction (map throw-foo-fn) (d/read r)))))))
  (testing "missing file exception"
    (is (thrown-with-msg?
         java.nio.file.NoSuchFileException #"/no/such/file"
         (with-open [r (d/file-reader "/no/such/file")]
           (d/read r))))))

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
        (with-open [w (d/file-writer {:invalid-input-handler (fn [record e] (reset! error-atom record))}
                                     (Schema/readString dremel-paper-schema-str)
                                     tmp-filename)]
          (.write w bad-record)
          (.write w dremel-paper-record1)
          (.write w dremel-paper-record2))
        (is (= @error-atom bad-record))
        (is (= [dremel-paper-record1 dremel-paper-record2]
               (with-open [r (d/file-reader tmp-filename)]
                 (doall (d/read r)))))))))

(deftest schemas-as-queries
  (testing "can query using schema"
    (.close (dremel-paper-writer))
    (is (= [dremel-paper-record1 dremel-paper-record2]
           (with-open [r (d/file-reader tmp-filename)]
             (doall (d/read {:query (d/schema r)} r)))))
    (is (= (map #(select-keys % [:docid :links]) [dremel-paper-record1 dremel-paper-record2])
           (with-open [r (d/file-reader tmp-filename)]
             (doall (d/read {:query (select-keys (d/schema r) [:docid :links])} r))))))
  (testing "can query using full-schema"
    (.close (dremel-paper-writer))
    (is (= [dremel-paper-record1 dremel-paper-record2]
           (with-open [r (d/file-reader tmp-filename)]
             (doall (d/read {:query (d/full-schema r)} r)))))
    (is (= (map #(select-keys % [:docid :links]) [dremel-paper-record1 dremel-paper-record2])
           (with-open [r (d/file-reader tmp-filename)]
             (doall (d/read {:query (select-keys (d/full-schema r) [:docid :links])} r)))))))

(deftest strict-queries
  (testing "queries fail when missing-fields-as-nil? is false and we query a missing field."
    (.close (dremel-paper-writer))
    (with-open [r (d/file-reader tmp-filename)]
      (is (= [nil nil] (d/read {:query {:foo '_}} r)))
      (is (= [nil nil] (r/reduce conj [] (d/read {:query {:foo '_}} r))))
      (is (thrown-with-msg?
           IllegalArgumentException #"The following fields don't exist: \[:foo\]"
           (helpers/throw-cause (doall (d/read {:query {:foo '_} :missing-fields-as-nil? false} r))))))))

(deftest sub-schemas
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (d/file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (testing "sub records"
      (is (= (map :links records) (with-open [r (d/file-reader tmp-filename)]
                                    (doall (d/read {:sub-schema-in [:links]} r))))))
    (testing "access a column directly"
      (is (= (map :docid records) (with-open [r (d/file-reader tmp-filename)]
                                    (doall (d/read {:sub-schema-in [:docid]} r))))))
    (testing "access a repeated column"
      (is (= (map #(get-in % [:links :backward]) records)
             (with-open [r (d/file-reader tmp-filename)]
               (doall (d/read {:sub-schema-in [:links :backward]} r))))))))

(deftest readers
  (testing "readers functions transform output"
    (.close (dremel-paper-writer))
    (with-open [r (d/file-reader tmp-filename)]
      (is (= [{:name 3, :docid 10} {:name 1, :docid 20}]
             (d/read {:query {:docid '_ :name (d/tag 'foo '_)} :readers {'foo count}} r)))
      (is (= [{:docid 11} {:docid 21}]
             (d/read {:query {:docid (d/tag 'foo '_)} :readers {'foo inc}} r)))
      (is (thrown-with-msg?
           IllegalArgumentException #"No reader function was provided for tag 'foo'."
           (helpers/throw-cause
            (doall (d/read {:query {:docid '_ :name (d/tag 'foo '_)}} r)))))))
  (testing "readers functions behave properly on repeated columns"
    (with-open [w (d/file-writer [{:foo 'int}] tmp-filename)]
      (.write w [nil {:foo 1}])
      (.write w [nil])
      (.write w nil)
      (.write w [{:foo 1}]))
    (with-open [r (d/file-reader tmp-filename)]
      (is (= [[nil {:foo 2}] [nil] nil [{:foo 2}]]
             (d/read {:query [{:foo (d/tag 'foo '_)}] :readers {'foo inc}} r))))
    (with-open [w (d/file-writer ['int] tmp-filename)]
      (.write w [1 nil 2])
      (.write w nil)
      (.write w [1 2])
      (.write w [nil]))
    (with-open [r (d/file-reader tmp-filename)]
      (is (= [[2 1 3] nil [2 3] [1]]
             (d/read {:query [(d/tag 'foo '_)] :readers {'foo (fnil inc 0)}} r)))))
  (testing "reader functions behave properly on missing fields"
    (.close (dremel-paper-writer))
    (with-open [r (d/file-reader tmp-filename)]
      (is (= [true true]
             (d/read {:query (d/tag 'foo {:foo '_}) :readers {'foo empty?}} r)))
      (is (= [{:docid 10 :foo {:bar 0}} {:docid 20 :foo {:bar 0}}]
             (d/read {:query {:docid '_ :foo {:bar (d/tag 'bar [{:baz '_}])}} :readers {'bar count}}
                     r)))))
  (testing "reader functions handle repeated dictionary columns properly"
    (with-open [w (d/file-writer [(d/col 'int 'dictionary)] tmp-filename)]
      (.write w [1 2 3])
      (.write w []))
    (with-open [r (d/file-reader tmp-filename)]
      (is (= [[2 3 4] []]
             (d/read {:query [(d/tag 'foo '_)] :readers {'foo inc}} r))))))

(deftest custom-types
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
           (helpers/throw-cause (with-open [w (d/file-writer {:docid 'long :at 'test-type} tmp-filename)]
                                  (.writeAll w records))))))
    (testing "throw error when invalid field is defined in custom-types"
      (is (thrown-with-msg?
           IllegalArgumentException #":invalid is not a valid custom type definition key"
           (helpers/throw-cause
            (.close (d/file-writer {:custom-types {'test-type {:invalid 'bar}}}
                                   {:docid 'long :at 'test-type}
                                   tmp-filename))))))
    (with-open [w (d/file-writer {:custom-types custom-types} {:docid 'long :at 'test-type} tmp-filename)]
      (.writeAll w records))
    (testing "read as derived type when :custom-types option is passed"
      (is (= records (with-open [r (d/file-reader {:custom-types custom-types} tmp-filename)]
                       (doall (d/read r))))))
    (testing "read as base type when :custom-types option is not passed"
      (is (= records-with-timestamps (with-open [r (d/file-reader tmp-filename)]
                                       (doall (d/read r))))))
    (testing "retrieve custom-type mappings"
      (is (= {'test-type 'long} (with-open [r (d/file-reader tmp-filename)]
                                  (d/custom-types r)))))))

(deftest view-sampling
  (testing "dremel paper records"
    (.close (dremel-paper-writer))
    (is (= [dremel-paper-record1] (with-open [r (d/file-reader tmp-filename)]
                                    (doall (d/sample even? (d/read r))))))
    (is (= [dremel-paper-record2] (with-open [r (d/file-reader tmp-filename)]
                                    (doall (d/sample odd? (d/read r)))))))
  (testing "random records"
    (let [records (take 1000 (helpers/rand-test-records))]
      (with-open [w (d/file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
        (.writeAll w records))
      (is (= (->> records (partition 2) (map second))
             (with-open [r (d/file-reader tmp-filename)]
               (doall (d/sample odd? (d/read r))))))
      (is (= (->> records (partition 2) (map second) (reduce #(+ %1 (:docid %2)) 0))
             (with-open [r (d/file-reader tmp-filename)]
               (reduce #(+ %1 (:docid %2)) 0 (d/sample odd? (d/read r))))))))
  (testing "invalid sampling"
    (.close (dremel-paper-writer))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot define multiple sample functions"
                          (with-open [r (d/file-reader tmp-filename)]
                            (->> (d/read r)
                                 (d/sample odd?)
                                 (d/sample odd?)))))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Sample function must be defined before any indexing or transducer function"
                          (with-open [r (d/file-reader tmp-filename)]
                            (->> (d/read r)
                                 (d/eduction (map :docid))
                                 (d/sample odd?)))))))

(deftest eductions
  (testing "dremel paper records"
    (.close (dremel-paper-writer))
    (is (= [3 1] (with-open [r (d/file-reader tmp-filename)]
                   (->> (d/read r)
                        (d/eduction (map :name) (map count))
                        doall))))
    (is (= [4 2] (with-open [r (d/file-reader tmp-filename)]
                   (->> (d/read r)
                        (d/eduction (map :name) (map count) (map inc))
                        doall))))
    (is (empty? (with-open [r (d/file-reader tmp-filename)]
                  (->> (d/read r)
                       (d/eduction (filter (constantly false)))
                       (into []))))))
  (testing "random records"
    (let [records (take 100 (helpers/rand-test-records))]
      (with-open [w (d/file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
        (.writeAll w records))
      (is (= (map :docid records)
             (with-open [r (d/file-reader tmp-filename)]
               (->> (d/read r) (d/eduction (map :docid)) doall))))
      (is (= (reduce + (map :docid records))
             (with-open [r (d/file-reader tmp-filename)]
               (->> (d/read r)
                    (d/eduction (map :docid))
                    (reduce +)))))
      (is (= (->> records
                  (filter :ngrams)
                  (keep :keywords)
                  (filter #(> (count %) 2))
                  (map count))
             (with-open [r (d/file-reader tmp-filename)]
               (->> (d/read r)
                    (d/eduction (filter :ngrams)
                                (keep :keywords)
                                (filter #(> (count %) 2))
                                (map count))
                    doall))))
      (is (= (->> records
                  (filter :ngrams)
                  (keep :keywords)
                  (filter #(> (count %) 2))
                  (map count)
                  (reduce +))
             (with-open [r (d/file-reader tmp-filename)]
               (->> (d/read r)
                    (d/eduction (filter :ngrams)
                                (keep :keywords)
                                (filter #(> (count %) 2))
                                (map count))
                    (reduce +))))))))

(deftest view-indexing
  (testing "dremel paper records"
    (.close (dremel-paper-writer))
    (is (= [3 2] (with-open [r (d/file-reader tmp-filename)]
                   (->> (d/read r)
                        (d/index-by (fn [i rec] (+ i (count (:name rec)))))
                        doall))))
    (is (= [4 3] (with-open [r (d/file-reader tmp-filename)]
                   (->> (d/read r)
                        (d/index-by (fn [i rec] (+ i (count (:name rec)))))
                        (d/eduction (map inc))
                        doall)))))
  (testing "random records"
    (let [records (take 100 (helpers/rand-test-records))
          f (fn [i rec] (+ i (:docid rec)))]
      (with-open [w (d/file-writer (Schema/readString helpers/test-schema-str) tmp-filename)]
        (.writeAll w records))
      (is (= (map-indexed f records)
             (with-open [r (d/file-reader tmp-filename)]
               (->> (d/read r) (d/index-by f) doall))))
      (is (= (reduce + (map-indexed f records))
             (with-open [r (d/file-reader tmp-filename)]
               (->> (d/read r) (d/index-by f) (reduce +))))))))

(deftest chunkiness
  (.close (dremel-paper-writer))
  (is (chunked-seq? (with-open [r (d/file-reader tmp-filename)]
                      (doall (next (d/read r)))))))

(deftest folding
  (.close (dremel-paper-writer))
  (with-open [r (d/file-reader tmp-filename)]
    (is (= 30 (->> (d/read r) (r/map :docid) (r/fold +))))
    (let [s (doto (d/read r) seq)]
      (is (= 30 (->> s (r/map :docid) (r/fold +)))))
    (is (= 31 (->> (d/read r) (r/map :docid) (r/fold * +))))
    (letfn [(combinef
              ([] (atom []))
              ([l r] (into @l @r)))
            (reducef [r v] (doto r (swap! conj v)))]
      (is (= [10 20] (->> (d/read r) (r/map :docid) (r/fold combinef reducef)))))
    (is (= 30 (->> (d/read r) (r/map :docid) (r/reduce +))))
    (is (= 40 (->> (d/read r) (r/map :docid) (r/reduce + 10))))))

(deftest reducing
  (.close (dremel-paper-writer))
  (with-open [r (d/file-reader tmp-filename)]
    (let [s (doto (d/read r) seq)]
      (is (= 30 (->> s (r/map :docid) (reduce +))))
      (is (= 40 (->> s (r/map :docid) (reduce + 10)))))
    (is (= 30 (->> (d/read r) (r/map :docid) (reduce +))))
    (is (= 40 (->> (d/read r) (r/map :docid) (reduce + 10))))))

(deftest multiple-files
  (.close (doto (dremel-paper-writer tmp-filename)
            (d/set-metadata! :foo)))
  (.close (doto (dremel-paper-writer tmp-filename2)
            (d/set-metadata! :bar)))
  (testing "reads"
    (is (= (concat (with-open [r (d/file-reader tmp-filename)]
                     (doall (d/read r)))
                   (with-open [r (d/file-reader tmp-filename2)]
                     (doall (d/read r))))
           (with-open [r (d/files-reader [tmp-filename tmp-filename2])]
             (doall (d/read r)))))))

(deftest writer-with-xform
  (let [records (take 100 (helpers/rand-test-records))
        xform (comp (map #(select-keys % [:docid :internal/is-active]))
                    (filter #(-> % :docid even?)))]
    (with-open [w (d/file-writer {} xform (Schema/readString helpers/test-schema-str) tmp-filename)]
      (.writeAll w records))
    (is (= (eduction xform records)
           (with-open [r (d/file-reader tmp-filename)]
             (doall (d/read r)))))))
