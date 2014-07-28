(ns dendrite.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [dendrite.core :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.schema :as schema]
            [dendrite.test-helpers :as helpers])
  (:refer-clojure :exclude [read]))

(def tmp-filename "target/foo.dend")

(defn- dremel-paper-writer []
  (doto (byte-buffer-writer (schema/read-string dremel-paper-schema-str))
    (write! [dremel-paper-record1 dremel-paper-record2])))

(deftest dremel-paper
  (let [byte-buffer (byte-buffer! (dremel-paper-writer))]
    (testing "full schema"
      (is (= [dremel-paper-record1 dremel-paper-record2] (-> byte-buffer byte-buffer-reader read))))
    (testing "two fields example"
      (is (= [{:docid 10
               :name [{:language [{:country "us"} nil]} nil {:language [{:country "gb"}]}]}
              {:docid 20}]
             (-> byte-buffer
                 (byte-buffer-reader :query {:docid '_ :name [{:language [{:country '_}]}]})
                 read))))))

(deftest byte-buffer-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))
        writer (doto (byte-buffer-writer (-> helpers/test-schema-str schema/read-string))
                 (write! records))
        byte-buffer (byte-buffer! writer)]
    (testing "full schema"
      (is (= records (read (byte-buffer-reader byte-buffer)))))))

(deftest file-random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))]
    (with-open [w (file-writer tmp-filename (-> helpers/test-schema-str schema/read-string))]
      (write! w records))
    (testing "full schema"
      (is (= records (with-open [r (file-reader tmp-filename)]
                       (doall (read r))))))
    (testing "one-field"
      (is (= (map #(select-keys % [:docid]) records)
             (with-open [r (file-reader tmp-filename :query {:docid '_})]
               (doall (read r))))))
    (io/delete-file tmp-filename)))

(deftest automatic-schema-optimization
  (let [records (take 100 (helpers/rand-test-records))
        test-schema (-> helpers/test-schema-str schema/read-string)
        writer (doto (byte-buffer-writer test-schema
                                         :optimize-columns? :all
                                         :compression-thresholds {})
                 (write! records))
        reader (-> writer byte-buffer! byte-buffer-reader)]
    (is (= (str "{:docid #req #col [long delta],"
                " :links {:backward (long), :forward [long]},"
                " :name [{:language [{:code #req #col [string dictionary],"
                                    " :country #col [string dictionary]}],"
                        " :url #col [string incremental]}],"
                " :meta {#col [string dictionary] #col [string dictionary]},"
                " :keywords #{#col [string dictionary]}}")
           (str (schema reader))))))

(deftest custom-metadata
  (let [test-custom-metadata {:foo {:bar "test"} :baz [1 2 3]}
        writer (doto (dremel-paper-writer)
                 (set-metadata! test-custom-metadata))
        byte-buffer (byte-buffer! writer)]
    (is (= test-custom-metadata (-> byte-buffer byte-buffer-reader metadata)))))

(deftest corrupt-data
  (let [byte-buffer (byte-buffer! (dremel-paper-writer))]
    (testing "corrupt magic bytes at start"
      (let [bad-byte-pos 2
            tmp-byte (.get byte-buffer bad-byte-pos)]
        (is (thrown-with-msg? IllegalArgumentException #"does not contain a valid dendrite serialization"
                     (byte-buffer-reader (doto byte-buffer (.put bad-byte-pos (byte 0))) :query '_)))
        (.put byte-buffer bad-byte-pos tmp-byte)))
    (testing "corrupt magic bytes at end"
      (let [bad-byte-pos (- (.limit byte-buffer) 2)
            tmp-byte (.get byte-buffer bad-byte-pos)]
        (is (thrown-with-msg? IllegalArgumentException #"does not contain a valid dendrite serialization"
                     (byte-buffer-reader (doto byte-buffer (.put bad-byte-pos (byte 0))) :query '_)))
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
                  writer (doto (byte-buffer-writer (-> helpers/test-schema-str schema/read-string)
                                                   :target-record-group-length target-length)
                           (write! records))
                  byte-buffer (byte-buffer! writer)]
              (->> (byte-buffer-reader byte-buffer)
                   stats
                   :record-groups
                   rest
                   butlast
                   (map :length)
                   helpers/avg)))]
    (testing "record-group lengths are approximately equal to target-record-group-length"
      (is (helpers/roughly (* 3 1024) (avg-record-group-length (* 3 1024))))
      (is (helpers/roughly 1024 (avg-record-group-length 1024))))))

(defn- throw-foo-fn [& args] (throw (Exception. "foo")))

(deftest errors
  (testing "exceptions in the writing thread are caught in the main thread"
    (testing "byte-buffer-writer"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.core/complete-record-group! throw-foo-fn]
             (.close (dremel-paper-writer))))))
    (testing "file-writer"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/write-byte-buffer (constantly (Exception. "foo"))]
             (with-open [w (file-writer tmp-filename (-> dremel-paper-schema-str schema/read-string))]
               (write! w [dremel-paper-record1])))))
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/flush-column-chunks-to-byte-buffer throw-foo-fn]
             (with-open [w (file-writer tmp-filename (-> dremel-paper-schema-str schema/read-string))]
               (write! w [dremel-paper-record1]))))))
    (io/delete-file tmp-filename))
  (testing "exceptions in the reading thread are caught in the main thread"
    (testing "byte-buffer-reader"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/byte-array-reader throw-foo-fn]
             (-> (dremel-paper-writer) byte-buffer! byte-buffer-reader read)))))
    (testing "file-reader"
      (is (thrown-with-msg?
           Exception #"foo"
           (with-redefs [dendrite.record-group/file-channel-reader throw-foo-fn]
             (with-open [w (file-writer tmp-filename (-> dremel-paper-schema-str schema/read-string))]
               (write! w [dremel-paper-record1]))
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
           (helpers/throw-cause (doto (dremel-paper-writer)
                                  (write! [bad-record]))))))
    (testing "invalid-input-handler can process exceptions"
      (let [error-atom (atom nil)
            w (doto (byte-buffer-writer (schema/read-string dremel-paper-schema-str)
                                        :invalid-input-handler (fn [record e] (reset! error-atom record)))
                (write! [bad-record dremel-paper-record1 dremel-paper-record2]))]
        (is (= @error-atom bad-record))
        (is (= [dremel-paper-record1 dremel-paper-record2]
               (read (-> w byte-buffer! byte-buffer-reader))))))))

(deftest strict-queries
  (testing "queries fail when missing-fields-as-nil? is false and we query a missing fields"
    (let [bb (byte-buffer! (dremel-paper-writer))]
      (is (= [nil nil] (read (byte-buffer-reader bb :query {:foo '_}))))
      (is (thrown-with-msg?
           IllegalArgumentException #"The following fields don't exist: \[:foo\]"
           (helpers/throw-cause (read (byte-buffer-reader bb :query {:foo '_}
                                                          :missing-fields-as-nil? false))))))))

(deftest readers
  (testing "readers functions transform output"
    (let [bb (byte-buffer! (dremel-paper-writer))]
      (is (= [{:name 3, :docid 10} {:name 1, :docid 20}]
             (read (byte-buffer-reader bb :query {:docid '_ :name (schema/tag 'foo '_)}
                                       :readers {'foo count}))))
      (is (thrown-with-msg?
           IllegalArgumentException #"No reader function was provided for tag 'foo'"
           (helpers/throw-cause
            (read (byte-buffer-reader bb :query {:docid '_ :name (schema/tag 'foo '_)}))))))))
