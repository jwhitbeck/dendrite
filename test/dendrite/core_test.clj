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
        writer (doto (byte-buffer-writer test-schema :optimize-columns? :all)
                 (write! records))
        reader (-> writer byte-buffer! byte-buffer-reader)]
    (is (= (str "{:docid #req #col {:encoding :delta, :type :long},"
                " :links {:backward (long), :forward [long]},"
                " :name [{:language [{:code #req #col {:encoding :dictionary, :type :string},"
                                    " :country #col {:encoding :dictionary, :type :string}}],"
                " :url #col {:encoding :incremental, :type :string}}],"
                " :meta {#col {:encoding :dictionary, :type :string}"
                       " #col {:encoding :dictionary, :type :string}},"
                " :keywords #{#col {:compression :lz4, :encoding :dictionary, :type :string}}}")
           (str (schema reader))))))

(deftest custom-metadata
  (let [test-custom-metadata {:foo {:bar "test"} :baz [1 2 3]}
        writer (doto (dremel-paper-writer)
                 (set-metadata! test-custom-metadata))
        byte-buffer (byte-buffer! writer)]
    (is (= test-custom-metadata (-> byte-buffer byte-buffer-reader metadata)))))

(deftest corrupt-data
  (let [byte-buffer (byte-buffer! (dremel-paper-writer))]
    (testing "corrupt magic bytes at file start"
      (let [bad-byte-pos 2
            tmp-byte (.get byte-buffer bad-byte-pos)]
        (is (thrown? IllegalArgumentException
                     (byte-buffer-reader (doto byte-buffer (.put bad-byte-pos (byte 0))) '_)))
        (.put byte-buffer bad-byte-pos tmp-byte)))
    (testing "corrupt magic bytes at file end"
      (let [bad-byte-pos (- (.limit byte-buffer) 2)
            tmp-byte (.get byte-buffer bad-byte-pos)]
        (is (thrown? IllegalArgumentException
                     (byte-buffer-reader (doto byte-buffer (.put bad-byte-pos (byte 0))) '_)))
        (.put byte-buffer bad-byte-pos tmp-byte)))))

(deftest record-group-lengths
  (testing "record-group lengths are approximately equal to target-record-group-length"
    (let [records (take 1000 (helpers/rand-test-records))
          target-record-group-length (* 3 1024)
          writer (doto (byte-buffer-writer (-> helpers/test-schema-str schema/read-string)
                                           :target-record-group-length target-record-group-length)
                   (write! records))
          byte-buffer (byte-buffer! writer)]
      (is (->> (byte-buffer-reader byte-buffer)
               stats
               :record-groups
               rest
               butlast
               (map :length)
               helpers/avg
               (helpers/roughly target-record-group-length))))))

(deftest errors
  (testing "exceptions in the writing thread are caught in the main thread"
    (testing "byte-buffer-writer"
      (is (thrown? Exception
                   (with-redefs [flush-record-group! (constantly (throw (Exception. "foo")))]
                     (dremel-paper-writer)))))
    (testing "file-writer"
      (is (thrown? Exception
                   (with-redefs [dendrite.record-group/flush-column-chunks-to-byte-buffer
                                   (constantly (throw (Exception. "foo")))]
                     (with-open [w (file-writer tmp-filename dremel-paper-schema)]
                       (write! w [dremel-paper-record1])))))))
  (testing "exceptions in the reading thread are caught in the main thread"
    (testing "byte-buffer-reader"
      (is (thrown? Exception
                   (with-redefs [record-group-readers (constantly (throw (Exception. "foo")))]
                     (-> (dremel-paper-writer) byte-buffer! byte-buffer-reader read)))))
    (testing "file-reader"
      (is (thrown? Exception
                   (with-redefs [dendrite.record-group/file-channel-reader
                                 (constantly (throw (Exception. "foo")))]
                     (with-open [w (file-writer tmp-filename dremel-paper-schema)]
                       (write! w [dremel-paper-record1]))
                     (with-open [r (file-reader tmp-filename)]
                       (read r))))))))
