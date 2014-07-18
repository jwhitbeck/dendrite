(ns dendrite.core-test
  (:require [clojure.test :refer :all]
            [dendrite.core :refer :all]
            [dendrite.dremel-paper-examples :refer :all]
            [dendrite.schema :as schema]
            [dendrite.test-helpers :as helpers])
  (:refer-clojure :exclude [read]))

(defn- dremel-paper-writer []
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
              {:docid 20}]
             (-> byte-buffer
                 (byte-buffer-reader :query {:docid '_ :name [{:language [{:country '_}]}]})
                 read))))))

(deftest random-records-write-read
  (let [records (take 100 (helpers/rand-test-records))
        writer (doto (byte-buffer-writer (-> helpers/test-schema-str schema/read-string))
                 (#(reduce write! % records)))
        byte-buffer (byte-buffer! writer)]
    (testing "full schema"
      (is (= records (read (byte-buffer-reader byte-buffer)))))))

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
                   (#(reduce write! % records)))
          byte-buffer (byte-buffer! writer)]
      (is (->> (byte-buffer-reader byte-buffer)
               stats
               :record-groups
               rest
               butlast
               (map :length)
               helpers/avg
               (helpers/roughly target-record-group-length))))))
