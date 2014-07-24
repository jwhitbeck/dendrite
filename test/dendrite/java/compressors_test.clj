(ns dendrite.java.compressors-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :refer [lorem-ipsum]])
  (:import [dendrite.java ByteArrayWriter ByteArrayReader
            Compressor Decompressor
            DeflateCompressor DeflateDecompressor
            LZ4Compressor LZ4Decompressor]))

(defn compress-decompress-lorum-ipsum [compressor decompressor]
  (let [baw (ByteArrayWriter. 10)
        compressed-baw (ByteArrayWriter. 10)]
    (.writeByteArray baw (.getBytes lorem-ipsum "UTF-8"))
    (doto compressor
      (.compress baw)
      (.flush compressed-baw))
    (let [bar (ByteArrayReader. (.buffer compressed-baw))]
      (-> decompressor
          (.decompress bar (.compressedLength compressor) (.uncompressedLength compressor))
          .buffer
          (String. "UTF-8")))))

(deftest deflate-test
  (testing "deflate compression"
    (is (= lorem-ipsum (compress-decompress-lorum-ipsum (DeflateCompressor.) (DeflateDecompressor.))))))

(deftest lz4-test
  (testing "deflate compression"
    (is (= lorem-ipsum (compress-decompress-lorum-ipsum (LZ4Compressor.) (LZ4Decompressor.))))))
