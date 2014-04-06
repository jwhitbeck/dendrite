(ns dendrite.java.compressors-test
  (:require [clojure.test :refer :all]
            [dendrite.java.test-helpers :refer [lorem-ipsum]])
  (:import [dendrite.java ByteArrayWriter ByteArrayReader
            Compressor Decompressor
            DeflateCompressor DeflateDecompressor
            LZ4Compressor LZ4Decompressor]))

(defn compress-decompress-lorum-ipsum [compressor decompressor]
  (let [baw (ByteArrayWriter. 10)
        compressed-baw (ByteArrayWriter. 10)]
    (.writeByteArray baw (.getBytes lorem-ipsum "UTF-8"))
    (.compress compressor (.buffer baw) 0 (.size baw) compressed-baw)
    (let [bar (ByteArrayReader. (.buffer compressed-baw))]
      (-> decompressor
          (.decompress bar (.compressedSize compressor) (.uncompressedSize compressor))
          .buffer
          (String. "UTF-8")))))

(deftest deflate-test
  (testing "Deflate compression works"
    (is (= lorem-ipsum (compress-decompress-lorum-ipsum (DeflateCompressor.) (DeflateDecompressor.))))))

(deftest lz4-test
  (testing "Deflate compression works"
    (is (= lorem-ipsum (compress-decompress-lorum-ipsum (LZ4Compressor.) (LZ4Decompressor.))))))
