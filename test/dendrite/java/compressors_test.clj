(ns dendrite.java.compressors-test
  (:require [clojure.test :refer :all]
            [dendrite.java.test-helpers :refer [lorem-ipsum]])
  (:import [dendrite.java ByteArrayWriter ByteArrayReader
            Compressor Decompressor
            DeflateCompressor DeflateDecompressor]))

(deftest deflate-test
  (testing "Deflate compression works"
    (let [baw (ByteArrayWriter. 10)
          compressed-baw (ByteArrayWriter. 10)
          compressor (DeflateCompressor.)
          lorem-ipsum-bytes (.getBytes lorem-ipsum "UTF-8")]
      (.writeByteArray baw lorem-ipsum-bytes)
      (.compressBytes compressor baw)
      (.writeCompressedTo compressor compressed-baw)
      (let [bar (ByteArrayReader. (.buffer compressed-baw))
            decompressed-bytes
              (-> (DeflateDecompressor.)
                  (.decompress bar (.compressedSize compressor) (.uncompressedSize compressor))
                  .buffer)
            decompressed-string (String. decompressed-bytes "UTF-8")]
        (is (= lorem-ipsum decompressed-string))))))
