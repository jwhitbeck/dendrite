;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.compressors-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :refer [lorem-ipsum]])
  (:import [dendrite.java ByteArrayWriter ByteArrayReader
            Compressor Decompressor
            DeflateCompressor DeflateDecompressor
            LZ4Compressor LZ4Decompressor]))

(set! *warn-on-reflection* true)

(defn compress-decompress-lorum-ipsum [^Compressor compressor ^Decompressor decompressor]
  (let [baw (ByteArrayWriter. 10)
        compressed-baw (ByteArrayWriter. 10)]
    (.writeByteArray baw (.getBytes (str lorem-ipsum) "UTF-8"))
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
