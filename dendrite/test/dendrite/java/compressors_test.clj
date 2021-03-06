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
  (:import [dendrite.java MemoryOutputStream
            ICompressor IDecompressor
            Deflate$Compressor Deflate$Decompressor]
           [java.nio ByteBuffer]))

(set! *warn-on-reflection* true)

(defn compress-decompress-lorum-ipsum [^ICompressor compressor ^IDecompressor decompressor]
  (let [mos (MemoryOutputStream. 10)
        compressed-mos (MemoryOutputStream. 10)]
    (.write mos (.getBytes (str lorem-ipsum) "UTF-8"))
    (doto compressor
      (.compress mos)
      (.writeTo compressed-mos))
    (let [bb (.toByteBuffer compressed-mos)]
      (-> decompressor
          (.decompress bb (.getLength compressor) (.getUncompressedLength compressor))
          .array
          (String. "UTF-8")))))

(deftest deflate-test
  (testing "deflate compression"
    (is (= lorem-ipsum (compress-decompress-lorum-ipsum (Deflate$Compressor.) (Deflate$Decompressor.))))))
