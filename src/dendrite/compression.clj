(ns dendrite.compression
  (:import [dendrite.java Compressor Decompressor LZ4Compressor LZ4Decompressor
            DeflateCompressor DeflateDecompressor]))

(defn compressor [compression-type]
  (case compression-type
    :none nil
    :lz4 (LZ4Compressor.)
    :deflate (DeflateCompressor.)))

(defn decompressor-ctor [compression-type]
  (case compression-type
    :none (constantly nil)
    :lz4 #(LZ4Decompressor.)
    :deflate #(DeflateDecompressor.)))
