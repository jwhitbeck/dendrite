(ns dendrite.compression
  (:import [dendrite.java Compressor Decompressor LZ4Compressor LZ4Decompressor
            DeflateCompressor DeflateDecompressor]))

(def supported-compression-types #{:none :lz4 :deflate})

(defn valid-compression-type? [compression-type]
  (contains? supported-compression-types compression-type))

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
