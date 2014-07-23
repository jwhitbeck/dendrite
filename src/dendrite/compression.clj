(ns dendrite.compression
  (:import [dendrite.java Compressor Decompressor LZ4Compressor LZ4Decompressor
            DeflateCompressor DeflateDecompressor]))

(defn- invalid-compression-type-exception [x]
  (IllegalArgumentException. (str x " is not a valid compression-type. "
                                  "Supported types are: :none, :lz4, :deflate.")))

(defn compressor [compression-type]
  (case compression-type
    :none nil
    :lz4 (LZ4Compressor.)
    :deflate (DeflateCompressor.)
    (throw (invalid-compression-type-exception compression-type))))

(defn decompressor-ctor [compression-type]
  (case compression-type
    :none (constantly nil)
    :lz4 #(LZ4Decompressor.)
    :deflate #(DeflateDecompressor.)
    (throw (invalid-compression-type-exception compression-type))))
