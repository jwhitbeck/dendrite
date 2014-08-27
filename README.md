# Dendrite

A high-performance columnar storage format for Clojure.

Dendrite supports all of Clojure's rich datatypes and is designed for complex nested data structures. It
implements the record shredding and assembly ideas from Google's Dremel paper [1]. Querying for only small
parts of the stored records can be up to several orders of magnitude faster than fully deserializing each
record and pulling out the desired information.

Furthermore, this library also borrows many ideas from the Parquet project [2], an implementation of the
Dremel file format for Hadoop. Unlike Parquet, Dendrite is not tied to any particular ecosystem and is
designed to be a small library with very few external dependencies (`net.jpountz.lz4/lz4` for LZ4 compression
and `org.clojure/data.fressian` for metadata serialization).

Word of warning: this code has not yet been battle-tested. Prior to the 1.0 release, no effort will be made at
preserving backwards compatibility of APIs or binary compatibility of files.

## Usage

FIXME

## Benchmarks

FIXME

## References

1. Sergey Melnik, Andrey Gubarev, Jing Jing Long, Geoffrey Romer, Shiva Shivakumar, Matt Tolton, Theo Vassilakis. [Dremel: Interactive Analysis of Web-Scale Datasets](http://research.google.com/pubs/pub36632.html). In _Proc. VLDB_, 2010
2. [Parquet](http://parquet.io/)
3. [EDN](https://github.com/edn-format/edn)

## License

Copyright &copy; 2013-2014 John Whitbeck

Distributed under the Eclipse Public License, the same as Clojure.
