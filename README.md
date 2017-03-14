# Dendrite

Dendrite is a library for querying large datasets on a single host at near-interactive speeds.

It attempts to be:

- __simple__: there is no configuration, no services to run, and reads are as simple as opening a file;
- __fast__: there are few bottlenecks and reads will usually make good use of all available CPU cores;
- __compact__: the file size is typically 30-40% lower than the equivalent compressed JSON;
- __flexible__: it supports the same rich set of nested data structures as [EDN][];
- __write once, read often__: optimizations are run at write-time to ensure fast read-time performance.

[EDN]: https://github.com/edn-format/edn


The current implementation is in Java but only exposes a Clojure API. In the future, I would like to expose a
clean Java interface and build a C implementation for non-JVM code.

This code has been in used in production for over a year. It has been successfully used both as a building
block in large ETL systems and for ad-hoc data-science studies. However, prior to the 1.0 release, no effort
will be made at preserving backwards compatibility of APIs or binary compatibility of files.

Dendrite implements the record shredding and assembly ideas from Google's [Dremel paper][Dremel] [1]. Querying
for only small parts of the stored records can be up to several orders of magnitude faster than fully
deserializing each record and pulling out the desired information. Furthermore, this library also borrows many ideas from the [Parquet][] project, an implementation of the
Dremel file format for Hadoop. Unlike Parquet, Dendrite is not tied to any particular ecosystem and is
designed to be a small library with no external dependencies.


[Dremel]: http://research.google.com/pubs/pub36632.html
[Parquet]: http://parquet.io/

__Status update__ (March 13, 2017): For personal reasons, I haven't been able to work on this project in the
past year. However, I have been accumulating ideas for the next iteration and hope to resume progress
later this year.

[![Build Status](https://travis-ci.org/jwhitbeck/dendrite.png)](https://travis-ci.org/jwhitbeck/dendrite.png)

## Documentation

Work-in-progress documentation and benchmarks are available at [dendrite.tech](http://dendrite.tech).

## Roadmap to 1.0

- Improve writer performance.
- Cleanly separate clojure and java code.
- Expose a good Java API
- Preserve presence/absence of record keys
- Add indexing

## References

1. Sergey Melnik, Andrey Gubarev, Jing Jing Long, Geoffrey Romer, Shiva Shivakumar, Matt Tolton, Theo Vassilakis.
[Dremel: Interactive Analysis of Web-Scale Datasets][Dremel].
In _Proc. VLDB_, 2010

## License

Copyright &copy; 2013-2017 John Whitbeck

Distributed under the Eclipse Public License, the same as Clojure.
