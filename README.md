# Dendrite

A columnar storage format for Clojure.

Dendrite supports all of Clojure's rich datatypes and is designed for complex nested data structures. It
implements the record shredding and assembly ideas from Google's
[Dremel paper][Dremel] [1]. Querying for only small parts of the stored
records can be up to several orders of magnitude faster than fully deserializing each record and pulling out
the desired information.

[Dremel]: http://research.google.com/pubs/pub36632.html

Furthermore, this library also borrows many ideas from the [Parquet][] project, an implementation of the
Dremel file format for Hadoop. Unlike Parquet, Dendrite is not tied to any particular ecosystem and is
designed to be a small library with very few external dependencies ([lz4-java][] for LZ4 compression and
[fressian][] for metadata serialization).

[Parquet]: http://parquet.io/
[lz4-java]: https://github.com/jpountz/lz4-java
[fressian]: https://github.com/clojure/data.fressian

Word of warning: this code has not yet been battle-tested. Prior to the 1.0 release, no effort will be made at
preserving backwards compatibility of APIs or binary compatibility of files.

[![Build Status](https://travis-ci.org/jwhitbeck/dendrite.png)](https://travis-ci.org/jwhitbeck/dendrite.png)

## Basic usage

```clojure
[dendrite "0.2.1"]

(require '[dendrite.core :as d])

;;; Define a schema for writing to a dendrite file. This schema is the one from the
;;; dremel paper. A dendrite schema is basically the clojure structure of the
;;; records to be serialized with type symbols instead of the actual values.
(def schema {:docid 'long
             :links {:forward ['long]
                     :backward ['long]}
             :name [{:language [{:code 'string
                                 :country 'string}]
                     :url 'string}]})

;;; Some example records
(def record1
  {:docid 10
   :links {:forward [20 40 60]}
   :name [{:language [{:code "en-us" :country "us"}
                      {:code "en"}]
           :url "http://A"}
          {:url "http://B"}
          {:language [{:code "en-gb" :country "gb"}]}]})

(def record2
  {:docid 20
   :links {:backward [10 30]
           :forward [80]}
   :name [{:url "http://C"}]})

;;; write the records to a file
(with-open [w (d/file-writer schema "/path/to/file.den")]
  (d/write! w record1)
  (d/write! w record2))

;;; read the full records from the file
(with-open [r (d/file-reader "/path/to/file.den")]
  (doall (d/read r)))
;;; => [record1 record2]

;;; read only the docid and the country
(with-open [r (d/file-reader "/path/to/file.den")]
  (let [query {:docid 'long
               :name [{:language [{:country 'string}]}]}]
    (doall (d/read {:query query} r))))
;;; => [{:docid 10
;;;      :name [{:language [{:country "us"} nil]}
;;;             nil
;;;             {:language [{:country "gb"}]}]}
;;;     {:docid 20 :name [nil]}]
```

For more information, check out the [full documentation](FIXME).

## Benchmarks

FIXME

## References

1. Sergey Melnik, Andrey Gubarev, Jing Jing Long, Geoffrey Romer, Shiva Shivakumar, Matt Tolton, Theo Vassilakis.
[Dremel: Interactive Analysis of Web-Scale Datasets][Dremel].
In _Proc. VLDB_, 2010

## License

Copyright &copy; 2013-2015 John Whitbeck

Distributed under the Eclipse Public License, the same as Clojure.
