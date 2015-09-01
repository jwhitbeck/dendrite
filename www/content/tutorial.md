---
date: 2015-08-23T19:31:12-07:00
menu:
    main:
        weight: -1
title: Tutorial
---

Let's get started with dendrite. This tutorial will take you through the basics of file I/O and show you how
take advantage of the columnar format for your queries.

The examples below assume that you have required the `dendrite.core` namespace as follows:

{{< highlight clojure >}}
(require '[dendrite.core :as d])
{{</ highlight >}}

## Reading and writing files

Writing to dendrite files should feel very similar to writing other types of files in clojure.

{{< highlight clojure >}}
(def filename "contacts.den")

(def schema {:name {:first 'string :last 'string}
             :emails ['string]})

(def contacts
  [{:name {:first "Alice" :last "Jones"}
    :emails ["alice@jones.com" "alice@acme.org"]}
   {:name {:first "Bob" :last "Smith"}
    :emails ["bob@smith.com"]}])

(with-open [w (d/file-writer schema filename)]
  (doseq [contact contacts]
    (.write w contact)))
{{</ highlight >}}

A few things to note:

- The writer needs to be aware of the _schema_ describing the structure and types of the records to be
  serialized. We'll see more advanced schemas later on, but you can think of them as basically just the same
  clojure object with type symbols instead of leaf values.
- The records are passed _as-is_ to the writer. No extra serialization step is needed.

In this example, the schema has three _columns_. The nested structure of the objects is encoded in the schema,
but the leaf values (e.g., "Alice", "bob@smith.com") are written out to three separate columns. For a good
primer on how this is done in practice, check out this
[blog post](https://blog.twitter.com/2013/dremel-made-simple-with-parquet) from the
[Parquet](https://parquet.apache.org/) project.

Reading is even simpler. Since the schema is stored within the file, it is not required for reading.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read r)))
;= ({:name {:first "Alice" :last "Jones"}
;    :emails ["alice@jones.com" "alice@acme.org"]}
;   {:name {:first "Bob" :last "Smith"}
;    :emails ["bob@smith.com"]})
{{</ highlight >}}


## Querying subsets of the data

Columnar storage formats shine when only reading subsets of the data. Dendrite has two mechanisms for
restricting the amount of data that needs to be deserialized: sub-schemas and queries. As we will see, they
can be used separately or together.

### Reading sub-schemas

Sub-schemas are the simplest way of drilling down into the records in a dendrite file. They are the equivalent
of calling `get-in` on each record. The following examples use the second arity of `dendrite.core/read` that
expects an options map as its first arg.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read {:sub-schema-in [:name]} r)))
;= ({:first "Alice" :last "Jones"}
;   {:first "Bob" :last "Smith"})

(with-open [r (d/file-reader filename)]
  (doall (d/read {:sub-schema-in [:name :first]} r)))
;= ("Alice" "Bob")

(with-open [r (d/file-reader filename)]
  (doall (d/read {:sub-schema-in [:emails]} r)))
;= (["alice@jones.com" "alice@acme.org"]
;   ["bob@smith.com"])
{{</ highlight >}}

Attempting to use the `:sub-schema-in` option to cross into a repeated field will throw an exception.

### Queries

Queries specify a 'skeleton' structure of the records that should be read from the file.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read {:query {:name {:last 'string}}} r)))
;= ({:name {:last "Jones"}}
;   {:name {:last "Smith"}})
{{</ highlight >}}

For ease of use, underscore symbols in queries are a shorthand for _everything in the schema under this
point_. For example, the following code snippet defines the same query as above.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read {:query {:name {:last '_}}} r)))
;= ({:name {:last "Jones"}}
;   {:name {:last "Smith"}})
{{</ highlight >}}

Underscore symbols can stand in for entire sub-schemas.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read {:query {:name '_}} r)))
;= ({:name {:first "Alice" :last "Jones"}}
;   {:name {:first "Bob" :last "Smith"}})
{{</ highlight >}}

### Sub-schema queries

It is of course possible to combine sub-schemas and queries. If both the `:sub-schema-in` and `:query` options
are provided, the query is applied to the sub-schema.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read {:sub-schema-in [:name] :query {:first '_}} r)))
;= ({:first "Alice"} {:first "Bob"})
{{</ highlight >}}

## File-level metadata

Dendrite files may contain arbitrary file-level metadata. This is useful in many contexts, whether you want to
keep track of the commit sha1 of the code that created the file or just add a description.

Set the metadata with `set-metadata!`.

{{< highlight clojure >}}
(with-open [w (d/file-writer schema filename)]
  (doseq [contact contacts]
    (.write w contact))
  (d/set-metadata! w {:description "Contacts" :owner "carl"})
{{</ highlight >}}

Read the metadata with `metadata`.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (d/metadata r))
;= {:description "Contacts" :owner "carl"}
{{</ highlight >}}

## Opting into strictness

## Schema manipulation

Dendrite schemas are regular immutable clojure objects and not a separate DSL. This may seem like a detail but
it makes them very easy to build and modify programatically. All the usual clojure data manipulation functions
such as `assoc`, `dissoc`, or `merge` will work as expected on schemas. Furthermore, they can be serialized to
[EDN](https://github.com/edn-format/edn) using the usual `pr-*` functions.

### Modifying an existing schema

Let's say we want to add an id to all our contacts. We could read-in all the existing contacts and use `range`
to add a simple incrementing id to the existing contacts.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  ;; Add the :id field with type long to the schema.
  (let [schema-with-id (-> (d/schema r) (assoc :id 'long))]
    (with-open [w (d/file-writer schema-with-id "contacts2.den")]
      (doseq [c (map #(assoc %1 :id %2) (d/read r) (range))]
        (.write w c)))))
{{</ highlight >}}

Let's see if that worked.

{{< highlight clojure >}}
(with-open [r (d/file-reader "contacts2.den")]
  (doall (d/read r)))
;= ({:id 0
;    :name {:first "Alice", :last "Jones"}
;    :emails ["alice@jones.com" "alice@acme.org"]}
;   {:id 1
;    :name {:first "Bob", :last "Smith"}
;    :emails ["bob@smith.com"]})
{{</ highlight >}}

### Serialization

TODO: Use required tags

As we have done so far, schemas can be stored directly inside clojure code. However, it is often convenient to
store them in separate files.

{{< highlight clojure >}}
(spit "schema.edn" (pr-str schema))
{{</ highlight >}}

To read a schema, use the `read-schema-string` function.

{{< highlight clojure >}}
(-> "schema.edn" slurp d/read-schema-string))
;= {:name {:first 'string :last 'string}
;          :emails ['string]}
{{</ highlight >}}

In this case, we could also have used `clojure.edn/read-string` instead of dendrite's
`read-schema-string`. However, the latter is preferred since it properly reads the dendrite schema annotations
(see below).

Large complex schemas can be quite hard to read


## Customizing record assembly
