---
date: 2015-08-23T19:31:12-07:00
showtoc: true
weight: 1
menu: main
title: Tutorial
---

# Tutorial

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

(def schema {:name {:first 'string
                    :last 'string}
             :emails ['string]})

(def contacts
  [{:name {:first "Alice"
           :last "Jones"}
    :emails ["alice@jones.com" "alice@acme.org"]}

   {:name {:first "Bob"
           :last "Smith"}
    :emails ["bob@smith.com"]}])

(with-open [w (d/file-writer schema filename)]
  (doseq [c contacts]
    (.write w c)))
{{</ highlight >}}

A few things to note:

- The writer needs to be aware of the _schema_ describing the structure and the types of the records to be
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


## Reading subsets of the data

Columnar storage formats shine when only reading subsets of the data. Indeed, the on-disk bytes for unselected
columns can be skipped entirely. Dendrite has two mechanisms for restricting the amount of data that needs to
be deserialized: sub-schemas and queries. As we will see, they can be used separately or together.

### Sub-schemas

Sub-schemas are the simplest way of drilling down into the records contained in a dendrite file. They are the
equivalent of calling `get-in` on each record. The following examples use the second arity of
`dendrite.core/read` that expects an options map as its first arg.

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

## Metadata

Dendrite files may contain arbitrary file-level metadata. This is useful in many contexts, whether you want to
keep track of the commit sha1 of the code that created the file or just add a description.

Set the metadata with `set-metadata!`.

{{< highlight clojure >}}
(with-open [w (d/file-writer schema filename)]
  (doseq [c contacts]
    (.write w c))
  (d/set-metadata! w {:description "Contacts" :owner "carl"})
{{</ highlight >}}

Read the metadata with `metadata`.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (d/metadata r))
;= {:description "Contacts" :owner "carl"}
{{</ highlight >}}

## Opting into strictness

### Tolerant writes by default

By default, dendrite is quite tolerant about writing objects that don't strictly adhere to the write
schema. Specifically, it considers that all fields are optional and silently drops fields that are not in the
schema.

{{< highlight clojure >}}
(def contact-with-no-name
  {:emails ["noname@none.com"]})

(def contact-with-extra-fields
  {:name {:first "Alice" :last "Jones"}
   :emails [nil]
   :age 24})

(with-open [w (d/file-writer schema filename)]
  (.write w contact-with-no-name)
  (.write w nil) ; Completely empty record
  (.write w contact-with-extra-fields))

(with-open [r (d/file-reader filename)]
  (doall (d/read r)))
;= ({:emails ["noname@none.com"]}
;   nil
;   {:name {:first "Alice" :last "Jones"}
;   :emails [nil]})
{{</ highlight >}}

Notice how Alice's age was not written to the file.

### Required fields

In many cases, it is useful to enforce the presence of certain record elements and throw an exception if any
are missing. Dendrite schemas can be annotated to mark certain elements as required (i.e., cannot be nil)
using the 'd/req` function.

{{< highlight clojure >}}
(def strict-schema
  (d/req ; nil records not allowed
    ; :name must be present but :first and :last are still optional
    {:name (d/req {:first 'string
                   :last 'string})
     ; :emails is optional but, if defined, the
     ;  email strings cannot be nil
     :emails [(d/req 'string)]}))
{{</ highlight >}}

Let's try writing the same records as in the previous example using this stricter schema.

{{< highlight clojure >}}
(with-open [w (d/file-writer strict-schema filename)]
  (.write w contact-with-no-name))
; Throws exception. Required field :name is missing

(with-open [w (d/file-writer strict-schema filename)]
  (.write w nil))
; Throws exception. Required record is missing

(with-open [w (d/file-writer strict-schema filename)]
  (.write w contact-with-extra-fields))
; Throws exception. Required email is nil.
{{</ highlight >}}

### Alert on extra fields

Keeping schemas in sync with application code can be tricky. In particular, you may want to throw an error if
the record to write contains fields that are not present in the schema. The `d/file-writer` function has a
second arity that accepts an options map to customize the writer's behavior. In this case, we want to set
`:ignore-extra-fields?` to `false`.

{{< highlight clojure >}}
(with-open [w (d/file-writer {:ignore-extra-fields? false}
                              schema filename)]
  (.write w contact-with-extra-fields))
; Throws exception. :age is not is schema.
{{</ highlight >}}

### Handling invalid records

In some contexts, it may be preferable to handle the occasional invalid record rather than crashing the
writing process. Dendrite supports that use case through the `:invalid-input-handler` option. This is a
function of two arguments: the record and the exception. Let's use this to populate an atom of invalid
records.

{{< highlight clojure >}}
(def invalid-records (atom []))

(defn handler [rec e]
  (swap! invalid-records conj rec))

(with-open [w (d/file-writer {:invalid-input-handler handler}
                             strict-schema filename)]
  ; Write some valid contacts
  (doseq [c contacts]
    (.write w c))
  ; Write an invalid contact
  (.write w contact-with-no-name))
;= nil

@invalid-records
;= [{:emails ["noname@none.com"]}]

; Verify that the valid records were successfully written
(with-open [r (d/file-reader filename)]
  (count (d/read r)))
;= 2

{{</ highlight >}}

### Reading missing columns

The previous sections covered enforcing various write-time checks. By default, dendrite is also quite tolerant
on reads. In particular, if a query requests fields that are not present in the file's schema, those fields
will simply be ignored.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  ; The :age field does not exist in the schema
  (doall (d/read {:query {:name {:first '_} :age '_}} r)))
;= ({:name {:first "Alice"}}
;   {:name {:first "Bob"}})
{{</ highlight >}}

This makes it very easy to apply the same query to many files, of which some may not have the requested
field. However, this tolerant behavior may not always be desirable and can be disabled by setting the
`:missing-fields-as-nil?` read option to `false`.

{{< highlight clojure >}}
(with-open [r (d/file-reader filename)]
  (doall (d/read {:query {:name {:first '_} :age '_}
                  :missing-fields-as-nil? false}
                  r)))
;= Throws exception. :age field does not exist.
{{</ highlight >}}

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

So far in this tutorial, schemas have been defined directly in the clojure code. However it is often
convenient to store them in separate files.

{{< highlight clojure >}}
; The simple schema from the beginning of the tutorial
(spit "schema.edn" (pr-str schema))
; The somewhat stricter schema from the "Opting into strictness"
; section
(spit "strict-schema.edn" (pr-str strict-schema))
{{</ highlight >}}

Let's see what those files look like.

{{< highlight clojure >}}
(slurp "schema.edn")
;= "{:name {:first string, :last string}, :emails [string]}"
(slurp "strict-schema.edn")
;= "#req {:name #req {:first string, :last string}, :emails [#req string]}"
{{</ highlight >}}

Note how the required parts of the schema are tagged with `#req` in the EDN string. Pretty-printing also works
as expected and makes large schemas easier to inspect.

To read a schema, use the `d/read-schema-string` function. This is basically the same as
`clojure.edn/read-string` with special reader functions for the `#req` and `#col` schema annotations (see
[advanced]({{< relref "advanced.md" >}}) for a description of `#col`).

{{< highlight clojure >}}
(-> "schema.edn" slurp d/read-schema-string))
;= {:name {:first 'string :last 'string}
;          :emails ['string]}
{{</ highlight >}}


## Next steps

We hope this tutorial was helpful in getting you started. However, it only scratches the surface of dendrite's
capabilities. [Advanced]({{< relref "advanced.md" >}}) functionality include injecting functions into the
record assembly process, custom logical types, and first class support for clojure's [transducers]({{< link
transducers >}}) and [reducers]({{< link reducers >}}). These topics are covered in the [advanced]({{<
relref "advanced.md" >}}) section and benefit from a good understanding of dendrite's [variation]({{< relref
"shredding.md" >}}) on the [Dremel]({{< link dremel >}}) record shredding algorithm, its [file format]({{<
relref "format.md" >}}), and its multi-threaded [implementation]({{< relref "implementation.md" >}}).
