---
date: 2015-08-23T19:31:12-07:00
showtoc: true
weight: 1
menu: main
title: Tutorial
---

# Tutorial

Let's get started with dendrite. This tutorial will take you through the basics of file I/O, show you how take
advantage of the columnar format for your queries, and how to efficiently apply transformations on reads.

The examples below assume that you have started a REPL in a project that imports dendrite as

{{< highlight clojure >}}
[dendrite "0.5.12"]
{{</ highlight >}}

and required the `dendrite.core` namespace as

{{< highlight clojure >}}
(require '[dendrite.core :as d])
{{</ highlight >}}

## Read and write files

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

## Supported types {#supported-types}

Dendrite's built-in types are a superset of those of [EDN]({{< link edn >}}). In dendrite schemas, types are
denoted using clojure symbols.

| Type                      | Description                                |
|---------------------------|--------------------------------------------|
| `boolean`                 | true or false                              |
| `int`                     | a 32-bit integer                           |
| `long`                    | a 64-bit integer                           |
| `float`                   | an IEEE 32-bit floating point value        |
| `double`                  | an IEEE 64-bit floating point value        |
| `byte-array`              | an arbitrary byte-array                    |
| `fixed-length-byte-array` | a byte array with a set size               |
| `string`                  | a string                                   |
| `inst`                    | a date                                     |
| `uuid`                    | a Universally Unique identifier            |
| `char`                    | a character or 16-bit integer              |
| `bigint`                  | an arbitrary-size integer                  |
| `bigdec`                  | an arbitrary-precision decimal number      |
| `ratio`                   | a ratio of two arbitrary-precision numbers |
| `keyword`                 | an EDN keyword                             |
| `symbol`                  | an EDN symbol                              |
| `byte-buffer`             | a java.nio.ByteBuffer                      |

The `boolean`, `int`, `long`, `float`, `double`, `byte-array`, and `fixed-length-byte-array` types are
dendrite's _primitive types_. All the others are _logical types_ that converted to and form a _primitive type_
during serialization and deserialization. This list of types can be easily extended to suit your needs by
defining [custom logical types]({{< relref "#custom-types" >}}).

In the spirit of "generous on input, strict on output", each type is associated with a _coercion function_
that does its best to convert the input data into the desired type.

For example, let's write to a simple file containing where records are just simple strings.

{{< highlight clojure >}}
(with-open [w (d/file-writer 'string "tmp.den")]
  (doseq [v [0.1   ; double
             100   ; long
             "foo" ; string
             :bar  ; keyword
             'baz  ; symbol
            ]]
    (.write w v)))

(with-open [r (d/file-reader "tmp.den")]
  (doall (d/read r)))
;= ("0.1" "100" "foo" ":bar" "baz")
{{</ highlight >}}

Note that all the values were converted to strings before being written. Obviously, it isn't always possible
to convert input values to the desired type.

{{< highlight clojure >}}
(with-open [w (d/file-writer 'int "tmp.den")]
   (.write w "foo"))
; IllegalArgumentException Could not coerce 'foo' into a int.
{{</ highlight >}}

## Read subsets of the data

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

## Opt into strictness

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

### Handle invalid records

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

### Read missing columns

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

### Modify an existing schema

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

### Serialization {#schema-serialization}

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
`clojure.edn/read-string` with special reader functions for the `#req` and `#col` schema annotations (see the
[manual column settings]({{< relref "#manual-column-settings" >}}) section below for a description of `#col`).

{{< highlight clojure >}}
(-> "schema.edn" slurp d/read-schema-string))
;= {:name {:first 'string :last 'string}
;          :emails ['string]}
{{</ highlight >}}

## File introspection {#file-introspection}

The following sections benefit from a good understanding of dendrite's [variation]({{< relref
"shredding.md" >}}) on the [Dremel]({{< link dremel >}}) record shredding algorithm, its [file format]({{<
relref "format.md" >}}), and its multi-threaded [implementation]({{< relref "implementation.md" >}}).

Furthermore, this section and all the following drop the toy _contacts_ schema used up until now and instead
rely on a test dataset consisting of 100,000 records from the [media-content benchmark]({{< relref
"benchmarks.md#media-content" >}}).

Download the [tutorial file]({{< link tutorial-file>}}) (6MB) and reference it from your REPL.

{{< highlight clojure >}}
(def file "/path/to/tutorial.den")
{{</ highlight >}}

As a columnar format, the file layout is very structured and must store data about its own organization. The
`d/stats` function retrieves low-level statistics from this internal file metadata.  These stats are useful
for debugging performance (e.g. what column takes up the most bytes?). The `d/stats` function returns an
object with three keys `:global`, `:columns`, and `:record-groups`.

### Global stats

Let's look at the global stats for the tutorial file.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (-> r d/stats :global))

;= {:num-records 100000
;   :num-record-groups 1
;   :num-columns 17
;   :length 6421801
;   :definition-levels-length 156
;   :repetition-levels-length 212898
;   :data-header-length 429
;   :data-length 5941004
;   :dictionary-header-length 57
;   :dictionary-length 266664
;   :metadata-length 422}
{{</ highlight >}}

All the `-length` values are in bytes. In the global stats, these values are aggregated across all
record-groups and column chunks in the file.

### Column stats

The object returned by `d/stats` has a `:columns` key that points to column stats aggregated across
record-groups.

Let's look a the stats for the first column.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (-> r d/stats :columns first))

;= {:path [:id]
;   :type int
;   :encoding delta
;   :compression deflate
;   :max-definition-level 0
;   :max-repetition-level 0
;   :length 2137
;   :definition-levels-length 0
;   :repetition-levels-length 0
;   :data-header-length 9
;   :data-length 2128
;   :dictionary-header-length 0
;   :dictionary-length 0
;   :num-values 100000
;   :num-dictionary-values 0
;   :num-non-nil-values 100000
;   :num-column-chunks 1
;   :num-pages 1}
{{</ highlight >}}

Here we can see this is the column for the top-level `:id` field in the [media-content record]({{< relref
"benchmarks.md#media-content-record" >}}). It consists of 32 bit integers encoded using the
[delta encoding]({{< relref "format.md#int-delta">}}) and compressed with [deflate]({{< relref
"format.md#deflate" >}}). Since its _max-definition-level_ and _max-repetition-level_ are both zero, this
column is required (i.e., cannot be nil) and non-repeated. Since the `:id` values are ordered, delta encoding
is very efficient and packs all 100,000 values into a single 2KB page (approx 0.02 bytes/values).

Now let's identify the largest columns in the file.

{{< highlight clojure >}}
(require '[clojure.pprint :as pp])

(with-open [r (d/file-reader file)]
  (->> (d/stats r)
       :columns
       (sort-by :length)
       reverse
       (map #(select-keys % [:path :type :length]))
       (take 10)
       pp/print-table))

;  |                 :path |  :type | :length |
;  |-----------------------+--------+---------|
;  |    [:images nil :uri] | string | 3064652 |
;  |         [:media :uri] | string | 1009330 |
;  | [:media :persons nil] | string |  753994 |
;  |  [:images nil :title] | string |  344648 |
;  |        [:media :size] |    int |  212518 |
;  |    [:media :duration] |    int |  209329 |
;  | [:images nil :height] |    int |  187988 |
;  |  [:images nil :width] |    int |  187987 |
;  |   [:media :copyright] | string |  116722 |
;  |       [:media :title] | string |  106425 |
{{</ highlight >}}

Notice how some paths contain a `nil`. This just indicates that the path is traversing a repeated element in
that position.

## Read transformations

### Reducers

The object returned by `d/read` is not only seqable but also reducible and foldable in the
[core.reducers]({{< link core-reducers >}}) sense.

{{< highlight clojure >}}
(require '[clojure.core.reducers :as r])

; Compute the sum of all durations in the datasets that are greater than 1000
(with-open [r (d/file-reader file)]
  (->> (d/read {:sub-schema-in [:media :duration]} r)
       (r/filter #(> % 1000))
       (r/reduce +)))
;= 165920499
{{</ highlight >}}

Following the [core.reducers]({{< link core-reducers >}}) contract, the code above doesn't build up any
intermediate lazy sequence. However, the real gains come from folding.

{{< highlight clojure >}}
; Same as above but faster
(with-open [r (d/file-reader file)]
  (->> (d/read {:sub-schema-in [:media :duration]} r)
       (r/filter #(> % 1000))
       (r/fold +)))
;= 165920499
{{</ highlight >}}

In the example above, `r/fold` will apply the filter and compute a partial sum _within each parallel assembly
thread_ (see [implementation notes]({{< relref "implementation.md" >}}) for details).

### Transducers

As explained in the [implementation notes]({{< relref "implementation.md" >}}), dendrite performs record
assembly in parallel by transforming lists of shredded records (by default 256 records per bundle) into a
[Chunk]({{< link clojure-ichunk >}}) of assembled records. This is a classic reduce operation. The
`d/eduction` function can modify this reduction with a transducer (or a composition of several
transducers). As long as the transducer is stateless, this is completely equivalent to calling
`clojure.core/eduction` on the sequence returned by `d/read`. However it is noticeably faster since the
transducer is applied in the parallel record chunk assembly threads.

The example below leverages transducers to compute the frequencies of image sizes in the whole dataset. Since
each record can define several images, we need to concatenate all the image information together before
passing it to `frequencies`.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (->> r
      (d/read {:sub-schema-in [:images]
               :query [{:size '_}]})
      (d/eduction cat (map :size))
      frequencies))
;= {"SMALL" 150603, "LARGE" 149987}
{{</ highlight >}}

It is of course possible to combine these transducers with a fold operation. In this case, the transducers are
applied to the fold's reduce function (instead of the default chunk assembly reduce function in the example
above). This efficiently produces the expected result.

For example, the somewhat contrived example below quickly computes the sum of all the heights of all images.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (->> r
      (d/read {:sub-schema-in [:images]
               :query [{:height '_}]})
      (d/eduction cat (map :height))
      (r/fold +)))
;= 59661367
{{</ highlight >}}

### Fast sampling

Dendrite can quickly skip records based on their position within the file using the `d/sample` function. This
is useful for sampling records from a file. Note that this only short-circuits record assembly, as the column
values must still be decoded before being skipped.

The example below uses `d/sample` to read one of ten records.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (->> (d/read r)
       (d/sample #(zero? (mod % 10)))
       count))
;= 10000
{{</ highlight >}}

### Indexing

The sampling functionality in the previous may not suffice if, for instance, you need to apply different
sample rates by class of records. For this use-case and others, dendrite has a generic way of inserting
indices into records using the `d/index-by` function that works like a parallel version of `map-indexed`. Note
that this indexing only occurs _after_ record assembly so this won't deliver read time improvements as
sampling.

Let's see how this works.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (->> (d/read r)
       (d/index-by #(assoc %2 :index %1))
       first))
;= {:index 0, :media {..}, :images [..]}
{{</ highlight >}}

### Reader tags

The previous transformations either modify the _sequence_ of records after assembly (reducers, transducers,
indexing) or short-circuit record assembly altogether (sampling). The reader tags described in this section
are very similar to [EDN tags]({{< link edn >}}) and enable customizing the record assembly
itself. Furthermore, when used in combination with dictionary-encoded columns they deliver one of dendrite's
key performance optimizations.

This first example uses tags to output clojure records.

Let's begin by defining clojure records for our main types.

{{< highlight clojure >}}
(defrecord MediaContent [id media images])
(defrecord Media [format height width copyright duration size
                  title persons bitrate player uri])
(defrecord Image [title uri height width size])
{{</ highlight >}}

Then, let's add tags to our dendrite query.
{{< highlight clojure >}}
(def query (d/tag 'media-content
            {:id '_
             :media (d/tag 'media '_)
             :images [(d/tag 'image '_)]}))
{{</ highlight >}}

Note that this query can be serialized to EDN.

{{< highlight clojure >}}
(def query-str (pr-str query))
;= "#media-content {:id _, :media #media _, :images [#image _]}"

(require '[clojure.edn :as edn])
(= query (edn/read-string {:default d/tag} query-str))
; true
{{</ highlight >}}

Finally, lets use our tagged query to build these nested clojure records.

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (->> r
      (d/read {:query query
               :readers {'media-content map->MediaContent
                         'media map->Media
                         'image map->Image}})
      last))
;= #user.MediaContent{
;    :id 99999,
;    :media #user.Media{
;             :format "MOV",
;             :height 480,
;             :width 480,
;             :copyright "Mybuzz",
;             :duration 1747,
;             :size 31973,
;             :title "Pellentesque viverra peed ac diam.",
;             :persons ["Antonio Harris"
;                       "Harold Price"
;                       "Janice Shaw"],
;             :bitrate 4000,
;             :player "FLASH",
;             :uri "http://merriam-webster.com/.../turpis.js"},
;             :images [#user.Image{
;                        :title "Fusce posuere felis sed lacus.",
;                        :uri "https://army.mil/.../auctor.html",
;                        :height 240,
;                        :width 160,
;                        :size "LARGE"}]}
{{</ highlight >}}

Reader tags on leaf values trigger an important optimization when those leaf values are dictionary-encoded. In
that case, the reader function gets applied to the dictionary itself in the column decoding thread, rather
than to each value in the record assembly thread. In effect, this computes the function once for each
_distinct_ value in the column, rather than on each value.

In some situations this can lead to an order of magnitude speedup. A good example is a machine learning
training set for an algorithm that uses the [hashing trick]({{< link hashing-trick >}}). Computing tens of
millions of hashes is very expensive, but computing just a few thousand distinct hashes is quite cheap.

Let's see how this plays out on the tutorial dataset. First let's define a generic hash function based on
clojure's built-in implementation of [murmur hash]({{< link murmur-hash >}}).

{{< highlight clojure >}}
(defn murmur-hash [x]
  (clojure.lang.Murmur3/hashUnencodedChars (str x)))
{{</ highlight >}}

Then we define two queries for the dictionary columns of the media object, one tagged, the other not.

{{< highlight clojure >}}
(def dict-ks
  [:format, :width, :copyright, :bitrate, :player, :height])

(defn map-vals [f m]
  (reduce-kv (fn [m k v] (assoc m k (f v))) {} m))

; Regular query for the fields in `dict-ks`
(def query
  (reduce #(assoc %1 %2 '_) {} dict-ks))

; Query in with each field in `dict-ks` is tagged with 'hash
(def tagged-query
  (map-vals #(d/tag 'hash %) query))
{{</ highlight >}}

Now let's compare the performance of hashing all the values for the fields in `dict-ks`, using a transducer on
the one hand, and reader tags on the other.

{{< highlight clojure >}}
; Hash all values with a transducer
(time (with-open [r (d/file-reader file)]
        (->> r
             (d/read {:sub-schema-in [:media]
                      :query query})
             (d/eduction (map (partial map-vals murmur-hash)))
             last)))
;= "Elapsed time: 97.027783 msecs"
;  {:format -992991939, :width 2996761, ... }

; Hash all distinct values in dictionaries
(time (with-open [r (d/file-reader file)]
        (->> r
             (d/read {:sub-schema-in [:media]
                      :query tagged-query
                      :readers {'hash murmur-hash}})
             last)))
;= "Elapsed time: 34.897017 msecs"
;  {:format -992991939, :width 2996761, ... }
{{</ highlight >}}

In this example, reader tags on dictionary columns yield a 3x speed improvement.

## Custom types {#custom-types}

Dendrite's [built-in types]({{< relref "#supported-types" >}}) should cover most use cases, but if not, it is
possible to define new types.

A custom needs to define the following:

- _type_: the name of the new type
- _base-type_: the name of an existing type that the new type can be serialized to
- _to base-type function_: a function to serialize to new type to the base type
- _from base-type function_: a function to deserialize the base type into the new type

Optionally, it is also possible to define a _coercion function_ to ensure that incoming values are indeed in
the _base-type_.

Let's see how this works on a simple example. We are going to define a new `edn` type that can contain
arbitrary EDN data. This could be useful if parts of your data must accommodate a completely free-form
datastructure.

{{< highlight clojure >}}
(require '[clojure.edn :as edn])

(def edn-type {:type 'edn
               :base-type 'string
               :to-base-type-fn pr-str
               :from-base-type-fn edn/read-string})

; A trivial schema with the new type
(def schema {:blob-id 'int :blob 'edn})

(def file "custom-type-example.den")

; Write two records to a file
(with-open [w (d/file-writer {:custom-types [edn-type]}
                             schema
                             file)]
  (.write w {:blob-id 1 :blob {:foo 2}})
  (.write w {:blob-id 2 :blob [#{:bar}]}))

; Read them
(with-open [r (d/file-reader {:custom-types [edn-type]} file)]
  (doall (d/read r)))
;= ({:blob-id 1, :blob {:foo 2}}
;   {:blob-id 2, :blob [#{:bar}]})
{{</ highlight >}}

Note that we have to pass the `:custom-types` option to both the reader and the writer. If the write does not
have that option it will throw an exception. The reader, however, degrades gracefully by reading the `edn`
values in its base-type (`string` in this example).

{{< highlight clojure >}}
(with-open [r (d/file-reader file)]
  (doall (d/read r)))
;= ({:blob-id 1, :blob "{:foo 2}"}
;   {:blob-id 2, :blob "[#{:bar}]"})
{{</ highlight >}}

Note how the `:blob` values are read as strings if the `:custom-types` option is not defined.

## Encoding and compression

### Available encodings

Under the hood, dendrite implements, for each primitive value type, several types of encodings that try to
accommodate very different types of data. The precise details of each encoding is explained in the
[file format]({{< relref "format.md" >}}). In the clojure API, encoding are denoted by a symbol. The table
below lists the available encodings for each primitive type.

| Type                      | Encodings                                                                         |
|---------------------------|-----------------------------------------------------------------------------------|
| `boolean`                 | `plain`, `dictionary`                                                             |
| `int`                     | `plain`, `dictionary`, `frequency`, `vlq`, `zigzag`, `packed-run-length`, `delta` |
| `long`                    | `plain`, `dictionary`, `frequency`, `vlq`, `zigzag`, `delta`                      |
| `float`                   | `plain`, `dictionary`, `frequency`                                                |
| `double`                  | `plain`, `dictionary`, `frequency`                                                |
| `byte-array`              | `plain`, `dictionary`, `frequency`, `delta-length`, `incremental`                 |
| `fixed-length-byte-array` | `plain`, `dictionary`, `frequency`                                                |

### Available compression

In many cases the encodings do a very good job of compressing the data. However, particularly for string
columns, is it often worth applying compression on top of the encoded data. At the moment, dendrite only
supports [deflate]({{< link deflate >}}) compression (denoted by the `deflate` symbol in the clojure
API). Earlier versions also supported [LZ4]({{< link lz4 >}}) but it never seemed to improve much over just
using the encodings without compression so it was dropped. However, it would be easy to re-introduce support
in future versions.

### Manual column settings {#manual-column-settings}

By default, dendrite uses a mix of straightforward heuristics (number of distinct values, max, min, etc.) and
greedy optimizations to pick a good [encoding]({{< relref "format.md" >}}) and [compression]({{< relref
"format.md">}}) for each column. This was a deliberate design decision to avoid forcing these decisions onto
the end-user. Indeed, manually specifying such settings for each column quickly becomes tedious as the number
of columns grows.

However, in some situations in can be advantageous to specify desired encodings up-front. For example, if
write speed is a concern, you may want to optimize the choice of encodings and compressions just once and
re-use those choices for all subsequent writes. Similarly, should the built-in heuristics produce a
sub-optimal design, you may want to override them for a specific column. As we will see, dendrite supports all
these use-cases.

Dendrite's column encoding & compression optimizer has three modes:

- __default__: all the columns that have `plain` encoding and `none` compression are optimized;
- __all__: all columns are optimized;
- __none__: no columns are optimized.

Furthermore, the `d/col` function can annotate the write schema to request specific encodings and
compressions. The `d/full-schema` function can retrieve the _fully-annotated_ schema from an existing
file. Let's see how this works on a simple example.

{{< highlight clojure >}}
(def tmp-file "/path/to/tmp-file.den")

(defn get-full-schema [file]
  (with-open [r (d/file-reader file)]
    (d/full-schema r)))

; Default behavior
(with-open [w (d/file-writer 'int tmp-file)]
  (.writeAll w (range 100)))
(get-full-schema tmp-file)
;= #col [int delta]
; Delta encoding with no compression

; Disable optimization
(with-open [w (d/file-writer {:optimize-columns? :none}
                             'int tmp-file)]
  (.writeAll w (range 100)))
(get-full-schema tmp-file)
;= int
; Plain int encoding with no compression

; Manually pick VLQ encoding
(with-open [w (d/file-writer (d/col 'int 'vlq 'deflate) tmp-file)]
  (.writeAll w (range 100)))
(get-full-schema tmp-file)
;= #col [int vlq deflate]
; VLQ encoding with deflate compression

; Manually pick VLQ encoding, but force optimization of all columns
(with-open [w (d/file-writer {:optimize-columns? :all}
                             (d/col 'int 'vlq) tmp-file)]
  (.writeAll w (range 100)))
;= #col [int delta]
; Delta encoding with no compression
{{</ highlight >}}

Note that object returned by `d/col` is printed using an EDN `#col` tag. Indeed, there is nothing special
about these _fully-annotated_ schemas as they can be manipulated and serialized just like the schemas
described [earlier]({{< relref "#schema-serialization">}}) in this tutorial.

### Compression thresholds

When determining whether or not to use compression for a given column, dendrite estimates a compression ratio
for each of the available compression algorithms (only [deflate]({{< link deflate >}}) at the moment) by
compressing one data page with each. If this compression ratio is greater than a certain threshold that
compression algorithm is kept for further consideration. Finally dendrite selects from all passing algorithms
the one that yielded the smallest page.

These thresholds are configurable through the `:compression-thresholds` writer option and allow the user finer
control over the file-size vs. read speed trade-off. Let's see how this works on a simple example.

{{< highlight clojure >}}
(def tmp-file "/path/to/tmp-file.den")

(defn first-col-stats [file]
  (with-open [r (d/file-reader file)]
    (select-keys (-> r d/stats :columns first)
                 [:compression :data-length])))

; Default: use deflate only if it achieves a 1.5 compression
; ratio or higher
(with-open [w (d/file-writer 'int tmp-file)]
  (.writeAll w (range 10000)))
(first-col-stats tmp-file)
;= {:compression deflate, :data-length 283}

; Only pick deflate compression if it achieves a compression ratio
; of 3 or higher
(with-open [w (d/file-writer {:compression-thresholds {'deflate 3}}
                             'int tmp-file)]
  (.writeAll w (range 10000)))
(first-col-stats tmp-file)
;= {:compression none, :data-length 716}
{{</ highlight >}}

The compressed length (283 bytes) is just 2.5 times smaller than the uncompressed length (716 bytes) so
deflate compression is selected in the former example but not in the latter.

## File layout customization

Dendrite tries to provide sensible defaults for all low-level settings. When defaults don't make sense, it
resorts to write-time heuristics to optimize for a good compactness/read-speed trade-off. In some cases
though, you may want to override the low-level settings. This sections discusses options to customize the
[file layout]({{< relref "format.md" >}}).

### Page lengths

Pages are the basic unit of parallelism and compression. As explained in the [implementation notes]({{< relref
"implementation.md" >}}), each page is lazily decoded (and possibly decompressed) in a separate thread. The
default page size is 256 KB. Since the first page of each column must be decoded before record assembly can
begin, excessively large pages increase the likelihood that a very large column will become a
bottleneck. However larger pages also allow for more efficient encoding and compression. Page size also
influences the heuristic for the maximum size of dictionary pages.

Let's see how this plays out on an example. We re-use the same tutorial file that was downloaded at the
beginning of the [file introspection]({{< relref "#file-introspection" >}}) section.

{{< highlight clojure >}}
; 'file' points to the provided tutorial, 'file2' to a tmp file
(def file "/path/to/tutorial.den")
(def file2 "/path/to/tmp-file.den")

; Fetch the schema from file
(def schema (with-open [r (d/file-reader file)]
              (d/schema r)))

; Copy file to file2 using a smaller page size
(with-open [r (d/file-reader file)
            w (d/file-writer {:data-page-length 1024} schema file2)]
  (doseq [o (d/read r)]
    (.write w o)))
{{</ highlight >}}

Note that `file2` is considerably larger than `file`.

{{< highlight clojure >}}
(require '[clojure.java.io :as io])

(.length (io/as-file file))
;= 6421801

(.length (io/as-file file2))
;= 19775457
{{</ highlight >}}

Let's have a closer look at page lengths and counts in both. We'll first define a `print-page-stats` function.

{{< highlight clojure >}}
(require '[clojure.pprint :as pp])

(defn print-page-stats [file]
  (with-open [r (d/file-reader file)]
    (->> (d/stats r)
         :columns
         (map #(select-keys % [:length :encoding :compression
                               :num-pages]))
         (sort-by :length)
         pp/print-table)))
{{</ highlight >}}

Let's use this function to analyze both files.

{{< highlight clojure >}}
(print-page-stats file)
; | :length |         :encoding | :compression | :num-pages |
; |---------+-------------------+--------------+------------|
; |    2137 |             delta |      deflate |          1 |
; |   12537 |        dictionary |         none |          2 |
; |   25050 |        dictionary |         none |          2 |
; |   37550 |        dictionary |         none |          2 |
; |   37551 |        dictionary |         none |          2 |
; |   37552 |        dictionary |         none |          2 |
; |   75238 |        dictionary |         none |          3 |
; |  106425 |        dictionary |         none |          2 |
; |  116722 |        dictionary |         none |          2 |
; |  187987 |        dictionary |         none |          3 |
; |  187988 |        dictionary |         none |          3 |
; |  209329 |         frequency |         none |          2 |
; |  212518 | packed-run-length |         none |          1 |
; |  344648 |        dictionary |         none |          3 |
; |  753994 |         frequency |      deflate |          3 |
; | 1009330 |       incremental |      deflate |          5 |
; | 3064652 |       incremental |      deflate |         13 |

(print-page-stats file2)
; | :length |         :encoding | :compression | :num-pages |
; |---------+-------------------+--------------+------------|
; |    4621 |             delta |      deflate |         79 |
; |   13790 |        dictionary |         none |         94 |
; |   26017 |        dictionary |         none |         77 |
; |   38613 |        dictionary |         none |         80 |
; |   38618 |        dictionary |         none |         80 |
; |   38618 |        dictionary |         none |         80 |
; |   81482 |        dictionary |         none |        259 |
; |  151937 | packed-run-length |         none |        149 |
; |  194842 |        dictionary |         none |        276 |
; |  194847 |        dictionary |         none |        276 |
; |  215053 | packed-run-length |         none |        212 |
; |  442669 |      delta-length |      deflate |        415 |
; | 1636137 |      delta-length |      deflate |       1636 |
; | 1821422 |       incremental |      deflate |       1713 |
; | 2309660 |       incremental |      deflate |       2193 |
; | 5545539 |       incremental |      deflate |       5126 |
; | 6980630 |       incremental |      deflate |       6467 |
{{</ highlight >}}

Note how the smaller page size increased the number of pages and changed the result of the heuristics for
encoding and compression. In this example, the very small 1KB pages reduced the number of dictionary encodings
and made the compression less efficient.

### Record-group length

Similarly to the page length, it is possible to tweak the record-group length. The record-group represents the
maximum amount of data that will be held in memory at write-time, and the size of the memory-mapping at
read-time. Due to JVM limitations, the maximum record-group length is 2 GB.

Let's quickly modify the record-group length of the tutorial file (reusing the `file`, `file2`, and `schema`
vars from the previous section.

{{< highlight clojure >}}
; Copy file to file2 using a smaller record-group-length
(with-open [r (d/file-reader file)
            w (d/file-writer {:record-group-length (* 1024 1024)}
                             schema file2)]
  (doseq [o (d/read r)]
    (.write w o)))
{{</ highlight >}}

Then let's inspect the file stats to see how this changed the file layout.

{{< highlight clojure >}}
(require '[clojure.pprint :as pp])

(defn print-rg-stats [file]
  (with-open [r (d/file-reader file)]
    (->> (d/stats r)
         :record-groups
         (map #(select-keys % [:num-records :length]))
         pp/print-table)))

(print-rg-stats file)
; | :num-records | :length |
; |--------------+---------|
; |       100000 | 6421208 |

(print-rg-stats file2)
; | :num-records | :length |
; |--------------+---------|
; |        14757 |  994736 |
; |        15436 | 1029789 |
; |        15726 | 1048090 |
; |        15684 | 1050273 |
; |        15767 | 1049096 |
; |        15745 | 1049766 |
; |         6885 |  471042 |
{{</ highlight >}}

In this example, lowering `:record-group-length` to 1 MB (down from 128 MB by default), created a copy of the
tutorial file with six record-groups instead of just one.
