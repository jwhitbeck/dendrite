 ---
date: 2015-08-23T17:56:28-07:00
menu: main
showtoc: true
weight: 3
title: Benchmarks
---

# Benchmarks

## Overview

Performance is one of the main design goals of dendrite. From the very beginning, benchmarks have been a core
part of its development workflow. The results below highlight how dendrite's key performance metrics such as
file read time or file size change with the characteristics of the underlying data (e.g. number of distinct
values per column) and which subset of columns is queried.

Furthermore, solid performance shouldn't be limited to scenarios that play to a columnar format's strengths.
Full file reads, in which each record is fully deserialized, should be at least as fast as with a good
row-major format. The benchmarks below show that dendrite's _full-schema_ read speed compares favorably to
other common formats on three datasets with very different characteristics.

The usual benchmark disclaimers apply. The datasets below may not be representative of your own and your
mileage may vary.

### Methodology

Fair benchmarks are notoriously difficult to set up. Nothing too sophisticated went it the results below, but
in general, a few dummy runs were performed to warm the Hotspot JIT compiler and to ensure that the files were
in the OS disk cache. Note that the latter removes disk I/O as a factor. This puts dendrite at a slight
disadvantage since dendrite files are typically 30% smaller than gzipped row-based formats.

All results below were obtained on a [c3.2xlarge EC2]({{< link aws-instances >}}) instance running Ubuntu
14.04LTS.

#### Sub-schema benchmark

The _sub-schema_ benchmarks measure the file scan time of dendrite queries with varying number of columns. If
a dendrite file has 100 columns, then for each _i_ between 1 an 100, generate 100 random queries hitting _i_
columns and read the entire file with each query. This benchmark measure how file scan time is impacted by the
number of columns read and their size on disk.

#### Full-schema benchmark {#full-schema}

The _full-schema_ benchmarks compare the full scan time of files using different serialization formats. Given
that dendrite performs parallel record assembly, all file formats have a parallel variant (denoted by `-par`
suffix) that uses a [chunked pmap]({{< link chunked-pmap >}}) approach to deserialize batches of records in
separate threads. Plaintext records (e.g. EDN, JSON) use newlines to separate records. Binary formats use a
simple length-prefix encoding to write the serialized bytes of the records.

File size is always important for disk and/or network I/O considerations. Therefore all the row-based
serialization formats in the benchmarks are compressed using either GZIP (denoted `-gz`) or LZ4 (denoted
`-lz4`).

### Serialization formats

The table below lists the serialization formats that were considered for the _full-schema_ benchmarks. To make
the list, the format has to have a clojure or java API, support nested data structures and not require a
supporting service to read the file. Unfortunately, this last condition excludes [Parquet]({{< link parquet
>}}), the columnar file format most similar to dendrite, because Parquet files [cannot be read]({{< link
parquet-issue >}}) without a supporting Hadoop or Spark cluster.

<table>
    <thead>
        <tr>
            <td>Format</td>
            <td>Project</td>
            <td>Full EDN</td>
            <td>Columnar</td>
            <td>Binary</td>
            <td>Schema</td>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Dendrite</td>
            <td><a href="{{< link dendrite >}}">dendrite</a></td>
            <td>Yes</td>
            <td>Yes</td>
            <td>Yes</td>
            <td>Write only</td>
        </tr>
        <tr>
            <td>EDN</td>
            <td><a href="{{< link clojure-edn >}}">clojure.edn</a></td>
            <td>Yes</td>
            <td>No</td>
            <td>No</td>
            <td>No</td>
        </tr>
        <tr>
            <td>JSON</td>
            <td><a href="{{< link cheshire >}}">cheshire</a></td>
            <td>No</td>
            <td>No</td>
            <td>No</td>
            <td>No</td>
        </tr>
        <tr>
            <td>SMILE</td>
            <td><a href="{{< link cheshire >}}">cheshire</a></td>
            <td>No</td>
            <td>No</td>
            <td>Yes</td>
            <td>No</td>
        </tr>
        <tr>
            <td>Protocol buffers</td>
            <td><a href="{{< link clojure-protobuf >}}">clojure-protobuf</a></td>
            <td>No</td>
            <td>No</td>
            <td>Yes</td>
            <td>Read/write</td>
        </tr>
        <tr>
            <td>Avro</td>
            <td><a href="{{< link abracad >}}">abracad</a></td>
            <td>No</td>
            <td>No</td>
            <td>Yes</td>
            <td>Write only</td>
         </tr>
         <tr>
            <td>Fressian</td>
            <td><a href="{{< link fressian >}}">clojure/data.fressian</a></td>
            <td>Yes</td>
            <td>No</td>
            <td>Yes</td>
            <td>No</td>
        </tr>
         <tr>
            <td>Nippy</td>
            <td><a href="{{< link nippy >}}">nippy</a></td>
            <td>Yes</td>
            <td>No</td>
            <td>Yes</td>
            <td>No</td>
        </tr>
    </tbody>
</table>

While cheshire's JSON and SMILE library defaults to decoding record keys as strings, it can also read them as
keywords for more idiomatic clojure usage. Of course, that incurs the added cost of interning those strings so
both variants are included in the benchmarks. The keywordized variants are identified by `-kw`.

A format is said to have a _write-only_ schema if that schema is stored within the serialized file
itself. Therefore a reader does not require any knowledge of the schema prior to reading the file.

### Running the benchmarks

The benchmark code, along with instructions on how to run it, is available in the project repository under the
[benchmarks]({{< link benchmarks >}}) directory.

If you would like to propose a code change to improve the benchmarks or to add a new format to the mix, please
do so through a pull-request.

## Benchmark 1: TPC-H

The [TPC-H]({{< link tpch >}}) benchmark is widely used by database vendors. For our purposes, we re-use their
random SQL table generator to build a benchmark dataset of about 600,000 fully-denormalized records. These
records have moderate nesting and just under 50 leaf values. [Code]({{< link tpc-h-code >}}) for converting
TPC-H tables into JSON records is checked into the repo.

The TPC-H data plays to dendrite's strong points. In particular, the absence of repeated elements in the
schema means that record assembly can [ignore repetition levels]({{< relref "implementation.md" >}})
altogether. Furthermore, the low cardinality of many fields (e.g. region names) enables dendrite's
[dictionary encodings]({{< relref "format.md" >}}) on most string columns. Given how expensive UTF-8 string
decoding is in Java, this results in significant speed gains. For example, let's assume a _region_ column
consists of a million `"EUROPE"`, `"ASIA"`, or `"AMERICA"` strings. Row-major formats have to deserialize each
one of those million strings, whereas in a columnar format using a dictionary encoding, those three strings
are just encoded once into a dictionary array, and all values are encoded as indices into that array. This is
not only much faster, but also yields a more compact on-disk representation and reduces the pressure on
the garbage collector.

Note that some real-world datasets, in particular machine-learning training sets, contain mostly
low-cardinality columns and dendrite's performance on those will be comparable to its performance on the
_TPC-H_ benchmark presented here.

### Sample record

{{< highlight clojure >}}
{:receipt-date "1996-03-22",
 :return-flag "N",
 :supplier
 {:name "Supplier#000000785",
  :address "W VkHBpQyD3qjQjWGpWicOpmILFehmEdWy67kUGY",
  :nation
  {:name "RUSSIA",
   :region {:name "EUROPE",
            :comment "ly final courts cajole furiously final
                      excuse"},
   :comment " requests against the platelets use never according
             to the quickly regular pint"},
  :phone "32-297-653-2203",
  :account-balance 5364.99,
  :comment " packages boost carefully. express ideas along"},
 :ship-date "1996-03-13",
 :ship-mode "TRUCK",
 :part
 {:name "powder wheat midnight mint salmon",
  :manufacturer "Manufacturer#4",
  :brand "Brand#44",
  :type "SMALL ANODIZED STEEL",
  :size 42,
  :container "JUMBO CASE",
  :retail-price 1434.51,
  :comment "e carefully expre"},
 :ship-instruct "DELIVER IN PERSON",
 :commit-date "1996-02-12",
 :tax 0.02,
 :comment "egular courts above the",
 :extended-price 24386.67,
 :order
 {:customer
  {:name "Customer#000003691",
   :address "la3aZ2dd41O3lCSTPnbU",
   :nation
   {:name "FRANCE",
    :region
    {:name "EUROPE",
     :comment "uickly special accounts cajole carefully blithely
               close requests. carefully final asymptotes haggle
               furiousl"},
    :comment "efully alongside of the slyly final dependencies. "},
   :phone "20-758-985-1035",
   :account-balance 7967.22,
   :market-segment "MACHINERY",
   :comment "ublate furiously alongside of the pinto bean"},
  :order-status "O",
  :total-price 194029.55,
  :order-date "1996-01-02",
  :order-priority "5-LOW",
  :clerk "Clerk#000000951",
  :ship-priority 0,
  :comment "nstructions sleep furiously among "},
 :quantity 17,
 :discount 0.04,
 :line-number 1,
 :line-status "O"}
{{</ highlight >}}

### Sub-schema

{{< plot "tpc_h_query_time_vs_max_column_length.svg" >}}

The plot above is a scatterplot built from many different random queries against the dendrite serialization of
the TPC-H records. Each point plots the total read time for a given query against the size (on-disk bytes)
of the largest column in that query.

These results show that dendrite fulfills the promise of a columnar format by delivering order of magnitude
speedups when requesting small subsets of the whole data.

Interestingly, the query read time can be accurately predicted using a simple linear model of two variables
_number of columns_ and _max column size_. The former is easy to understand, as the cost of record assembly
scales with the number of columns. The latter is interesting since _max column size_ is a better predictor of
read time than the total size of columns in the query. This is explained by dendrite's
[parallel column decoding]({{< relref "implementation.md" >}}) implementation, in which the largest column can
become the bottleneck. This effect is much more pronounced in the other two benchmarks as each have a
mega-column.

### Full-schema

{{< plot "tpc_h_full_schema_read_time_vs_file_size.svg" >}}

This plot summarizes how each the [parallel variants]({{< relref "#full-schema" >}}) of the serialization
formats perform on disk-usage vs full-schema read time on the 600k TPC-H records. The closer to the top, the
smaller the file size, and the further to the right, the fastest. As this benchmark plays to dendrite's
strengths, it is in a class of its own here. The other benchmarks have closer results.

Both axis units are relative the performance of the gzipped EDN file (the slowest format in the benchmark).

The red vertical line indicates the time required for GZIP decompression alone (no record deserialization) of
the JSON file. This line represents the upper bound on read time for _any_ deserialization code based on
[java.util.zip.GZIPInputStream]({{< link java-gzip-inputstream >}}).

Each serialization family has at least two points, corresponding to GZIP and [LZ4]({{< link lz4-java >}})
compression. Slower formats (EDN, Avro, Fressian) can't make use of a faster decompression algorithm since
that is not the bottleneck, whereas faster formats (SMILE, Protobuf) can trade-off larger file sizes for
faster read-times. In this example, dendrite doesn't trade-off anything as it achieves both the smallest file
size and the fastest read times.

The two plots detail the read-time and file-size results for all serialization formats and variants.

{{< plot "tpc_h_full_schema_avg_read_time.svg" >}}

{{< plot "tpc_h_full_schema_file_size.svg" >}}

## Benchmark 2: Media content

The _media-content_ benchmark re-uses the schema from the [jvm-serializers]({{< link jvm-serializers >}})
benchmarks that measures the round-trip time of encoding/decoding data structures. The records have a shallow
2-level nesting, one level of repetition, and a mix of integer and string fields (17 in total). Approximately
400,000 records were generated using the excellent [mockaroo]({{< link mockaroo >}}) random data
generator. Compared to the _TPC-H_ benchmark above, only the "enum" fields such as `:format` or `:player` are
selected for dictionary encodings. [Code]({{< link media-content-code >}}) for generating the _media-content_
records from [mockaroo]({{< link mockaroo >}}) is checked into the repo.

### Sample record

{{< highlight clojure >}}
{:id 0,
 :media
 {:format "MKV",
  :width 3840,
  :copyright "Feedspan",
  :duration 158,
  :size 19636,
  :title "Phasellus sit amet erat.",
  :persons ["Joshua Barnes" "Terry Sanders"],
  :bitrate 400,
  :player "FLASH",
  :uri "https://fotki.com/vitae/ipsum/aliquam.js",
  :height 1080},
 :images
 [{:size "SMALL",
   :height 400,
   :width 240,
   :title "Etiam pretium iaculis justo.",
   :uri "https://businessweek.com/amet/nulla/quisque/arcu/libero/
         rutrum/ac.aspx"}
  {:size "LARGE",
   :height 60,
   :width 250,
   :title "Aliquam erat volutpat.",
   :uri "http://salon.com/iaculis/justo.jsp"}]}
{{</ highlight >}}

### Sub-schema

{{< plot "media_content_query_time_vs_max_column_length.svg" >}}

Just like the equivalent plot in the TPC-H benchmark, the scatterplot above plots
_read time_ vs _max column size_ for random queries for subsets of the _media-content_ schema.

As in the _TPC-H_ sub-schema benchmark, reading fewer columns enables order-of-magnitude speedups.

Unlike the _TPC-H_ data, the read-time is dominated by the presence of a mega-column: the `:url` inside the
repeated `:images` data structure. If present in the query, the total read-time effectively doubles.

### Full-schema

{{< plot "media_content_full_schema_read_time_vs_file_size.svg" >}}

The _full-schema_ results are similar to those of the _TPC-H_ benchmarks. While dendrite's lead is less
pronounced, it still offers the smallest file-size and fastest read speed.

The following two plots detail the read-time and file-size results for all serialization formats and variants.

{{< plot "media_content_full_schema_avg_read_time.svg" >}}

{{< plot "media_content_full_schema_file_size.svg" >}}

## Benchmark 3: User events

The _user-events_ benchmark was devised to stress dendrite's least efficient encodings and code paths. It
consists of about 30,000 large (>10KB) deeply nested records with multiply-nested repeated elements. Most of
the leaf nodes are strings that are not suitable for dictionary encoding. As in the _media-content_ benchmark,
these records were generated using the [mockaroo]({{< link mockaroo >}}) random data generator. The [code]({{<
link user-events-code >}}) for generating the _user-events_ records is checked into the repo.

### Sample record

The user event records are quite large (>10KB in plaintext) so the [sample record](/txt/sample_user_event.txt)
is not printed on this page.

### Sub-schema

{{< plot "user_events_query_time_vs_max_column_length.svg" >}}

Just like the equivalent plots in the previous benchmarks, the scatterplot above plots
_read time_ vs _max column size_ for random queries for subsets of the _media-content_ schema.

Similarly to the previous benchmarks, reading fewer columns enables order-of-magnitude speedups.

Like the _media-content_ benchmark, the read-time is dominated by the presence of a mega-column: the `:at`
timestamp field for the repeated `:events` structure (itself nested within two repeated structures). If
present in the query, the total read-time effectively triples.

### Full-schema

{{< plot "user_events_full_schema_read_time_vs_file_size.svg" >}}

The _full-schema_ results are similar to those of the _media-content_ benchmarks. While dendrite's lead is
less pronounced, it still offers the smallest file-size and fastest read speed. Interestingly, the schema-less
SMILE binary encoding does very well on this data and approaches the read performance of protocol buffers.

The following two plots detail the read-time and file-size results for all serialization formats and variants.

{{< plot "user_events_full_schema_avg_read_time.svg" >}}

{{< plot "user_events_full_schema_file_size.svg" >}}
