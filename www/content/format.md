---
date: 2015-08-23T19:30:50-07:00
showtoc: true
menu: main
weight: 4
title: File format
---

# File format

## Overview

Dendrite's file format is very similar to [Parquet's]({{< link parquet >}}) file format.

<figure>
  <img src="/img/file-format.svg">
</figure>


TODO: add glossary and description. Discuss units of parallelization.

## Primitives

### Varints

- length variants
- zigzag (signed)

### Strings

UTF-8 byte arrays. VarSint + bytes

## Encodings

## Metadata

### Record-group

### Column-chunk

### Schema

#### Column

#### Collection

#### Record

Field

### Custom Types

### Custom metadata

## Record Group

## Column Chunk

Dictionary vs regular columns

## Page

### Data

### Dictionary
