# Encodings

## Base-types and derived-types

Internally, dendrite distinguishes between _base-types_ and _derived-types_:

- _base-types_ are directly serialized/deserialized to bytes
- _derived-types_ are mapped to _base-types_ (or another _derived-type_) and must first be converted to that
  _base-type_ before serialization and converted back from that _base-type_ upon deserialization.

The following _base-types_ are supported.

<table>
  <thead>
    <tr>
      <td>Base type</td>
      <td>Description</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>boolean</td>
      <td>a bit</td>
    </tr>
    <tr>
      <td>int</td>
      <td>a 32 bit integer</td>
    </tr>
    <tr>
      <td>long</td>
      <td>a 64 bit integer</td>
    </tr>
    <tr>
      <td>float</td>
      <td>a single precision IEEE floating point number (32 bit)</td>
    </tr>
    <tr>
      <td>double</td>
      <td>a double precision IEEE floating point number (64 bit)</td>
    </tr>
    <tr>
      <td>byte-array</td>
      <td>a raw byte-array of any length</td>
    </tr>
    <tr>
      <td>fixed-length-byte-array</td>
      <td>a raw byte-array (all byte-arrays in a fixed-length-byte-array column must have the same length)</td>
    </tr>
  </tbody>
</table>

Furthermore, the following _derived-types_ are always defined (though more _derived-types_ can be defined
through the API).

<table>
  <thead>
    <tr>
      <td>Derived type</td>
      <td>Base type</td>
      <td>Accepted input</td>
      <td>Deserialized object</td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>string</td>
      <td>byte-array</td>
      <td>any valid `x` argument to `(str x)`</td>
      <td>java.lang.String</td>
    </tr>
    <tr>
      <td>inst</td>
      <td>long</td>
      <td>java.util.Date</td>
      <td>java.util.Date</td>
    </tr>
    <tr>
      <td>uuid</td>
      <td>fixed-length-byte-array</td>
      <td>java.util.UUID</td>
      <td>java.util.UUID</td>
    </tr>
    <tr>
      <td>char</td>
      <td>int</td>
      <td>any valid `x` argument to `(char x)`</td>
      <td>java.lang.Character</td>
    </tr>
    <tr>
      <td>bigint</td>
      <td>byte-array</td>
      <td>any valid `x` argument to `(bigint x)`</td>
      <td>clojure.lang.BigInt</td>
    </tr>
    <tr>
      <td>bigdec</td>
      <td>byte-array</td>
      <td>any valid `x` argument to `(bigdec x)`</td>
      <td>java.math.BigDecimal</td>
    </tr>
    <tr>
      <td>bigdec</td>
      <td>byte-array</td>
      <td>any valid `x` argument to `(bigdec x)`</td>
      <td>java.math.BigDecimal</td>
    </tr>
    <tr>
      <td>ratio</td>
      <td>byte-array</td>
      <td>a clojure ratio or any valid argument `x` to `(bigint x)`</td>
      <td>clojure.lang.Ratio</td>
    </tr>
    <tr>
      <td>keyword</td>
      <td>string</td>
      <td>a regular or namespaced clojure keyword</td>
      <td>clojure.lang.Keyword</td>
    </tr>
    <tr>
      <td>symbol</td>
      <td>string</td>
      <td>a clojure symbol</td>
      <td>clojure.lang.Symbol</td>
    </tr>
  </tbody>
</table>


## Base-type encodings

### Boolean

### Int

### Long

### Float

### Double

### Byte array

### Fixed-length byte array

FIXME
