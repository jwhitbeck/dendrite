(ns dendrite.benchmarks.tpc-h
  "Benchmark on the TPC-H (http://www.tpc.org/tpch/) test data."
  (:require [abracad.avro :as avro]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [dendrite.benchmarks.utils :as utils]
            [dendrite.core :as d]
            [flatland.protobuf.core :refer :all])
  (:import [dendrite.benchmarks TpcH$LineItem]))

(set! *warn-on-reflection* true)

; The dbgen tool generates the TPC-H data. It outputs 7 csv files: customer.tbl, lineitem.tbl, nation.tbl,
; orders.tbl, partsupp.tbl, part.tbl, region.tbl, supplier.tbl. Each represents a table for the TPC-H
; benchmark. The top-level entity is the 'line-item' (see
; http://www.tpc.org/tpc_documents_current_versions/pdf/tpch2.17.1.pdf for all relations). For the dendrite
; benchmark, we join all the nested entities (customer, nation, order, part, region, supplier) onto the
; 'line-item'.

(def dbgen-delim #"\|")

(defn- csv-seq [r] (map #(string/split % dbgen-delim) (line-seq r)))

(defrecord Region [name comment])

(defn read-regions [region-file]
  (with-open [r (io/reader region-file)]
    (into {} (for [[key-str name comment] (csv-seq r)]
               [(Long/parseLong key-str) (Region. name comment)]))))

(defrecord Nation [name region comment])

(defn read-nations [regions nations-file]
  (with-open [r (io/reader nations-file)]
    (into {} (for [[key-str name region-key-str comment] (csv-seq r)
                   :let [region (get regions (Long/parseLong region-key-str))]]
               [(Long/parseLong key-str) (Nation. name region comment)]))))

(defrecord Supplier [name address nation phone account-balance comment])

(defn read-suppliers [nations suppliers-file]
  (with-open [r (io/reader suppliers-file)]
    (into {} (for [[key-str name address nation-key-str phone account-balance-str comment] (csv-seq r)
                   :let [nation (get nations (Long/parseLong nation-key-str))
                         account-balance (Float/parseFloat account-balance-str)]]
               [(Long/parseLong key-str) (Supplier. name address nation phone account-balance comment)]))))

(defrecord Customer [name address nation phone account-balance market-segment comment])

(defn read-customers [nations customers-file]
  (with-open [r (io/reader customers-file)]
    (into {} (for [[key-str name address nation-key-str phone account-balance-str market-segment comment]
                     (csv-seq r)
                   :let [nation (get nations (Long/parseLong nation-key-str))
                         account-balance (Float/parseFloat account-balance-str)]]
               [(Long/parseLong key-str)
                (Customer. name address nation phone account-balance market-segment comment)]))))

(defrecord Part [name manufacturer brand type size container retail-price comment])

(defn read-parts [parts-file]
  (with-open [r (io/reader parts-file)]
    (into {} (for [[key-str name manufacturer brand type size-str container retail-price-str comment]
                     (csv-seq r)
                   :let [size (Long/parseLong size-str)
                         retail-price (Float/parseFloat retail-price-str)]]
               [(Long/parseLong key-str)
                (Part. name manufacturer brand type size container retail-price comment)]))))

(defrecord Order [customer order-status total-price order-date order-priority clerk ship-priority comment])

(defn read-orders [customers orders-file]
  (with-open [r (io/reader orders-file)]
    (into {} (for [[key-str customer-key-str order-status total-price-str order-date order-priority clerk
                    ship-priority-str comment] (csv-seq r)
                    :let [customer (get customers (Long/parseLong customer-key-str))
                          total-price (Float/parseFloat total-price-str)
                          ship-priority (Long/parseLong ship-priority-str)]]
               [(Long/parseLong key-str)
                (Order. customer order-status total-price order-date order-priority clerk ship-priority
                        comment)]))))

(defrecord LineItem [order part supplier line-number quantity extended-price discount tax return-flag
                     line-status ship-date commit-date receipt-date ship-instruct ship-mode comment])

(defn convert-line-items-to-json [orders parts suppliers line-item-file json-file]
  (with-open [r (io/reader line-item-file)
              w (-> json-file utils/file-output-stream utils/gzip-output-stream io/writer)]
    (doseq [[order-key-str part-key-str supplier-key-str line-number-str quantity-str
             extended-price-str discount-str tax-str return-flag line-status ship-date commit-date
             receipt-date ship-instruct ship-mode comment]
              (csv-seq r)
            :let [order (get orders (Long/parseLong order-key-str))
                  part (get parts (Long/parseLong part-key-str))
                  supplier (get suppliers (Long/parseLong supplier-key-str))
                  line-number (Long/parseLong line-number-str)
                  quantity (Long/parseLong quantity-str)
                  extended-price (Float/parseFloat extended-price-str)
                  discount (Float/parseFloat discount-str)
                  tax (Float/parseFloat tax-str)
                  line-item (LineItem. order part supplier line-number quantity extended-price discount tax
                                       return-flag line-status ship-date commit-date receipt-date
                                       ship-instruct ship-mode comment)]]
      (.write w (str (json/generate-string line-item) "\n")))))

(defn build-tpch-dataset [dbgen-output-dir json-file]
  (let [regions (read-regions (io/file dbgen-output-dir "region.tbl"))
        nations (read-nations regions (io/file dbgen-output-dir "nation.tbl"))
        suppliers (read-suppliers nations (io/file dbgen-output-dir "supplier.tbl"))
        customers (read-customers nations (io/file dbgen-output-dir "customer.tbl"))
        parts (read-parts (io/file dbgen-output-dir "part.tbl"))
        orders (read-orders customers (io/file dbgen-output-dir "orders.tbl"))]
    (convert-line-items-to-json orders parts suppliers (io/file dbgen-output-dir "lineitem.tbl") json-file)))

(def dendrite-schema (-> "tpc_h_schema.edn" io/resource slurp d/read-schema-string))

(def TpcHLineItem (protodef TpcH$LineItem))

(defn proto-serialize ^bytes [user]
  (protobuf-dump (apply protobuf TpcHLineItem (apply concat user))))

(defn proto-deserialize [^bytes bs]
  (protobuf-load TpcHLineItem bs))

(def base-file-url "https://s3.amazonaws.com/dendrite.whitbeck.net/tpc_h.json.gz")

(def avro-region
  {:name "region"
   :type "record"
   :fields [{:name "name" :type "string"}
            {:name "comment" :type "string"}]})

(def avro-nation
  {:name "nation"
   :type "record"
   :fields [{:name "name" :type "string"}
            {:name "region" :type avro-region}
            {:name "comment" :type "string"}]})

(def avro-customer
  {:name "customer"
   :type "record"
   :fields [{:name "name" :type "string"}
            {:name "address" :type "string"}
            {:name "nation" :type avro-nation}
            {:name "phone" :type "string"}
            {:name "account-balance" :type "float"}
            {:name "market-segment"
             :type {:type "enum"
                    :name "market-segment"
                    :symbols ["MACHINERY" "HOUSEHOLD" "AUTOMOBILE" "BUILDING" "FURNITURE"]}}
            {:name "comment" :type "string"}]})

(def avro-order
  {:name "order"
   :type "record"
   :fields [{:name "customer" :type avro-customer}
            {:name "order-status"
             :type {:name "order-status"
                    :type "enum"
                    :symbols ["O" "F" "P"]}}
            {:name "total-price" :type "float"}
            {:name "order-date" :type "string"}
            {:name "order-priority" :type "string"}
            {:name "clerk" :type "string"}
            {:name "ship-priority" :type "int"}
            {:name "comment" :type "string"}]})

(def avro-part
  {:name "part"
   :type "record"
   :fields [{:name "name" :type "string"}
            {:name "manufacturer" :type "string"}
            {:name "brand" :type "string"}
            {:name "type" :type "string"}
            {:name "size" :type "int"}
            {:name "container" :type "string"}
            {:name "retail-price" :type "float"}
            {:name "comment" :type "string"}]})

(def avro-supplier
  {:name "supplier"
   :type "record"
   :fields [{:name "name" :type "string"}
            {:name "address" :type "string"}
            {:name "nation" :type "nation"}
            {:name "phone" :type "string"}
            {:name "account-balance" :type "float"}
            {:name "comment" :type "string"}]})

(def avro-line-item
  {:name "line-item"
   :type "record"
   :fields [{:name "order" :type avro-order}
            {:name "part" :type avro-part}
            {:name "supplier" :type avro-supplier}
            {:name "line-number" :type "int"}
            {:name "quantity" :type "int"}
            {:name "extended-price" :type "float"}
            {:name "discount" :type "float"}
            {:name "tax" :type "float"}
            {:name "return-flag" :type {:name "returnFlag"
                                        :type "enum"
                                        :symbols ["N" "R" "A"]}}
            {:name "line-status" :type {:name "lineStatus"
                                        :type "enum"
                                        :symbols ["O" "F"]}}
            {:name "ship-date" :type "string"}
            {:name "commit-date" :type "string"}
            {:name "receipt-date" :type "string"}
            {:name "ship-instruct" :type "string"}
            {:name "ship-mode" :type "string"}
            {:name "comment" :type "string"}]})

(def avro-schema
  (avro/parse-schema avro-line-item))

(def full-schema-benchmarks
  (let [n 600572]
    (concat (utils/json-benchmarks)
            (utils/smile-benchmarks n)
            (utils/edn-benchmarks)
            (utils/fressian-benchmarks n)
            (utils/nippy-benchmarks n)
            (utils/avro-benchmarks n avro-schema)
            (utils/protobuf-benchmarks n proto-serialize proto-deserialize)
            (utils/dendrite-benchmarks dendrite-schema))))

(def sub-schema-benchmarks
  {:create-fn #(utils/json-file->dendrite-file dendrite-schema %1 %2)
   :random-queries-fn utils/random-queries})
