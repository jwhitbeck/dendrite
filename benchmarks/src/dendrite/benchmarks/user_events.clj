(ns dendrite.benchmarks.user-events
  (:require [abracad.avro :as avro]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [dendrite.core :as d]
            [dendrite.benchmarks.utils :as utils]
            [flatland.protobuf.core :refer :all])
  (:import [dendrite.benchmarks UserEvents$User]
           [java.text SimpleDateFormat]
           [java.util Date]))

(set! *warn-on-reflection* true)

(def mockaroo-columns
  [{:name "id" :type "Sequence"}
   {:name "guid" :type "GUID"}
   {:name "username" :type "Username"}
   {:name "language" :type "Language"}
   {:name "is-active" :type "Boolean"}
   {:name "balance" :type "Money" :min 0 :max 1000}
   {:name "personal.picture" :type "URL" :includeQueryString false :percentBlank 80}
   {:name "personal.yob" :type "Date" :min "01/01/1950" :max "01/01/2010" :format "%Y"}
   {:name "personal.eye-color" :type "Custom List" :values ["blue" "brown" "green"] :percentBlank 50}
   {:name "personal.first-name" :type "First Name"}
   {:name "personal.last-name" :type "Last Name"}
   {:name "personal.company" :type "Company Name" :percentBlank 50}
   {:name "personal.gender" :type "Gender" :percentBlank 5}
   {:name "personal.email" :type "Email Address"}
   {:name "personal.address.street" :type "Street Address"}
   {:name "personal.address.city" :type "City"}
   {:name "personal.address.state" :type "State (abbrev)"}
   {:name "personal.address.zip" :type "Zip"}
   {:name "personal.address.country" :type "Country"}
   {:name "personal.phone" :type "Phone" :percentBlank 80}
   {:name "tagline" :type "Sentences" :min 1 :max 1}
   {:name "about" :type "Paragraphs" :percentBlank 20}
   {:name "registered-at" :type "Date" :min "01/01/2012"}
   {:name "tags" :type "JSON Array" :maxItems 3}
   {:name "tags.name" :type "Custom List" :values ["foo" "bar" "baz" "foobar"]}
   {:name "devices" :type "JSON Array" :maxItems 3}
   {:name "devices.family" :type "Custom List" :values ["ios" "android" "windows"]}
   {:name "devices.mac" :type "MAC Address"}
   {:name "devices.connections" :type "JSON Array" :maxItems 20}
   {:name "devices.connections.at" :type "Date" :min "01/01/2012"}
   {:name "devices.connections.ip" :type "IP Address V4"}
   {:name "devices.connections.referrer" :type "Custom List"
    :values ["Google" "Facebook" "Twitter" "Reddit" "Blog" "Pinterest"]}
   {:name "devices.connections.geo.lat" :type "Latitude"}
   {:name "devices.connections.geo.long" :type "Longitude"}
   {:name "devices.connections.events" :type "JSON Array"}
   {:name "devices.connections.events.at" :type "Date" :min "01/01/2012"}
   {:name "devices.connections.events.type" :type "Custom List"
    :values ["Listing" "Purchase" "Search" "Profile update"]}
   {:name "devices.connections.events.amount" :type "Money" :min 0 :max 100 :percentBlank 40}])

(defn- fix-lat-long [obj]
  (update-in
   obj
   [:devices]
   (fn [devices]
     (->> devices
          (map (fn [device]
                 (update-in device [:connections]
                            (fn [connections]
                              (->> connections
                                   (map (fn [connection]
                                          (-> connection
                                              (update-in [:geo :long] #(Float/parseFloat %))
                                              (update-in [:geo :lat] #(Float/parseFloat %))))))))))))))

(defn- fix-yob [obj]
  (update-in obj [:personal :yob] #(Long/parseLong %)))

(defn- fix-device [device]
  (-> device
      (update-in [:connections] (partial map #(dissoc % :type :amount)))
      (select-keys [:connections :mac :family])))

(defn- fix-connections [obj]
  (update-in obj [:devices] (partial map fix-device)))

(defn fix-json-file [input-json-filename output-json-filename]
  (utils/fix-json-file (comp fix-connections fix-yob fix-lat-long) input-json-filename output-json-filename))

(defn json-file->dendrite-file [json-filename dendrite-filename]
  (utils/json-file->dendrite-file "user_events_schema.edn" json-filename dendrite-filename))

(def avro-geo-type
  {:name "geo"
   :type "record"
   :fields [{:name "long" :type "float"}
            {:name "lat" :type "float"}]})

(def avro-event-type
  {:name "event"
   :type "record"
   :fields [{:name "amount" :type ["string" "null"]}
            {:name "type" :type "string"}
            {:name "at" :type "string"}]})

(def avro-connection-type
  {:name "connection"
   :type "record"
   :fields [{:name "events" :type {:name "event-list" :type "array" :items avro-event-type}}
            {:name "geo" :type avro-geo-type}
            {:name "referrer" :type "string"}
            {:name "ip" :type "string"}
            {:name "at" :type "string"}]})

(def avro-device-type
  {:name "device"
   :type "record"
   :fields [{:name "connections" :type {:name "connection-list" :type "array" :items avro-connection-type}}
            {:name "mac" :type "string"}
            {:name "family" :type "string"}]})

(def avro-tag-type
  {:name "tag"
   :type "record"
   :fields [{:name "name" :type ["string" "null"]}]})

(def avro-address-type
  {:name "address"
   :type "record"
   :fields [{:name "country" :type "string"}
            {:name "zip" :type "string"}
            {:name "state" :type "string"}
            {:name "city" :type "string"}
            {:name "street" :type "string"}]})

(def avro-personal-type
  {:name "personal"
   :type "record"
   :fields [{:name "company" :type ["string" "null"]}
            {:name "gender" :type ["string" "null"]}
            {:name "address" :type avro-address-type}
            {:name "email" :type "string"}
            {:name "first-name" :type "string"}
            {:name "last-name" :type "string"}
            {:name "phone" :type ["string" "null"]}
            {:name "yob" :type "int"}
            {:name "picture" :type ["string" "null"]}
            {:name "eye-color" :type ["string" "null"]}]})

(def avro-schema
  (avro/parse-schema
   {:name "user"
    :type "record"
    :fields [{:name "id" :type "int"}
             {:name "guid" :type "string"}
             {:name "about" :type ["string" "null"]}
             {:name "devices" :type {:name "device-list" :type "array" :items avro-device-type}}
             {:name "registered-at" :type "string"}
             {:name "tags" :type {:name "tag-list" :type "array" :items avro-tag-type}}
             {:name "tagline" :type "string"}
             {:name "personal" :type avro-personal-type}
             {:name "username" :type "string"}
             {:name "is-active" :type "boolean"}
             {:name "balance" :type "string"}
             {:name "language" :type "string"}]}))


(def User (protodef UserEvents$User))

(defn proto-serialize ^bytes [user]
  (protobuf-dump (apply protobuf User (apply concat user))))

(defn proto-deserialize [^bytes bs]
  (protobuf-load User bs))

(def base-file-url "https://s3.amazonaws.com/files.dendrite.tech/user_events.json.gz")

(def dendrite-schema (-> "user_events_schema.edn" io/resource slurp d/read-schema-string))

(def full-schema-benchmarks
  (let [n 31000]
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
