(ns dendrite.benchmark.build-benchmark-data
  (:require [cheshire.core :as json]
            [dendrite :as d]
            [dendrite.benchmark.utils :as utils]
            [org.httpkit.client :as http]
            [ring.util.codec :as codec]))

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
   {:name "devices.connections.events.amount" :type "Money" :min 0 :max 100 :percentBlank 40}
   ])

(defn rand-samples [n mockaroo-columns mockaroo-api-key]
  (-> (format "http://www.mockaroo.com/api/generate.json?count=%d&key=%s&columns=%s"
              n mockaroo-api-key (-> mockaroo-columns json/generate-string codec/url-encode))
      http/get
      deref
      :body
      codec/url-decode
      (json/parse-string keyword)))

(defn generate-samples-file [filename n mockaroo-columns mockaroo-api-key]
  (let [batch-size 100
        samples (->> (repeatedly #(rand-samples batch-size mockaroo-columns mockaroo-api-key))
                     (apply concat)
                     (map #(assoc %2 :id %1) (range))
                     (take n))]
    (with-open [f (utils/gzip-writer filename)]
      (doseq [sample samples]
        (when (zero? (mod (:id sample) 100))
          (println (str "Processing sample " (:id sample))))
        (.write f (str (json/generate-string sample) "\n"))))))

(defn- fix-lat-long [obj]
  (update-in obj [:devices]
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
  (update-in obj [:personal :yob] #(Integer/parseInt %)))

(defn json-file->dendrite-file [json-filename dendrite-filename]
  (let [schema (-> "resources/mockaroo_schema.edn" slurp d/read-schema-string)]
    (with-open [r (utils/gzip-reader json-filename)
                w (d/file-writer dendrite-filename schema :target-record-group-length (* 10 1024 1024))]
      (d/write! w (->> r line-seq (map #(-> % (json/parse-string keyword) fix-yob fix-lat-long)))))))
