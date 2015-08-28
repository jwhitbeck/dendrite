(ns dendrite.benchmarks.media-content
  (:require [abracad.avro :as avro]
            [clojure.java.io :as io]
            [dendrite.core :as d]
            [dendrite.benchmarks.utils :as utils]
            [flatland.protobuf.core :refer :all])
  (:import [dendrite.benchmarks V22LiteMedia$MediaContent]))

(def mockaroo-columns
  [{:name "images" :type "JSON Array"}
   {:name "images.uri" :type "URL" :includeQueryString false}
   {:name "images.title" :type "Sentences" :min 1 :max 1}
   {:name "images.width" :type "Custom List" :values [240 468 88 300 728 234 250 125 180 200 320 120 160 336]}
   {:name "images.height" :type "Custom List" :values [240 60 50 31 600 90 150 100 250 280 125 200 400]}
   {:name "images.size" :type "Custom List" :values ["SMALL" "LARGE"]}
   {:name "media.uri" :type "URL" :includeQueryString false}
   {:name "media.title" :type "Sentences" :min 1 :max 1}
   {:name "media.width" :type "Custom List" :values [480 720 1280 1920 3840 7680]}
   {:name "media.height" :type "Custom List" :values [480 576 720 1080 2160 4320]}
   {:name "media.format" :type "Custom List" :values ["MP4" "MKV" "AVI" "MOV"]}
   {:name "media.duration" :type "Number" :min 1 :max 3600 :decimals 0}
   {:name "media.size" :type "Number" :min (* 1024) :max (* 100 1024) :decimals 0}
   {:name "media.bitrate" :type "Custom List" :values [400 700 1500 2500 4000]}
   {:name "media.persons" :type "JSON Array" :maxItems 3}
   {:name "media.persons.item" :type "Full Name" :values ["FLASH" "JAVA"]}
   {:name "media.player" :type "Custom List"}
   {:name "media.copyright" :type "Company Name"}])

(defn fix-media [obj]
  (-> obj
      (update-in [:media :width] #(Integer/parseInt %))
      (update-in [:media :height] #(Integer/parseInt %))
      (update-in [:media :bitrate] #(Integer/parseInt %))))

(defn fix-image [obj]
  (-> obj
      (update-in [:image] (fn [images]
                            (->> images
                                 (map (fn [img]
                                        (-> img
                                            (update-in [:width] #(Integer/parseInt %))
                                            (update-in [:height] #(Integer/parseInt %))))))))))

(defn fix-persons [obj]
  (update-in obj [:media :persons] (partial mapv :item)))

(defn fix-json-file [input-json-filename output-json-filename]
  (utils/fix-json-file (comp fix-persons fix-media fix-image) input-json-filename output-json-filename))

(def avro-schema
  (avro/parse-schema
   {:name "media-content"
    :type "record"
    :fields [{:name "id" :type "int"}
             {:name "media"
              :type {:name "media-record"
                     :type "record"
                     :fields [{:name "format" :type "string"}
                              {:name "width" :type "int"}
                              {:name "height" :type "int"}
                              {:name "copyright" :type "string"}
                              {:name "duration" :type "int"}
                              {:name "size" :type "int"}
                              {:name "title" :type "string"}
                              {:name "persons" :type {:name "persons-list"
                                                      :type "array"
                                                      :items {:type "string"}}}
                              {:name "bitrate" :type "int"}
                              {:name "player" :type {:name "playerEnum"
                                                     :type "enum"
                                                     :symbols ["JAVA" "FLASH"]}}
                              {:name "uri" :type "string"}]}}
             {:name "images"
              :type {:name "images-list"
                     :type "array"
                     :items {:name "image"
                             :type "record"
                             :fields [{:name "size" :type {:name "imageSize"
                                                           :type "enum"
                                                           :symbols ["SMALL" "LARGE"]}}
                                      {:name "height" :type "int"}
                                      {:name "width" :type "int"}
                                      {:name "title" :type "string"}
                                      {:name "uri" :type "string"}]}}}]}))

(def MediaContent (protodef V22LiteMedia$MediaContent))

(defn proto-serialize ^bytes [media-content]
  (let [images (:images media-content)
        fixed-media-content (-> media-content
                                (assoc :image images)
                                (dissoc :images))]
    (protobuf-dump (apply protobuf MediaContent (apply concat fixed-media-content)))))

(defn proto-deserialize [^bytes bs]
  (protobuf-load MediaContent bs))

(def base-file-url "https://s3.amazonaws.com/dendrite.whitbeck.net/media_content.json.gz")

(def dendrite-schema (-> "media_content_schema.edn" io/resource slurp d/read-schema-string))

(def full-schema-benchmarks
  (let [n 401000]
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
