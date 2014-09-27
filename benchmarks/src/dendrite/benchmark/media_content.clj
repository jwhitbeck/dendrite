(ns dendrite.benchmark.media-content
  (:require [dendrite.benchmark.utils :as utils]))

(def mockaroo-columns
  [{:name "images" :type "JSON Array"}
   {:name "images.uri" :type "URL" :includeQueryString false}
   {:name "images.title" :type "Sentences" :min 1 :max 1}
   {:name "images.width" :type "Custom List" :values [240 468 88 300 728 234 250 125 180 200 320 120 160 336]}
   {:name "images.height" :type "Custom List" :values [240 60 50 31 600 90 150 100 250 280 125 200 400]}
   {:name "images.size" :type "Number" :min (* 1024) :max (* 100 1024) :decimals 0}
   {:name "media.uri" :type "URL" :includeQueryString false}
   {:name "media.title" :type "Sentences" :min 1 :max 1}
   {:name "media.width" :type "Custom List" :values [480 720 1280 1920 3840 7680]}
   {:name "media.height" :type "Custom List" :values [480 576 720 1080 2160 4320]}
   {:name "media.format" :type "Custom List" :values ["MP4" "MKV" "AVI" "MOV"]}
   {:name "media.duration" :type "Number" :min 1 :max 3600 :decimals 0}
   {:name "media.size" :type "Custom List" :values ["SMALL" "LARGE"]}
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
