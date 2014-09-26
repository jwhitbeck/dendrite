(ns dendrite.benchmark.media-content
  (:require [dendrite.benchmark.utils :as utils]))

(def mockaroo-columns
  [{:name "image" :type "JSON Array"}
   {:name "image.uri" :type "URL" :includeQueryString false}
   {:name "image.title" :type "Sentences" :min 1 :max 1}
   {:name "image.width" :type "Custom List" :values [240 468 88 300 728 234 250 125 180 200 320 120 160 336]}
   {:name "image.height" :type "Custom List" :values [240 60 50 31 600 90 150 100 250 280 125 200 400]}
   {:name "image.size" :type "Number" :min (* 1024) :max (* 100 1024) :decimals 0}
   {:name "media.uri" :type "URL" :includeQueryString false}
   {:name "media.title" :type "Sentences" :min 1 :max 1}
   {:name "media.width" :type "Custom List" :values [480 720 1280 1920 3840 7680]}
   {:name "media.height" :type "Custom List" :values [480 576 720 1080 2160 4320]}
   {:name "media.format" :type "Custom List" :values ["MP4" "MKV" "AVI" "MOV"]}
   {:name "media.duration" :type "Number" :min 1 :max 3600 :decimals 0}
   {:name "media.size" :type "Number" :min (* 1024 1024) :max (* 1024 1024 1024) :decimals 0}
   {:name "media.bitrate" :type "Custom List" :values [400 700 1500 2500 4000]}
   {:name "media.person" :type "JSON Array" :maxItems 3}
   {:name "media.person.items" :type "Full Name"}
   {:name "player" :type "Number" :min 0 :max 10 :decimals 0}
   {:name "copyright" :type "Company Name"}])

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

(defn fix-json-file [input-json-filename output-json-filename]
  (utils/fix-json-file (comp fix-media fix-image) input-json-filename output-json-filename))
