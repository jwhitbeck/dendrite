(ns dendrite.estimation)

(set! *warn-on-reflection* true)

(defprotocol IEstimateCorrector
  (correct [_ estimate])
  (update! [_ observed estimated]))

(defrecord RatioEstimateCorrector [total-estimated total-observed]
  IEstimateCorrector
  (correct [_ estimate]
    (if (zero? @total-estimated)
      estimate
      (* estimate (/ @total-observed @total-estimated))))
  (update! [_ observed estimated]
    (swap! total-observed + observed)
    (swap! total-estimated + estimated)))

(defn ratio-estimator []
  (map->RatioEstimateCorrector
   {:total-estimated (atom 0)
    :total-observed (atom 0)}))

(defn next-threshold-check [idx value target]
  (if (zero? value)
    (inc idx)
    (let [next-idx (-> (/ target value) (* idx) (- idx) (/ 2) (+ idx) int)]
      (if (= next-idx idx)
        (inc next-idx)
        next-idx))))
