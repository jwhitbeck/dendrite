(ns dendrite.estimation)

(defprotocol IEstimateCorrector
  (correct [_ estimate])
  (update! [_ observed estimated]))

(deftype RatioEstimateCorrector
    [^{:unsynchronized-mutable :int} total-estimated
     ^{:unsynchronized-mutable :int} total-observed]
  IEstimateCorrector
  (correct [_ estimate]
    (if (zero? total-estimated)
      estimate
      (* estimate (/ total-observed total-estimated))))
  (update! [_ observed estimated]
    (set! total-observed (+ total-observed observed))
    (set! total-estimated (+ total-estimated estimated))))

(defn ratio-estimator [] (RatioEstimateCorrector. 0 0))

(defn next-threshold-check [idx value target]
  (if (zero? value)
    (inc idx)
    (let [next-idx (-> (/ target value) (* idx) (- idx) (/ 2) (+ idx) int)]
      (if (= next-idx idx)
        (inc next-idx)
        next-idx))))
