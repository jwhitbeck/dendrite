(ns dendrite.estimation)

(set! *warn-on-reflection* true)

(defprotocol IEstimateCorrector
  (correct [_ estimate])
  (update! [_ observed estimated]))

(deftype RatioEstimateCorrector [^{:unsynchronized-mutable true :tag long} total-estimated
                                 ^{:unsynchronized-mutable true :tag long} total-observed]
  IEstimateCorrector
  (correct [_ estimate]
    (if (zero? total-estimated)
      estimate
      (* estimate (/ total-observed total-estimated))))
  (update! [_ observed estimated]
    (set! total-observed (long (+ observed total-observed)))
    (set! total-estimated (long (+ total-estimated estimated)))))

(defn ratio-estimator [] (RatioEstimateCorrector. 0 0))

(defn next-threshold-check
  ^long [^long idx value target]
  (if (zero? value)
    (inc idx)
    (let [next-idx (-> (/ target value) (* idx) (- idx) (/ 2) (+ idx) int)]
      (if (= next-idx idx)
        (inc next-idx)
        next-idx))))
