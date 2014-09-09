;; Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

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
    (let [next-idx (-> (/ target value) (* idx) (- idx) (/ 2) (+ idx) long)]
      (if (= next-idx idx)
        (inc next-idx)
        next-idx))))
