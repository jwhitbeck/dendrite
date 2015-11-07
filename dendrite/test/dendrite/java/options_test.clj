;; Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;;
;; You must not remove this notice, or any other, from this software.

(ns dendrite.java.options-test
  (:require [clojure.test :refer :all]
            [dendrite.test-helpers :as helpers])
  (:import [dendrite.java Options]))

(set! *warn-on-reflection* true)

(deftest invalid-writer-options
  (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                    (Options/getWriterOptions opts))
       {:record-group-length (inc Integer/MAX_VALUE)}
       ":record-group-length expects a positive int but got '2147483648'"
       {:record-group-length "foo"}
       ":record-group-length expects a positive int but got 'foo'"
       {:record-group-length nil}
       ":record-group-length expects a positive int but got 'null'"
       {:record-group-length -1.5}
       ":record-group-length expects a positive int but got '-1.5'"
       {:data-page-length "foo"}
       ":data-page-length expects a positive int but got 'foo'"
       {:data-page-length nil}
       ":data-page-length expects a positive int but got 'null'"
       {:data-page-length -1.5}
       ":data-page-length expects a positive int but got '-1.5'"
       {:optimize-columns? "foo"}
       ":optimize-columns\\? expects one of :all, :none, or :default but got 'foo'"
       {:compression-thresholds :deflate}
       ":compression-thresholds expects a map."
       {:compression-thresholds {:deflate -0.2}}
       ":compression-thresholds expects its keys to be symbols but got ':deflate'"
       {:compression-thresholds {'deflate -0.2}}
       ":compression-thresholds expects its values to be positive"
       {:custom-types "foo"}
       ":custom-types expects a list but got 'foo'"
       {:invalid-input-handler "foo"}
       ":invalid-input-handler expects a function"
       {:invalid-option "foo"}
       ":invalid-option is not a supported writer option"
       {:ignore-extra-fields? nil}
       ":ignore-extra-fields\\? expects a boolean but got 'null'"
       {:ignore-extra-fields? "foo"}
       ":ignore-extra-fields\\? expects a boolean but got 'foo'"))

(deftest invalid-reader-options
  (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                    (Options/getReaderOptions opts))
       {:custom-types "foo"}
       ":custom-types expects a list but got 'foo'"
       {:invalid-option "foo"}
       ":invalid-option is not a supported reader option."))

(deftest invalid-read-options
  (are [opts msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                    (Options/getReadOptions opts))
       {:sub-schema-in :foo}
       ":sub-schema-in expects a seqable object but got ':foo'"
       {:sub-schema-in [:foo 'foo]}
       "sub-schema-in can only contain keywords, but got 'foo'"
       {:missing-fields-as-nil? nil}
       ":missing-fields-as-nil\\? expects a boolean but got 'null'"
       {:missing-fields-as-nil? "foo"}
       ":missing-fields-as-nil\\? expects a boolean but got 'foo'"
       {:readers "foo"}
       ":readers expects a map but got 'foo'"
       {:readers {:foo "foo"}}
       "reader key should be a symbol but got ':foo'"
       {:readers {'foo "foo"}}
       "reader value for tag 'foo' should be a function but got 'foo'"
       {:invalid-option "foo"}
       ":invalid-option is not a supported read option."))

(deftest invalid-custom-types
  (are [custom-types msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                            (Options/getCustomTypeDefinitions {:custom-types custom-types}))
       :foo
       ":custom-types expects a list but got ':foo'"
       [:bar]
       "custom type definition should be a map but got ':bar'")
  (are [custom-types msg] (thrown-with-msg? IllegalArgumentException (re-pattern msg)
                                            (helpers/throw-cause
                                             (Options/getCustomTypeDefinitions {:custom-types custom-types})))
       [{}]
       "Required field :type is missing"
       [{:type :foo}]
       "Custom type ':foo' is not a symbol"
       [{:type 'foo}]
       "Required field :base-type is missing"
       [{:type 'foo :base-type :int}]
       "Base type ':int' is not a symbol"
       [{:type 'foo :base-type 'int :invalid nil}]
       ":invalid is not a valid custom type definition key"
       [{:type 'foo :base-type 'int :coercion-fn 2}]
       ":coercion-fn expects a function"
       [{:type 'foo :base-type 'int :to-base-type-fn 2}]
       ":to-base-type-fn expects a function"
       [{:type 'foo :base-type 'int :from-base-type-fn 2}]
       ":from-base-type-fn expects a function"))
