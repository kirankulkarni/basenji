(ns ^{:doc "Utility functions"
      :author "Kiran Kulkarni <kk@helpshift.com>"}
  basenji.utils
  (:import [org.hbase.async Bytes]))


(defprotocol IByteArray
  (to-byte-array [this]))


(extend-protocol IByteArray
  nil
  (to-byte-array [x] (byte-array 0))

  java.lang.String
  (to-byte-array [s] (Bytes/UTF8 s))

  java.lang.Short
  (to-byte-array [short-int] (Bytes/fromShort short-int))

  java.lang.Integer
  (to-byte-array [integer] (Bytes/fromInt integer))

  java.lang.Long
  (to-byte-array [long-int] (Bytes/fromLong long-int))

  java.nio.ByteBuffer
  (to-byte-array [byte-buffer] (.array byte-buffer))

  clojure.lang.Keyword
  (to-byte-array [k] (Bytes/UTF8 (name k)) )

  clojure.lang.IPersistentList
  (to-byte-array [l] (Bytes/UTF8 (binding [*print-dup* false]
                                   (pr-str l))))

  clojure.lang.APersistentVector
  (to-byte-array [v] (Bytes/UTF8 (binding [*print-dup* false]
                                   (pr-str v))))
  clojure.lang.APersistentMap
  (to-byte-array [m] (Bytes/UTF8 (binding [*print-dup* false]
                                   (pr-str m))))

  clojure.lang.APersistentSet
  (to-byte-array [s] (Bytes/UTF8 (binding [*print-dup* false]
                                   (pr-str s)))))


(defn to-byte-array-2d
  "Shamelessly modified to-array-2d taken from Clojure Core.
  Returns a (potentially-ragged) 2-dimensional Byte array of Objects
  containing the contents of coll, which can be any Collection of
  ByteArrays"
  ^"[[B"
  [^java.util.Collection coll]
  (let [ret (make-array (Class/forName "[B") (.size coll))]
    (loop [i 0 xs (seq coll)]
      (when xs
        (aset ret i ^"[B" (first xs))
        (recur (inc i) (next xs))))
    ret))


(def non-empty-string? (every-pred string? seq))
