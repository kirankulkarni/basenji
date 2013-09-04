(ns ^{:doc "Basenji Core"
      :author "Kiran Kulkarni <kk.questworld@gmail.com>"}
  basenji.core
  (:require [basenji.utils :as bu])
  (:import [org.hbase.async HBaseClient ClientStats
                            TableNotFoundException
                            NoSuchColumnFamilyException
                            PutRequest]))


(def ^:dynamic *hbase-client* nil)
(def ^:dynamic *timeout* (* 30 1000))

(defn get-hbase-client
  ([] (get-hbase-client "localhost" "/hbase"))
  ([quorum_spec] (get-hbase-client quorum_spec "/hbase"))
  ([quorum_spec base_path]
     (HBaseClient. quorum_spec base_path)))


(defmacro hbase-wrap
  [hbase-client & forms]
  `(binding [*hbase-client* ~hbase-client]
     ~@forms))


(defn table-exists?
  "Predicate function to check whether given table exists or not"
  [^String table-name]
  (try (.. *hbase-client*
           (ensureTableExists table-name)
           (join *timeout*))
       true
       (catch TableNotFoundException _
         false)))


(defn column-family-exists?
  "Predicat efunction to check whether given table exists and
   given column-family exists for that table or not"
  [^String table-name ^String family-name]
  (let [table-name-bytes (.getBytes table-name)
        family-name-bytes (.getBytes family-name)]
    (try (.. *hbase-client*
             (ensureTableFamilyExists table-name-bytes
                                      family-name-bytes)
             (join *timeout*))
         true
         (catch TableNotFoundException _
           false)
         (catch NoSuchColumnFamilyException _
           false))))


(defn get-stats
  "Returns client stats"
  []
  (let [client-stats ^ClientStats (.stats *hbase-client*)]
    {:connections (.connectionsCreated client-stats)
     :gets (.gets client-stats)
     :puts (.puts client-stats)
     :scans (.scans client-stats)
     :deletes (.deletes client-stats)
     :flushes (.flushes client-stats)
     :rpcs (.numBatchedRpcSent client-stats)
     :meta-lookups (.uncontendedMetaLookups client-stats)}))


(defn coerce-qualifier-value-map
  [qual-val-m]
  (let [[qualifiers values] (reduce (fn [[qualifiers values] [qual val]]
                                      (let [qual-byte-array (bu/to-byte-array qual)
                                            val-byte-array (bu/to-byte-array val)]
                                        [(conj qualifiers qual-byte-array)
                                         (conj values val-byte-array)]))
                                    [[] []]
                                    qual-val-m)]
    [(bu/to-byte-array-2d qualifiers)
     (bu/to-byte-array-2d values)]))


(defn- ^PutRequest construct-putrequest
  [table-name row-key column-family-name qualifiers values timestamp]
  (if (number? timestamp)
    (if (instance? (Class/forName "[[B") qualifiers)
      (PutRequest. ^"[B" table-name
                   ^"[B" row-key
                   ^"[B" column-family-name
                   ^"[[B" qualifiers
                   ^"[[B" values
                   (long timestamp))
      (PutRequest. ^"[B" table-name
                   ^"[B" row-key
                   ^"[B" column-family-name
                   ^"[B" qualifiers
                   ^"[B" values
                   (long timestamp)))
    (if (instance? (Class/forName "[[B") qualifiers)
      (PutRequest. ^"[B" table-name
                   ^"[B" row-key
                   ^"[B" column-family-name
                   ^"[[B" qualifiers
                   ^"[[B" values)
      (PutRequest. ^"[B" table-name
                   ^"[B" row-key
                   ^"[B" column-family-name
                   ^"[B" qualifiers
                   ^"[B" values))))


(defn insert
  "Atomically Inserts record in HBase."
  [table-name row-key column-family-name qualifiers-values-map & {timestamp :timestamp}]
  (let [table (bu/to-byte-array table-name)
        row (bu/to-byte-array row-key)
        cf (bu/to-byte-array column-family-name)
        [quals vals] (coerce-qualifier-value-map qualifiers-values-map)]
    (.. *hbase-client*
        (atomicCreate (construct-putrequest table
                                            row
                                            cf
                                            quals
                                            vals
                                            timestamp))
        (join *timeout*))))
