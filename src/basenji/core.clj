(ns ^{:doc "Basenji Core"
      :author "Kiran Kulkarni <kk.questworld@gmail.com>"}
  basenji.core
  (:import [org.hbase.async HBaseClient ClientStats
            TableNotFoundException
            NoSuchColumnFamilyException]))


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
