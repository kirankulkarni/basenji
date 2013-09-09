# Basenji

A Client Library for HBase using [asynchbase](https://github.com/OpenTSDB/asynchbase)

There is another client library [clojure-hbase](https://github.com/davidsantiago/clojure-hbase). It uses `hbase` java library. I opted for asynchbase because it promises better [performance](http://www.tsunanet.net/~tsuna/asynchbase/benchmark/viz.html)

Currently Basenji is under development and is *not* production ready.
## Installation
TBD

## Getting Started

All the operations are available in `basenji.core` namespace. Just require every function in it to start with

```clojure
(require '[basenji.core :refer :all])
```
### Connection
You need to get a `hbase-client` and use `hbase-wrap` macro before you call any operation.
To get client you can use `get-hbase-client` It takes two parameters `quorum-spec` and `base-path`.

```clojure
(get-hbase-client "host1,host2,host3", "/hbase")
;;; Since /hbase is common base-path we can skip it
(get-hbase-client "host1,host2,host3")
;;; If you are doing development you can skip quorum, it will connect to localhost
(get-hbase-client)
```

For executing every operation you must use `hbase-wrap` macro

```clojure
(hbase-wrap hbase-client
            forms)
```

forms can be any clojure s-expressions.

### Basic Operations
It supports HBase operation `put`, `get`, `scan` and `delete`.

#### Put
Atomically inserts a row in HBase. Actually with this you can insert multiple qualifiers-values of single family. If your row has more than one column-family you need to call put for each one of them.

```clojure
(hbase-wrap hbase-client
            (put "test" "r1" "cf1" {:q1 "val1", :q2 "val2"}))
```

#### get-row
Get a whole row from HBase.

```clojure
(hbase-wrap hbase-client
            (get-row "test" "r2"))
```

Output is a sorted-map of row-qualifiers-timestmp and
```
{row-bytes
 {qual1-bytes {timestamp1 val1
               timestamp2 val2}
  qual2-bytes {timestamp1 val1
               timestamp2 val2}}
```

e.g.

```clojure
(hbase-wrap hbase-client
            (get-row "test" "r2"))

{#<byte[] [B@1e4877ec> {#<byte[] [B@27b9367c> {1378563922180 #<byte[] [B@70a81b43>}, #<byte[] [B@2c96057d> {1378563922180 #<byte[] [B@2d426869>}}}
```

You can also provide row-fn, qual-fn and val-fn which will be run on respective byte-arrays

```clojure
(hbase-wrap hbase-client
            (get-row "test" "r2"
                     :row-fn #(String. %)
                     :qual-fn #(String. %)
                     :val-fn #(String. %)))

{"r2" {"q1" {1378563922180 "val1"}, "q3" {1378563922180 "val3"}}}
```

#### Scan
HBase provides excellent range-queries. You can use `scan` API to perform a range-query. It take a `table-name` argument which will scan whole table. You can provide optional arguments to narrow it down:

start-row  - Start Row for the scan (Inclusive)
stop-row   - Stop Row for the scan (Exclusive)
family     - Column Family to scan
qualifiers - List of qualifiers to scan and return
min-timestamp - All values equal to and above this will be returned (Inclusive)
max-timestamp - All values below this will be returned (Exclusive)

It also accepts functions to process result like `get`:
row-fn - Function that will be applied to row-key bytes
qual-fn - Function that will be applied to qualifier bytes
val-fn - Function that will be applied to value bytes

A table scan can return many results and there is a probabily that your JVM memory will be overwhelmed with all these result. Hence this function is lazy and will fetch one row at a time from HBase. You can turn this behavior by passing keyword argument `:lazy? false` to the scan function

```clojure
(hbase-wrap hbase-client
            (scan "test"
                  :start-row "r1"
                  :end-row "r3"
                  :family "d"
                  :qualifiers [:q1 :q3]
                  :row-fn #(String. %)
                  :qual-fn #(String. %)
                  :val-fn #(String. %)))

({"r1" {"q1" {1378287632328 "q1"}}} {"r2" {"q1" {1378563922180 "val1"}, "q3" {1378563922180 "val3"}}})
```

#### Delete
Using this operation you can delete a row or a column-family of a row or a column in a row or a value of row

```clojure
(hbase-wrap hbase-client
            (delete "test" "r5" ))
```

### Other operations
Apart from these operations it also provides some more utilities:

`table-exists?` - It checks whether given table exists in HBase
`column-family-exists?` - It checks whether given column-family exists in given HBase table.
`flush` - Flushes to Hbase any buffered client-side operation
`get-stats` - Return client stats


## License

Copyright © 2013 Kiran Kulkarni

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
