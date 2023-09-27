### IVM Benchmarks

* DuckDB 0.6.2, TPCH scale factor 2



#### DuckDB without IVM

Query 2 execution time: 41s (warm cache)

- CREATE VIEW LINEITEM_TEST AS query2;
- SELECT * FROM LINEITEM_TEST -- 40s
- select count(*) from lineitem where l_linenumber < 2; -- removes 3 000 000 rows
- delete from lineitem where l_linenumber < 2; -- 10.65s
- SELECT * FROM LINEITEM_TEST -- 22s

#### DuckDB with IVM

