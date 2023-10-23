# ivm-extension

Incrementally maintain view `result` using query `PRAGMA ivm_upsert('catalog', 'schema', 'result')`;

Read more [here](https://github.com/cwida/ivm-extension/blob/ivm-optimizer-rule/VLDB%20Summer%20School%202023%20Poster.pdf)

## Usage
Given a base table `my_table` and a view `my_view` on top of `my_table`, this extension incrementally computes the changes to the view `my_result` when the underlying table `my_table` changes. 

This extension assumes that changes to base table `my_table` are present in delta table `delta_my_table`. This table additionally has a multiplicity column `_duckdb_ivm_multiplicity` of type `BOOL`. `duckdb_ivm_multiplicity=true` means insertions and `_duckdb_ivm_multiplicity` means deletion. 
Updates to a row in base table `my_table` are modelled as deletion + insertion.

Here is an example. First create the base table and the view:
```SQL
CREATE TABLE sales (order_id INT PRIMARY KEY, product_name VARCHAR(1), amount INT, date_ordered DATE);
```
Insert sample data:
```SQL
INSERT INTO sales VALUES (1, 'a', 100, '2023-01-10'), (2, 'b', 50, '2023-01-12'), (3, 'a', 75, '2023-01-15'), (4, 'c', 60, '2023-01-18'), (5, 'b', 30, '2023-01-20'), (6, 'b', 35, '2023-01-21');
```
Now create a materialized view:
```SQL
CREATE MATERIALIZED VIEW product_sales AS SELECT product_name, SUM(amount) AS total_amount, COUNT(*) AS total_orders FROM sales WHERE product_name = 'a' OR product_name = 'b' GROUP BY product_name;
CREATE MATERIALIZED VIEW product_sales AS SELECT * FROM sales WHERE product_name = 'a';
SELECT * FROM product_sales; -- to check the view content
```
Now we assume that changes are to be stored in a delta table, in our case `delta_sales`.
Insertion example (with an additional boolean for insertions/deletions, assuming only insertions):
```SQL
INSERT INTO delta_sales VALUES (6, 'a', 90, '2023-01-21', true), (7, 'b', 10, '2023-01-25', true), (8, 'a', 20, '2023-01-26', true), (9, 'c', 45, '2023-01-28', true);
```
Test with deletions:
```SQL
INSERT INTO delta_sales VALUES (1, 'a', 100, '2023-01-10', false), (2, 'b', 50, '2023-01-12', false), (7, 'a', 20, '2023-01-26', true), (8, 'c', 45, '2023-01-28', true);
```

### Incrementally maintaining view *result*
Run the extension as
```SQL
PRAGMA ivm_upsert('test_sales', 'main', 'product_sales');
```
The output of the above will be the table `delta_product_sales`, which will contain incremental processing of the changes to the base table of view `product_sales`. 

### Extent of SQL Support
* Only SELECT, FILTER, GROUP BY, PROJECTION
* Aggregations supported: SUM, COUNT
* Joins, nested-subqueries and other SQL clauses like HAVING **not supported**.

### Known issues
IVM on queries in which the base table returns no data because of a `WHERE` clause, **will fail**. So, while using `WHERE`, always ensure that the base table returns a non-empty result. More details in [this issue](https://github.com/cwida/ivm-extension/issues/10).

## Building the Extension
* Download the files in the repo into folder `duckdb_project_root/extension/ivm`
* Search for `JSON_EXTENSION` and make similar changes for `IVM_EXTENSION`
* Build the `duckdb` binary using `make`. The IVM extension will be included in the binary
* Enable debug mode in extension using `make debug`

## Running Tests
* The tests are present in `project_root/tests`. 
* Create a folder `duckdb_project_root/test/ivm`.
* Copy the test files into the above folder.
* Use the `unittest` executable and provide name of the test as program argument

