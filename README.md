# ivm-extension

Incrementally maintain view `result` using query `PRAGMA ivm_upsert('catalog', 'schema', 'result')`;

Read more [here](https://github.com/cwida/ivm-extension/blob/ivm-optimizer-rule/VLDB%20Summer%20School%202023%20Poster.pdf)

## Usage
Given a base table `hello` and a view `result` on top of `hello`, this extension incrementally computes the changes to the view `result` when the underlying table `hello` changes. 

This extension assumes that changes to base table `hello` are present in delta table `delta_hello`. This table additionally has a multiplicity column `_duckdb_ivm_multiplicity` of type `BOOL`. `duckdb_ivm_multiplicity=true` means insertions and `duckdb_ivm_multiplicity` means deletion. Updates to a row in base table `hello` are modelled as deletion+insertion.

First create the base table and the view:
```SQL
CREATE TABLE hello(a INTEGER, b INTEGER , c VARCHAR);
CREATE VIEW result AS (SELECT sum(a), count(c), b FROM hello GROUP BY b);
```

Create `delta_hello`:
```SQL
CREATE TABLE delta_hello AS (SELECT * FROM hello LIMIT 0);
ALTER TABLE delta_hello ADD COLUMN _duckdb_ivm_multiplicity BOOL;
INSERT INTO delta_hello VALUES (1,1, 'Mark',true), (2,2, 'Hannes',false), (3,1, 'Kriti',true), (4,1, 'Peter',false);
```
**NOTE**: The extension assumes the presence of the delta base table `delta_hello`.

### Incrementally maintaining view *result*
Run the extension as
```SQL
PRAGMA ivm_upsert('memory', 'main','result');
```
The output of the above will be the table `delta_result`, which will contain incremental processing of the changes to the base table of view `result`. 

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

