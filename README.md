# Presto Flex Connector
This is a presto connector to access local file (e.g. csv, tsv). Please keep in mind that this is not production ready and it was created for tests.

# Query
You need to specify file type by schema name.
```sql
select * from free.csv."file:///tmp/numbers-2.csv"
select * from free.csv."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers-2.csv"

select * from free.tsv."file:///tmp/numbers.tsv";
select * from free.tsv."https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers.tsv";
```