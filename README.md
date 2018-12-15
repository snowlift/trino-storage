# Presto Flex Connector
This is a presto connector to access local file (e.g. csv, tsv). Please keep in mind that this is not production ready and it was created for tests.

# Query
You need to specify file type by schema name.
```sql
select * from free.csv."http://s3.amazonaws.com/presto-example/v2/numbers-1.csv"

select * from free.tsv."file:///tmp/numbers.tsv";
```