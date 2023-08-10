# Trino Storage Connector [![Build Status](https://github.com/snowlift/trino-storage/workflows/CI/badge.svg)](https://github.com/snowlift/trino-storage/actions?query=workflow%3ACI+event%3Apush+branch%3Amaster)
This is a [Trino](http://trino.io/) connector to access single file (e.g. csv, tsv). Please keep in mind that this is not production ready and it was created for tests.

# Supported scheme
- hdfs
- s3a
- file
- http
- https

> Note: reading local files (with the `file` or no schema) can be disabled by setting `allow-local-files=false` in the catalog configuration.

# Query
You need to specify file type by schema name and use absolute path.
```sql
SELECT * FROM
storage.csv."file:///tmp/numbers-2.csv";

SELECT * FROM
storage.csv."https://raw.githubusercontent.com/snowlift/trino-storage/master/src/test/resources/example-data/numbers-2.csv";
``` 

Supported schemas are below.
- `tsv`
- `csv`
- `ssv`
- `txt`
- `raw`
- `excel`
- `orc`
- `json`

`csv` plugin extracts each line, with columns separated by `,`. Currently the first line is used as column names.
```sql
SELECT * FROM
storage.csv."https://raw.githubusercontent.com/snowlift/trino-storage/master/src/test/resources/example-data/numbers-2.csv";
```
```
  ten   | 10 
--------+----
 eleven | 11 
 twelve | 12
(2 rows)
```

Tab (`\t`) and semicolon (`;`) delimiters are also supported, using the `tsv` and `ssv` plugins, respectively.

`txt` plugin doesn't extract each line. Currently column name is always `value`.
```sql
SELECT * FROM
storage.txt."https://raw.githubusercontent.com/snowlift/trino-storage/master/src/test/resources/example-data/numbers.tsv";
``` 
```
 value  
--------
 one    1   
 two    2   
 three  3
(3 rows)
```

`raw` plugin doesn't extract each line. Currently column name is always `data`. This connector is similar to `txt` plugin. 
The main difference is `txt` plugin may return multiple rows, but `raw` plugin always return only one row.
```sql
SELECT * FROM
storage.raw."https://raw.githubusercontent.com/snowlift/trino-storage/master/src/test/resources/example-data/numbers.tsv";
``` 
```
  data  
--------
 one    1   
 two    2   
 three  3 
(1 row)
```

`excel` plugin currently read first sheet.
```sql
SELECT * FROM
storage.excel."https://raw.githubusercontent.com/snowlift/trino-storage/master/src/test/resources/example-data/sample.xlsx";
``` 
```
  data  
--------
 one    1   
 two    2   
 three  3 
(1 row)
```

# Table functions

The connector provides specific table functions to list directory status and read files.
```sql
SELECT * FROM TABLE(storage.system.list('/tmp/trino-storage'));
```
```
     file_modified_time      | size |            name
-----------------------------+------+-----------------------------
 2023-05-03 12:14:22.107 UTC |   12 | /tmp/trino-storage/test.txt
```

```sql
SELECT * FROM TABLE(storage.system.read_file('csv', '/tmp/trino-storage/test.txt'));
```
```
     col
-------------
 hello world
```

# Build
Run all the unit test classes.
```
./mvnw test
```

Build without running tests
```
./mvnw clean install -DskipTests
```

> Note: tests include intergration tests, that will run Minio and HDFS as Docker containers. They need to pull their images,
> which can take a while. If you see the tests getting stuck, try pulling these images before starting tests, to see the progress.
> Look for image names and versions in `TestingMinioServer` and `TestingHadoopServer` test classes.
> It is also required to set the `HADOOP_USER_NAME` environmental variable to `hive`.

# Deploy
Unarchive trino-storage-{version}.zip and copy jar files in target directory to use storage connector in your Trino cluster.
