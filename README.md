## BaikalDB：A Distributed HTAP Database
[![Build Status](https://travis-ci.org/baidu/BaikalDB.svg?branch=master)](https://travis-ci.org/baidu/BaikalDB)

BaikalDB supports sequential and randomised realtime read/write of structural data in petabytes-scale.
BaikalDB is compatible with MySQL protocol and it supports MySQL style SQL dialect, by which users can migrate their data storage from MySQL to BaikalDB seamlessly.

BaikalDB internally provides projections, filter operators (corresponding with SQL WHERE or HAVING clause), aggregation operators (corresponding with GROPY BY clause) and sort operators (corresponding with SQL ORDER BY), with which users can fulfill their complex and time-critical anayltical and transational requirement by writing SQL statements. In a typical scenario, hundreds of millions of rows can be scanned and aggregated in few seconds.

BaikalDB also supports full-text search by building inverted indices after words segmentation. 
Users can harness fuzzy search feature simply by adding a `FULLTEXT KEY` type index when creating tables and then use LIKE clause in their queries.

See the github [wiki](https://github.com/baidu/BaikalDB/wiki) for more explanation.

## License
baidu/BaikalDB is licensed under the Apache License 2.0

## Acknowledgements
* We are especially grateful to the teams of RocksDB, brpc and braft, who built powerful and stable libraries to support important features of BaikalDB.
* We give special thanks to TiDB team and Impala team. We referred their design schemes when designing and developing BaikalDB.
* Last but not least, we give special thanks to the authors of all libraries that BaikalDB depends on, without whom BaikalDB could not have been developed and built so easily.
* Thanks our friend team -- The Baidu TafDB team, who provide the space efficient snapshot scheme based on braft.
