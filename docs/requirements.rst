Requirements
============

DBImport have the following requirements on the server where it's installed and on the Hadoop environment in which its operating

  - Python 3.11, and Spark 3.4 or later on the cluster
  - Hive running with binary transport mode. Preferable LLAP instance
  - Kerberos enabled
  - MySQL or MariaDB database for configuration, with the latin1 character set
  - Direct JDBC connection to the Hive Metastore database

