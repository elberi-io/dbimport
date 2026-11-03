# DBImport specific
VERSION = "1.00.006"

# Database types
MYSQL = "mysql"
ORACLE = "oracle"
MSSQL = "sql_server"
POSTGRESQL = "postgresql"
PROGRESS = "progress_db"
DB2_UDB = "db2 udb"
DB2_AS400 = "db2 as400"
MONGO = "mongo"
CACHEDB = "cache"
SNOWFLAKE = "snowflake"
AWS_S3 = "aws_s3"
INFORMIX = "informix"
SQLANYWHERE = "sqlanywhere"

# Key constraints
PRIMARY_KEY = "P"
FOREIGN_KEY = "F"

# Phase defintions
IMPORT_PHASE_MONGO_FULL = "mongo_full"
IMPORT_PHASE_ORACLE_FLASHBACK = "oracle_flashback"
IMPORT_PHASE_MSSQL_CHANGE_TRACKING = "mssql_change_tracking"
IMPORT_PHASE_FULL = "full"
IMPORT_PHASE_INCR = "incr"
IMPORT_PHASE_SLAVE = "slave"
COPY_PHASE_NONE = "none"
ETL_PHASE_NONE = "none"
ETL_PHASE_EXTERNAL = "external"
ETL_PHASE_INSERT = "insert"
ETL_PHASE_TRUNCATEINSERT = "truncate_insert"
ETL_PHASE_MERGEHISTORYAUDIT = "merge_history_audit"
ETL_PHASE_MERGEONLY = "merge"
EXPORT_PHASE_FULL = "full"
EXPORT_PHASE_AWSS3_FULL = "awss3_full"
EXPORT_PHASE_INCR = "incr"

VALIDATION_METHOD_ROWCOUNT = "rowCount"
VALIDATION_METHOD_CUSTOMQUERY = "customQuery"

ETL_ENGINE_HIVE = "hive"
ETL_ENGINE_SPARK = "spark"

CATALOG_HIVE_DIRECT = "hive_direct"
CATALOG_GLUE = "glue_catalog"

# General result
RESULT_OK = "ok"
RESULT_FAILED = "failed"
RESULT_FOREIGN_KEY_FAILED = "fk_failed"

