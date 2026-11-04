from typing import List, Union

from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class User(BaseModel):
    username: str
    password: str
    disabled: bool
    fullname: Union[str, None] = None
    department: Union[str, None] = None
    email: Union[str, None] = None


class changeUser(BaseModel):
    disabled: Union[bool, None] = None
    fullname: Union[str, None] = None
    department: Union[str, None] = None
    email: Union[str, None] = None


class changePassword(BaseModel):
    old_password: Union[str, None] = None
    new_password: str


class status(BaseModel):
    status: str
    version: str


class importDBs(BaseModel):
    name: str
    tables: int
    lastImport: Union[str, None] = None
    lastSize: Union[int, None] = None
    lastRows: Union[int, None] = None


# class testConnectionResponse(BaseModel):
# 	status: str


class connectionSearch(BaseModel):
    name: Union[str, None] = None
    connectionString: Union[str, None] = None
    timeWindowTimezone: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    contactInformation: Union[str, None] = None
    description: Union[str, None] = None
    owner: Union[str, None] = None


class connection(BaseModel):
    name: str
    connectionString: Union[str, None] = None
    serverType: Union[str, None] = None


class connectionDetails(BaseModel):
    name: str
    connectionString: str
    privateKeyPath: Union[str, None] = None
    publicKeyPath: Union[str, None] = None
    credentials: Union[str, None] = None
    source: Union[str, None] = None
    forceString: Union[int, None] = None
    maxSessions: Union[int, None] = None
    createDatalakeImport: Union[bool, None] = None
    timeWindowStart: Union[str, None] = None
    timeWindowStop: Union[str, None] = None
    timeWindowTimezone: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    contactInformation: Union[str, None] = None
    description: Union[str, None] = None
    owner: Union[str, None] = None
    environment: Union[str, None] = None
    seedFile: Union[str, None] = None
    createForeignKey: Union[bool, None] = None
    atlasDiscovery: Union[bool, None] = None
    atlasIncludeFilter: Union[str, None] = None
    atlasExcludeFilter: Union[str, None] = None
    atlasLastDiscovery: Union[str, None] = None


# class encryptCredentialResult(BaseModel):
# 	result: str


class encryptCredential(BaseModel):
    connection: str
    username: str
    password: str


class generateJDBCconnectionString(BaseModel):
    databaseType: str
    version: str
    hostname: str
    port: Union[int, None] = None
    database: str


class generatedJDBCconnectionString(BaseModel):
    connectionString: str


class jdbcDriver(BaseModel):
    databaseType: str
    version: str
    driver: str
    classpath: str


class configuration(BaseModel):
    airflow_aws_instanceids: Union[str, None] = None
    airflow_aws_pool_to_instanceid: Union[bool, None] = None
    airflow_create_pool_with_task: Union[bool, None] = None
    airflow_dag_directory: Union[str, None] = None
    airflow_dag_file_group: Union[str, None] = None
    airflow_dag_file_permission: Union[str, None] = None
    airflow_dag_staging_directory: Union[str, None] = None
    airflow_dbimport_commandpath: Union[str, None] = None
    airflow_default_pool_size: Union[int, None] = None
    airflow_disable: Union[bool, None] = None
    airflow_dummy_task_queue: Union[str, None] = None
    airflow_major_version: Union[int, None] = None
    airflow_sudo_user: Union[str, None] = None
    airflow_url: Union[str, None] = None
    atlas_discovery_interval: Union[int, None] = None
    cluster_name: Union[str, None] = None
    export_default_sessions: Union[int, None] = None
    export_max_sessions: Union[int, None] = None
    export_stage_disable: Union[bool, None] = None
    export_staging_database: Union[str, None] = None
    export_start_disable: Union[bool, None] = None
    hdfs_address: Union[str, None] = None
    hdfs_basedir: Union[str, None] = None
    hdfs_blocksize: Union[str, None] = None
    hive_acid_with_clusteredby: Union[bool, None] = None
    hive_insert_only_tables: Union[bool, None] = None
    hive_major_compact_after_merge: Union[bool, None] = None
    hive_print_messages: Union[bool, None] = None
    hive_remove_locks_by_force: Union[bool, None] = None
    hive_validate_before_execution: Union[bool, None] = None
    hive_validate_table: Union[str, None] = None
    impala_invalidate_metadata: Union[bool, None] = None
    import_columnname_delete: Union[str, None] = None
    import_columnname_histtime: Union[str, None] = None
    import_columnname_import: Union[str, None] = None
    import_columnname_insert: Union[str, None] = None
    import_columnname_iud: Union[str, None] = None
    import_columnname_source: Union[str, None] = None
    import_columnname_update: Union[str, None] = None
    import_default_sessions: Union[int, None] = None
    import_history_database: Union[str, None] = None
    import_history_table: Union[str, None] = None
    import_max_sessions: Union[int, None] = None
    import_process_empty: Union[bool, None] = None
    import_stage_disable: Union[bool, None] = None
    import_staging_database: Union[str, None] = None
    import_staging_table: Union[str, None] = None
    import_start_disable: Union[bool, None] = None
    import_work_database: Union[str, None] = None
    import_work_table: Union[str, None] = None
    kafka_brokers: Union[str, None] = None
    kafka_saslmechanism: Union[str, None] = None
    kafka_securityprotocol: Union[str, None] = None
    kafka_topic: Union[str, None] = None
    kafka_trustcafile: Union[str, None] = None
    post_airflow_dag_operations: Union[bool, None] = None
    post_data_to_kafka_extended: Union[bool, None] = None
    post_data_to_kafka: Union[bool, None] = None
    post_data_to_rest_extended: Union[bool, None] = None
    post_data_to_rest: Union[bool, None] = None
    post_data_to_awssns_extended: Union[bool, None] = None
    post_data_to_awssns: Union[bool, None] = None
    post_data_to_awssns_topic: Union[str, None] = None
    restserver_admin_user: Union[str, None] = None
    restserver_authentication_method: Union[str, None] = None
    restserver_token_ttl: Union[int, None] = None
    rest_timeout: Union[int, None] = None
    rest_trustcafile: Union[str, None] = None
    rest_url: Union[str, None] = None
    rest_verifyssl: Union[bool, None] = None
    spark_max_executors: Union[int, None] = None
    timezone: Union[str, None] = None
    timestamp_with_timezone: Union[bool, None] = None


class discoverImportTable(BaseModel):
    database: str
    table: str
    connection: str
    sourceSchema: str
    sourceTable: str


class discoverImportTableOptions(BaseModel):
    database: str
    connection: str
    schemaFilter: Union[str, None]
    tableFilter: Union[str, None]
    addSchemaToTable: bool
    addCounterToTable: bool
    counterStart: Union[int, None]
    addCustomText: Union[str, None]


class importTable(BaseModel):
    database: str
    table: str
    connection: str
    sourceSchema: str
    sourceTable: str
    importPhaseType: str
    etlPhaseType: str
    importTool: str
    etlEngine: str
    lastUpdateFromSource: Union[str, None] = None
    includeInAirflow: bool
    operatorNotes: Union[str, None] = None


class importTableColumnsRead(BaseModel):
    # 	database: str
    # 	table: str
    columnName: str
    columnOrder: int
    sourceColumnName: str
    columnType: str
    sourceColumnType: str
    sourceDatabaseType: Union[str, None] = None
    columnNameOverride: Union[str, None] = None
    columnTypeOverride: Union[str, None] = None
    sqoopColumnType: Union[str, None] = None
    sqoopColumnTypeOverride: Union[str, None] = None
    forceString: int
    includeInImport: bool
    sourcePrimaryKey: Union[int, None] = None
    lastUpdateFromSource: str
    comment: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    anonymizationFunction: str


class importTableColumnsWrite(BaseModel):
    # 	database: str
    # 	table: str
    columnName: str
    columnOrder: int
    sourceColumnName: str
    columnType: str
    sourceColumnType: str
    sourceDatabaseType: Union[str, None] = None
    columnNameOverride: Union[str, None] = None
    columnTypeOverride: Union[str, None] = None
    sqoopColumnType: Union[str, None] = None
    sqoopColumnTypeOverride: Union[str, None] = None
    forceString: int
    includeInImport: bool
    sourcePrimaryKey: Union[int, None] = None
    lastUpdateFromSource: str
    comment: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    anonymizationFunction: str


class importTableDetailsRead(BaseModel):
    database: str
    table: str
    connection: str
    sourceSchema: str
    sourceTable: str
    importPhaseType: str
    etlPhaseType: str
    importTool: str
    etlEngine: str
    lastUpdateFromSource: Union[str, None] = None
    sqlWhereAddition: Union[str, None] = None
    nomergeIngestionSqlAddition: Union[str, None] = None
    includeInAirflow: bool
    airflowPriority: Union[int, None] = None
    validateImport: bool
    validationMethod: str
    validateSource: str
    validateDiffAllowed: int
    validationCustomQuerySourceSQL: Union[str, None] = None
    validationCustomQueryHiveSQL: Union[str, None] = None
    validationCustomQueryValidateImportTable: bool
    sqlSessions: int
    softDeleteDuringMerge: bool
    sourceRowcount: Union[int, None] = None
    sourceRowcountIncr: Union[int, None] = None
    targetRowcount: Union[int, None] = None
    validationCustomQuerySourceValue: Union[str, None] = None
    validationCustomQueryHiveValue: Union[str, None] = None
    incrMode: Union[str, None] = None
    incrColumn: Union[str, None] = None
    incrValidationMethod: str
    incrMinvalue: Union[str, None] = None
    incrMaxvalue: Union[str, None] = None
    incrMinvaluePending: Union[str, None] = None
    incrMaxvaluePending: Union[str, None] = None
    pkColumnOverride: Union[str, None] = None
    pkColumnOverrideMergeonly: Union[str, None] = None
    hiveContainerSize: Union[int, None] = None
    splitCount: Union[int, None] = None
    sparkExecutorMemory: Union[str, None] = None
    sparkExecutors: Union[int, None] = None
    splitByColumn: Union[str, None] = None
    sqoopCustomQuery: Union[str, None] = None
    sqoopOptions: Union[str, None] = None
    lastSize: Union[int, None] = None
    lastRows: Union[int, None] = None
    lastSqlSessions: Union[int, None] = None
    lastExecution: Union[int, None] = None
    sqoopUseGeneratedSql: bool
    allowTextSplitter: bool
    forceString: int
    comment: Union[str, None] = None
    generatedHiveColumnDefinition: Union[str, None] = None
    generatedSqoopQuery: Union[str, None] = None
    generatedSqoopOptions: Union[str, None] = None
    generatedPkColumns: Union[str, None] = None
    datalakeSource: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    copyFinished: Union[str, None] = None
    copySlave: bool
    createForeignKeys: int
    invalidateImpala: int
    customMaxQuery: Union[str, None] = None
    mergeCompactionMethod: str
    sourceTableType: Union[str, None] = None
    importDatabase: Union[str, None] = None
    importTable: Union[str, None] = None
    historyDatabase: Union[str, None] = None
    historyTable: Union[str, None] = None
    columns: List[importTableColumnsRead] = []


class importTableDetailsWrite(BaseModel):
    # 	database: str
    # 	table: str
    connection: str
    sourceSchema: str
    sourceTable: str
    importPhaseType: str
    etlPhaseType: str
    importTool: str
    etlEngine: str
    lastUpdateFromSource: Union[str, None] = None
    sqlWhereAddition: Union[str, None] = None
    nomergeIngestionSqlAddition: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    airflowPriority: Union[int, None] = None
    validateImport: Union[bool, None] = None
    validationMethod: Union[str, None] = None
    validateSource: Union[str, None] = None
    validateDiffAllowed: Union[int, None] = None
    validationCustomQuerySourceSQL: Union[str, None] = None
    validationCustomQueryHiveSQL: Union[str, None] = None
    validationCustomQueryValidateImportTable: Union[bool, None] = None
    sqlSessions: Union[int, None] = None
    softDeleteDuringMerge: Union[bool, None] = None
    incrMode: Union[str, None] = None
    incrColumn: Union[str, None] = None
    incrValidationMethod: Union[str, None] = None
    pkColumnOverride: Union[str, None] = None
    pkColumnOverrideMergeonly: Union[str, None] = None
    hiveContainerSize: Union[int, None] = None
    splitCount: Union[int, None] = None
    sparkExecutorMemory: Union[str, None] = None
    sparkExecutors: Union[int, None] = None
    splitByColumn: Union[str, None] = None
    sqoopCustomQuery: Union[str, None] = None
    sqoopOptions: Union[str, None] = None
    sqoopUseGeneratedSql: Union[bool, None] = None
    allowTextSplitter: Union[bool, None] = None
    forceString: Union[int, None] = None
    comment: Union[str, None] = None
    datalakeSource: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    copySlave: Union[bool, None] = None
    createForeignKeys: Union[int, None] = None
    invalidateImpala: Union[int, None] = None
    customMaxQuery: Union[str, None] = None
    mergeCompactionMethod: Union[str, None] = None
    sourceTableType: Union[str, None] = None
    importDatabase: Union[str, None] = None
    importTable: Union[str, None] = None
    historyDatabase: Union[str, None] = None
    historyTable: Union[str, None] = None
    columns: List[importTableColumnsWrite] = []


class importTableSearch(BaseModel):
    database: Union[str, None] = None
    table: Union[str, None] = None
    connection: Union[str, None] = None
    sourceSchema: Union[str, None] = None
    sourceTable: Union[str, None] = None
    importPhaseType: Union[str, None] = None
    etlPhaseType: Union[str, None] = None
    importTool: Union[str, None] = None
    etlEngine: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    operatorNotes: Union[str, None] = None


class importTablePK(BaseModel):
    database: str
    table: str


class importTableBulkUpdate(BaseModel):
    connection: Union[str, None] = None
    importPhaseType: Union[str, None] = None
    etlPhaseType: Union[str, None] = None
    importTool: Union[str, None] = None
    etlEngine: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    operatorNotes: Union[str, None] = None
    importTables: List[importTablePK]


class defaultResultResponse(BaseModel):
    result: str


class exportConnections(BaseModel):
    name: str
    tables: int
    lastExport: Union[str, None] = None
    lastRows: Union[int, None] = None


class exportTableSearch(BaseModel):
    connection: Union[str, None] = None
    targetSchema: Union[str, None] = None
    targetTable: Union[str, None] = None
    database: Union[str, None] = None
    table: Union[str, None] = None
    exportType: Union[str, None] = None
    exportTool: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    operatorNotes: Union[str, None] = None


class discoverExportTable(BaseModel):
    database: str
    table: str
    connection: str
    targetSchema: str
    targetTable: str


class discoverExportTableOptions(BaseModel):
    connection: str
    targetSchema: str
    databaseFilter: Union[str, None]
    tableFilter: Union[str, None]
    addDBToTable: bool
    addCounterToTable: bool
    counterStart: Union[int, None]
    addCustomText: Union[str, None]


class exportTable(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str
    database: str
    table: str
    exportType: str
    exportTool: str
    lastUpdateFromHive: Union[str, None] = None
    includeInAirflow: bool
    operatorNotes: Union[str, None] = None


class exportTableColumnsRead(BaseModel):
    # 	connection: str
    # 	targetSchema: str
    # 	targetTable: str
    columnName: str
    columnType: str
    columnOrder: Union[int, None] = None
    targetColumnName: Union[str, None] = None
    targetColumnType: Union[str, None] = None
    lastUpdateFromHive: Union[str, None] = None
    includeInExport: bool
    comment: Union[str, None] = None
    operatorNotes: Union[str, None] = None


class exportTableColumnsWrite(BaseModel):
    # 	connection: str
    # 	targetSchema: str
    # 	targetTable: str
    columnName: str
    columnType: str
    columnOrder: Union[int, None] = None
    targetColumnName: Union[str, None] = None
    targetColumnType: Union[str, None] = None
    lastUpdateFromHive: Union[str, None] = None
    includeInExport: bool
    comment: Union[str, None] = None
    operatorNotes: Union[str, None] = None


class exportTableDetailsRead(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str
    exportType: str
    exportTool: str
    database: str
    table: str
    lastUpdateFromHive: Union[str, None] = None
    sqlWhereAddition: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    airflowPriority: Union[int, None] = None
    forceCreateTempTable: Union[bool, None] = None
    validateExport: Union[bool, None] = None
    validationMethod: Union[str, None] = None
    validationCustomQueryHiveSQL: Union[str, None] = None
    validationCustomQueryTargetSQL: Union[str, None] = None
    uppercaseColumns: Union[int, None] = None
    truncateTarget: Union[bool, None] = None
    sqlSessions: Union[int, None] = None
    tableRowcount: Union[int, None] = None
    targetRowcount: Union[int, None] = None
    validationCustomQueryHiveValue: Union[str, None] = None
    validationCustomQueryTargetValue: Union[str, None] = None
    incrColumn: Union[str, None] = None
    incrValidationMethod: Union[str, None] = None
    incrMinvalue: Union[str, None] = None
    incrMaxvalue: Union[str, None] = None
    incrMinvaluePending: Union[str, None] = None
    incrMaxvaluePending: Union[str, None] = None
    sqoopOptions: Union[str, None] = None
    lastSize: Union[int, None] = None
    lastRows: Union[int, None] = None
    lastSqlSessions: Union[int, None] = None
    lastExecution: Union[int, None] = None
    hiveContainerSize: Union[int, None] = None
    createTargetTableSql: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    columns: List[exportTableColumnsRead] = []


class exportTableDetailsWrite(BaseModel):
    # 	connection: str
    # 	targetSchema: str
    # 	targetTable: str
    exportType: str
    exportTool: str
    database: str
    table: str
    lastUpdateFromHive: Union[str, None] = None
    sqlWhereAddition: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    airflowPriority: Union[int, None] = None
    forceCreateTempTable: Union[bool, None] = None
    validateExport: Union[bool, None] = None
    validationMethod: Union[str, None] = None
    validationCustomQueryHiveSQL: Union[str, None] = None
    validationCustomQueryTargetSQL: Union[str, None] = None
    uppercaseColumns: Union[int, None] = None
    truncateTarget: Union[bool, None] = None
    sqlSessions: Union[int, None] = None
    # 	tableRowcount: Union[int, None] = None
    # 	targetRowcount: Union[int, None] = None
    # 	validationCustomQueryHiveValue: Union[str, None] = None
    # 	validationCustomQueryTargetValue: Union[str, None] = None
    incrColumn: Union[str, None] = None
    incrValidationMethod: Union[str, None] = None
    # 	incrMinvalue: Union[str, None] = None
    # 	incrMaxvalue: Union[str, None] = None
    # 	incrMinvaluePending: Union[str, None] = None
    # 	incrMaxvaluePending: Union[str, None] = None
    sqoopOptions: Union[str, None] = None
    # 	lastSize: Union[int, None] = None
    # 	lastRows: Union[int, None] = None
    # 	lastSqlSessions: Union[int, None] = None
    # 	lastExecution: Union[int, None] = None
    hiveContainerSize: Union[int, None] = None
    createTargetTableSql: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    columns: List[exportTableColumnsWrite] = []


class exportTablePK(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str


class exportTableBulkUpdate(BaseModel):
    exportType: Union[str, None] = None
    exportTool: Union[str, None] = None
    includeInAirflow: Union[bool, None] = None
    operatorNotes: Union[str, None] = None
    exportTables: List[exportTablePK]


class airflowAllDags(BaseModel):
    name: str
    type: str
    scheduleInterval: Union[str, None] = None
    autoRegenerateDag: bool


# 	operatorNotes: Union[str, None] = None
# 	applicationNotes: Union[str, None] = None


class airflowImportDags(BaseModel):
    name: str
    filterTable: Union[str, None] = None
    scheduleInterval: Union[str, None] = None
    autoRegenerateDag: bool
    airflowLink: Union[str, None] = None
    # 	operatorNotes: Union[str, None] = None
    # 	applicationNotes: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    slaWarningTime: Union[str, None] = None
    concurrency: Union[int, None] = None


class airflowExportDags(BaseModel):
    name: str
    filterConnection: Union[str, None] = None
    filterTargetSchema: Union[str, None] = None
    filterTargetTable: Union[str, None] = None
    scheduleInterval: Union[str, None] = None
    autoRegenerateDag: bool
    airflowLink: Union[str, None] = None
    # 	operatorNotes: Union[str, None] = None
    # 	applicationNotes: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    slaWarningTime: Union[str, None] = None
    concurrency: Union[int, None] = None


class airflowCustomDags(BaseModel):
    name: str
    scheduleInterval: Union[str, None] = None
    autoRegenerateDag: bool
    airflowLink: Union[str, None] = None
    # 	operatorNotes: Union[str, None] = None
    # 	applicationNotes: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    slaWarningTime: Union[str, None] = None
    concurrency: Union[int, None] = None


class airflowTask(BaseModel):
    name: str
    type: str
    placement: str
    connection: Union[str, None] = None
    airflowPool: Union[str, None] = None
    airflowPriority: Union[int, None] = None
    includeInAirflow: bool
    taskDependencyDownstream: Union[str, None] = None
    taskDependencyUpstream: Union[str, None] = None
    taskConfig: Union[str, None] = None
    sensorPokeInterval: Union[int, None] = None
    sensorTimeoutMinutes: Union[int, None] = None
    sensorConnection: Union[str, None] = None
    sensorSoftFail: Union[int, None] = None
    sudoUser: Union[str, None] = None


class airflowCustomDag(BaseModel):
    name: str
    scheduleInterval: Union[str, None] = None
    retries: Union[int, None] = None
    operatorNotes: Union[str, None] = None
    applicationNotes: Union[str, None] = None
    airflowNotes: Union[str, None] = None
    autoRegenerateDag: Union[bool, None] = None
    sudoUser: Union[str, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    tags: Union[str, None] = None
    slaWarningTime: Union[str, None] = None
    retryExponentialBackoff: Union[int, None] = None
    concurrency: Union[int, None] = None
    airflowLink: Union[str, None] = None
    tasks: List[airflowTask] = []


class airflowExportDag(BaseModel):
    name: str
    scheduleInterval: Union[str, None] = None
    filterConnection: Union[str, None] = None
    filterTargetSchema: Union[str, None] = None
    filterTargetTable: Union[str, None] = None
    retries: Union[int, None] = None
    operatorNotes: Union[str, None] = None
    applicationNotes: Union[str, None] = None
    autoRegenerateDag: Union[bool, None] = None
    airflowNotes: Union[str, None] = None
    sudoUser: Union[str, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    tags: Union[str, None] = None
    slaWarningTime: Union[str, None] = None
    retryExponentialBackoff: Union[int, None] = None
    concurrency: Union[int, None] = None
    airflowLink: Union[str, None] = None
    tasks: List[airflowTask] = []


class airflowImportDag(BaseModel):
    name: str
    scheduleInterval: Union[str, None] = None
    filterTable: Union[str, None] = None
    finishAllStage1First: Union[bool, None] = None
    runImportAndEtlSeparate: Union[bool, None] = None
    retries: Union[int, None] = None
    retriesStage1: Union[int, None] = None
    retriesStage2: Union[int, None] = None
    poolStage1: Union[str, None] = None
    poolStage2: Union[str, None] = None
    operatorNotes: Union[str, None] = None
    applicationNotes: Union[str, None] = None
    autoRegenerateDag: Union[bool, None] = None
    airflowNotes: Union[str, None] = None
    sudoUser: Union[str, None] = None
    metadataImport: Union[bool, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    tags: Union[str, None] = None
    slaWarningTime: Union[str, None] = None
    retryExponentialBackoff: Union[bool, None] = None
    concurrency: Union[int, None] = None
    airflowLink: Union[str, None] = None
    tasks: List[airflowTask] = []


class airflowDagPK(BaseModel):
    name: str


class airflowDagBulkUpdate(BaseModel):
    scheduleInterval: Union[str, None] = None
    autoRegenerateDag: Union[bool, None] = None
    operatorNotes: Union[str, None] = None
    sudoUser: Union[str, None] = None
    timezone: Union[str, None] = None
    email: Union[str, None] = None
    emailOnFailure: Union[bool, None] = None
    emailOnRetries: Union[bool, None] = None
    slaWarningTime: Union[str, None] = None
    concurrency: Union[int, None] = None
    dags: List[airflowDagPK]
