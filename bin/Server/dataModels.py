from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None


class User(BaseModel):
    username: str
    password: str
    disabled: bool
    fullname: str | None = None
    department: str | None = None
    email: str | None = None


class changeUser(BaseModel):
    disabled: bool | None = None
    fullname: str | None = None
    department: str | None = None
    email: str | None = None


class changePassword(BaseModel):
    old_password: str | None = None
    new_password: str


class status(BaseModel):
    status: str
    version: str


class importDBs(BaseModel):
    name: str
    tables: int
    lastImport: str | None = None
    lastSize: int | None = None
    lastRows: int | None = None


# class testConnectionResponse(BaseModel):
# 	status: str


class connectionSearch(BaseModel):
    name: str | None = None
    connectionString: str | None = None
    timeWindowTimezone: str | None = None
    operatorNotes: str | None = None
    contactInformation: str | None = None
    description: str | None = None
    owner: str | None = None


class connection(BaseModel):
    name: str
    connectionString: str | None = None
    serverType: str | None = None


class connectionDetails(BaseModel):
    name: str
    connectionString: str
    privateKeyPath: str | None = None
    publicKeyPath: str | None = None
    credentials: str | None = None
    source: str | None = None
    forceString: int | None = None
    maxSessions: int | None = None
    createDatalakeImport: bool | None = None
    timeWindowStart: str | None = None
    timeWindowStop: str | None = None
    timeWindowTimezone: str | None = None
    operatorNotes: str | None = None
    contactInformation: str | None = None
    description: str | None = None
    owner: str | None = None
    environment: str | None = None
    seedFile: str | None = None
    createForeignKey: bool | None = None
    atlasDiscovery: bool | None = None
    atlasIncludeFilter: str | None = None
    atlasExcludeFilter: str | None = None
    atlasLastDiscovery: str | None = None


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
    port: int | None = None
    database: str


class generatedJDBCconnectionString(BaseModel):
    connectionString: str


class jdbcDriver(BaseModel):
    databaseType: str
    version: str
    driver: str
    classpath: str


class configuration(BaseModel):
    airflow_aws_instanceids: str | None = None
    airflow_aws_pool_to_instanceid: bool | None = None
    airflow_create_pool_with_task: bool | None = None
    airflow_dag_directory: str | None = None
    airflow_dag_file_group: str | None = None
    airflow_dag_file_permission: str | None = None
    airflow_dag_staging_directory: str | None = None
    airflow_dbimport_commandpath: str | None = None
    airflow_default_pool_size: int | None = None
    airflow_disable: bool | None = None
    airflow_dummy_task_queue: str | None = None
    airflow_major_version: int | None = None
    airflow_sudo_user: str | None = None
    airflow_url: str | None = None
    atlas_discovery_interval: int | None = None
    cluster_name: str | None = None
    export_default_sessions: int | None = None
    export_max_sessions: int | None = None
    export_stage_disable: bool | None = None
    export_staging_database: str | None = None
    export_start_disable: bool | None = None
    hdfs_address: str | None = None
    hdfs_basedir: str | None = None
    hdfs_blocksize: str | None = None
    hive_acid_with_clusteredby: bool | None = None
    hive_insert_only_tables: bool | None = None
    hive_major_compact_after_merge: bool | None = None
    hive_print_messages: bool | None = None
    hive_remove_locks_by_force: bool | None = None
    hive_validate_before_execution: bool | None = None
    hive_validate_table: str | None = None
    impala_invalidate_metadata: bool | None = None
    import_columnname_delete: str | None = None
    import_columnname_histtime: str | None = None
    import_columnname_import: str | None = None
    import_columnname_insert: str | None = None
    import_columnname_iud: str | None = None
    import_columnname_source: str | None = None
    import_columnname_update: str | None = None
    import_default_sessions: int | None = None
    import_history_database: str | None = None
    import_history_table: str | None = None
    import_max_sessions: int | None = None
    import_process_empty: bool | None = None
    import_stage_disable: bool | None = None
    import_staging_database: str | None = None
    import_staging_table: str | None = None
    import_start_disable: bool | None = None
    import_work_database: str | None = None
    import_work_table: str | None = None
    kafka_brokers: str | None = None
    kafka_saslmechanism: str | None = None
    kafka_securityprotocol: str | None = None
    kafka_topic: str | None = None
    kafka_trustcafile: str | None = None
    post_airflow_dag_operations: bool | None = None
    post_data_to_kafka_extended: bool | None = None
    post_data_to_kafka: bool | None = None
    post_data_to_rest_extended: bool | None = None
    post_data_to_rest: bool | None = None
    post_data_to_awssns_extended: bool | None = None
    post_data_to_awssns: bool | None = None
    post_data_to_awssns_topic: str | None = None
    restserver_admin_user: str | None = None
    restserver_authentication_method: str | None = None
    restserver_token_ttl: int | None = None
    rest_timeout: int | None = None
    rest_trustcafile: str | None = None
    rest_url: str | None = None
    rest_verifyssl: bool | None = None
    spark_max_executors: int | None = None
    timezone: str | None = None
    timestamp_with_timezone: bool | None = None


class discoverImportTable(BaseModel):
    database: str
    table: str
    connection: str
    sourceSchema: str
    sourceTable: str


class discoverImportTableOptions(BaseModel):
    database: str
    connection: str
    schemaFilter: str | None
    tableFilter: str | None
    addSchemaToTable: bool
    addCounterToTable: bool
    counterStart: int | None
    addCustomText: str | None


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
    lastUpdateFromSource: str | None = None
    includeInAirflow: bool
    operatorNotes: str | None = None


class importTableColumnsRead(BaseModel):
    # 	database: str
    # 	table: str
    columnName: str
    columnOrder: int
    sourceColumnName: str
    columnType: str
    sourceColumnType: str
    sourceDatabaseType: str | None = None
    columnNameOverride: str | None = None
    columnTypeOverride: str | None = None
    sqoopColumnType: str | None = None
    sqoopColumnTypeOverride: str | None = None
    forceString: int
    includeInImport: bool
    sourcePrimaryKey: int | None = None
    lastUpdateFromSource: str
    comment: str | None = None
    operatorNotes: str | None = None
    anonymizationFunction: str


class importTableColumnsWrite(BaseModel):
    # 	database: str
    # 	table: str
    columnName: str
    columnOrder: int
    sourceColumnName: str
    columnType: str
    sourceColumnType: str
    sourceDatabaseType: str | None = None
    columnNameOverride: str | None = None
    columnTypeOverride: str | None = None
    sqoopColumnType: str | None = None
    sqoopColumnTypeOverride: str | None = None
    forceString: int
    includeInImport: bool
    sourcePrimaryKey: int | None = None
    lastUpdateFromSource: str
    comment: str | None = None
    operatorNotes: str | None = None
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
    lastUpdateFromSource: str | None = None
    sqlWhereAddition: str | None = None
    nomergeIngestionSqlAddition: str | None = None
    includeInAirflow: bool
    airflowPriority: int | None = None
    validateImport: bool
    validationMethod: str
    validateSource: str
    validateDiffAllowed: int
    validationCustomQuerySourceSQL: str | None = None
    validationCustomQueryHiveSQL: str | None = None
    validationCustomQueryValidateImportTable: bool
    sqlSessions: int
    softDeleteDuringMerge: bool
    sourceRowcount: int | None = None
    sourceRowcountIncr: int | None = None
    targetRowcount: int | None = None
    validationCustomQuerySourceValue: str | None = None
    validationCustomQueryHiveValue: str | None = None
    incrMode: str | None = None
    incrColumn: str | None = None
    incrValidationMethod: str
    incrMinvalue: str | None = None
    incrMaxvalue: str | None = None
    incrMinvaluePending: str | None = None
    incrMaxvaluePending: str | None = None
    pkColumnOverride: str | None = None
    pkColumnOverrideMergeonly: str | None = None
    hiveContainerSize: int | None = None
    splitCount: int | None = None
    sparkExecutorMemory: str | None = None
    sparkExecutors: int | None = None
    splitByColumn: str | None = None
    sqoopCustomQuery: str | None = None
    sqoopOptions: str | None = None
    lastSize: int | None = None
    lastRows: int | None = None
    lastSqlSessions: int | None = None
    lastExecution: int | None = None
    sqoopUseGeneratedSql: bool
    allowTextSplitter: bool
    forceString: int
    comment: str | None = None
    generatedHiveColumnDefinition: str | None = None
    generatedSqoopQuery: str | None = None
    generatedSqoopOptions: str | None = None
    generatedPkColumns: str | None = None
    datalakeSource: str | None = None
    operatorNotes: str | None = None
    copyFinished: str | None = None
    copySlave: bool
    createForeignKeys: int
    invalidateImpala: int
    customMaxQuery: str | None = None
    mergeCompactionMethod: str
    sourceTableType: str | None = None
    importDatabase: str | None = None
    importTable: str | None = None
    historyDatabase: str | None = None
    historyTable: str | None = None
    columns: list[importTableColumnsRead] = []


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
    lastUpdateFromSource: str | None = None
    sqlWhereAddition: str | None = None
    nomergeIngestionSqlAddition: str | None = None
    includeInAirflow: bool | None = None
    airflowPriority: int | None = None
    validateImport: bool | None = None
    validationMethod: str | None = None
    validateSource: str | None = None
    validateDiffAllowed: int | None = None
    validationCustomQuerySourceSQL: str | None = None
    validationCustomQueryHiveSQL: str | None = None
    validationCustomQueryValidateImportTable: bool | None = None
    sqlSessions: int | None = None
    softDeleteDuringMerge: bool | None = None
    incrMode: str | None = None
    incrColumn: str | None = None
    incrValidationMethod: str | None = None
    pkColumnOverride: str | None = None
    pkColumnOverrideMergeonly: str | None = None
    hiveContainerSize: int | None = None
    splitCount: int | None = None
    sparkExecutorMemory: str | None = None
    sparkExecutors: int | None = None
    splitByColumn: str | None = None
    sqoopCustomQuery: str | None = None
    sqoopOptions: str | None = None
    sqoopUseGeneratedSql: bool | None = None
    allowTextSplitter: bool | None = None
    forceString: int | None = None
    comment: str | None = None
    datalakeSource: str | None = None
    operatorNotes: str | None = None
    copySlave: bool | None = None
    createForeignKeys: int | None = None
    invalidateImpala: int | None = None
    customMaxQuery: str | None = None
    mergeCompactionMethod: str | None = None
    sourceTableType: str | None = None
    importDatabase: str | None = None
    importTable: str | None = None
    historyDatabase: str | None = None
    historyTable: str | None = None
    columns: list[importTableColumnsWrite] = []


class importTableSearch(BaseModel):
    database: str | None = None
    table: str | None = None
    connection: str | None = None
    sourceSchema: str | None = None
    sourceTable: str | None = None
    importPhaseType: str | None = None
    etlPhaseType: str | None = None
    importTool: str | None = None
    etlEngine: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None


class importTablePK(BaseModel):
    database: str
    table: str


class importTableBulkUpdate(BaseModel):
    connection: str | None = None
    importPhaseType: str | None = None
    etlPhaseType: str | None = None
    importTool: str | None = None
    etlEngine: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None
    importTables: list[importTablePK]


class defaultResultResponse(BaseModel):
    result: str


class exportConnections(BaseModel):
    name: str
    tables: int
    lastExport: str | None = None
    lastRows: int | None = None


class exportTableSearch(BaseModel):
    connection: str | None = None
    targetSchema: str | None = None
    targetTable: str | None = None
    database: str | None = None
    table: str | None = None
    exportType: str | None = None
    exportTool: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None


class discoverExportTable(BaseModel):
    database: str
    table: str
    connection: str
    targetSchema: str
    targetTable: str


class discoverExportTableOptions(BaseModel):
    connection: str
    targetSchema: str
    databaseFilter: str | None
    tableFilter: str | None
    addDBToTable: bool
    addCounterToTable: bool
    counterStart: int | None
    addCustomText: str | None


class exportTable(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str
    database: str
    table: str
    exportType: str
    exportTool: str
    lastUpdateFromHive: str | None = None
    includeInAirflow: bool
    operatorNotes: str | None = None


class exportTableColumnsRead(BaseModel):
    # 	connection: str
    # 	targetSchema: str
    # 	targetTable: str
    columnName: str
    columnType: str
    columnOrder: int | None = None
    targetColumnName: str | None = None
    targetColumnType: str | None = None
    lastUpdateFromHive: str | None = None
    includeInExport: bool
    comment: str | None = None
    operatorNotes: str | None = None


class exportTableColumnsWrite(BaseModel):
    # 	connection: str
    # 	targetSchema: str
    # 	targetTable: str
    columnName: str
    columnType: str
    columnOrder: int | None = None
    targetColumnName: str | None = None
    targetColumnType: str | None = None
    lastUpdateFromHive: str | None = None
    includeInExport: bool
    comment: str | None = None
    operatorNotes: str | None = None


class exportTableDetailsRead(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str
    exportType: str
    exportTool: str
    database: str
    table: str
    lastUpdateFromHive: str | None = None
    sqlWhereAddition: str | None = None
    includeInAirflow: bool | None = None
    airflowPriority: int | None = None
    forceCreateTempTable: bool | None = None
    validateExport: bool | None = None
    validationMethod: str | None = None
    validationCustomQueryHiveSQL: str | None = None
    validationCustomQueryTargetSQL: str | None = None
    uppercaseColumns: int | None = None
    truncateTarget: bool | None = None
    sqlSessions: int | None = None
    tableRowcount: int | None = None
    targetRowcount: int | None = None
    validationCustomQueryHiveValue: str | None = None
    validationCustomQueryTargetValue: str | None = None
    incrColumn: str | None = None
    incrValidationMethod: str | None = None
    incrMinvalue: str | None = None
    incrMaxvalue: str | None = None
    incrMinvaluePending: str | None = None
    incrMaxvaluePending: str | None = None
    sqoopOptions: str | None = None
    lastSize: int | None = None
    lastRows: int | None = None
    lastSqlSessions: int | None = None
    lastExecution: int | None = None
    hiveContainerSize: int | None = None
    createTargetTableSql: str | None = None
    operatorNotes: str | None = None
    columns: list[exportTableColumnsRead] = []


class exportTableDetailsWrite(BaseModel):
    # 	connection: str
    # 	targetSchema: str
    # 	targetTable: str
    exportType: str
    exportTool: str
    database: str
    table: str
    lastUpdateFromHive: str | None = None
    sqlWhereAddition: str | None = None
    includeInAirflow: bool | None = None
    airflowPriority: int | None = None
    forceCreateTempTable: bool | None = None
    validateExport: bool | None = None
    validationMethod: str | None = None
    validationCustomQueryHiveSQL: str | None = None
    validationCustomQueryTargetSQL: str | None = None
    uppercaseColumns: int | None = None
    truncateTarget: bool | None = None
    sqlSessions: int | None = None
    # 	tableRowcount: Union[int, None] = None
    # 	targetRowcount: Union[int, None] = None
    # 	validationCustomQueryHiveValue: Union[str, None] = None
    # 	validationCustomQueryTargetValue: Union[str, None] = None
    incrColumn: str | None = None
    incrValidationMethod: str | None = None
    # 	incrMinvalue: Union[str, None] = None
    # 	incrMaxvalue: Union[str, None] = None
    # 	incrMinvaluePending: Union[str, None] = None
    # 	incrMaxvaluePending: Union[str, None] = None
    sqoopOptions: str | None = None
    # 	lastSize: Union[int, None] = None
    # 	lastRows: Union[int, None] = None
    # 	lastSqlSessions: Union[int, None] = None
    # 	lastExecution: Union[int, None] = None
    hiveContainerSize: int | None = None
    createTargetTableSql: str | None = None
    operatorNotes: str | None = None
    columns: list[exportTableColumnsWrite] = []


class exportTablePK(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str


class exportTableBulkUpdate(BaseModel):
    exportType: str | None = None
    exportTool: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None
    exportTables: list[exportTablePK]


class airflowAllDags(BaseModel):
    name: str
    type: str
    scheduleInterval: str | None = None
    autoRegenerateDag: bool


# 	operatorNotes: Union[str, None] = None
# 	applicationNotes: Union[str, None] = None


class airflowImportDags(BaseModel):
    name: str
    filterTable: str | None = None
    scheduleInterval: str | None = None
    autoRegenerateDag: bool
    airflowLink: str | None = None
    # 	operatorNotes: Union[str, None] = None
    # 	applicationNotes: Union[str, None] = None
    operatorNotes: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None


class airflowExportDags(BaseModel):
    name: str
    filterConnection: str | None = None
    filterTargetSchema: str | None = None
    filterTargetTable: str | None = None
    scheduleInterval: str | None = None
    autoRegenerateDag: bool
    airflowLink: str | None = None
    # 	operatorNotes: Union[str, None] = None
    # 	applicationNotes: Union[str, None] = None
    operatorNotes: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None


class airflowCustomDags(BaseModel):
    name: str
    scheduleInterval: str | None = None
    autoRegenerateDag: bool
    airflowLink: str | None = None
    # 	operatorNotes: Union[str, None] = None
    # 	applicationNotes: Union[str, None] = None
    operatorNotes: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None


class airflowTask(BaseModel):
    name: str
    type: str
    placement: str
    connection: str | None = None
    airflowPool: str | None = None
    airflowPriority: int | None = None
    includeInAirflow: bool
    taskDependencyDownstream: str | None = None
    taskDependencyUpstream: str | None = None
    taskConfig: str | None = None
    sensorPokeInterval: int | None = None
    sensorTimeoutMinutes: int | None = None
    sensorConnection: str | None = None
    sensorSoftFail: int | None = None
    sudoUser: str | None = None


class airflowCustomDag(BaseModel):
    name: str
    scheduleInterval: str | None = None
    retries: int | None = None
    operatorNotes: str | None = None
    applicationNotes: str | None = None
    airflowNotes: str | None = None
    autoRegenerateDag: bool | None = None
    sudoUser: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    tags: str | None = None
    slaWarningTime: str | None = None
    retryExponentialBackoff: int | None = None
    concurrency: int | None = None
    airflowLink: str | None = None
    tasks: list[airflowTask] = []


class airflowExportDag(BaseModel):
    name: str
    scheduleInterval: str | None = None
    filterConnection: str | None = None
    filterTargetSchema: str | None = None
    filterTargetTable: str | None = None
    retries: int | None = None
    operatorNotes: str | None = None
    applicationNotes: str | None = None
    autoRegenerateDag: bool | None = None
    airflowNotes: str | None = None
    sudoUser: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    tags: str | None = None
    slaWarningTime: str | None = None
    retryExponentialBackoff: int | None = None
    concurrency: int | None = None
    airflowLink: str | None = None
    tasks: list[airflowTask] = []


class airflowImportDag(BaseModel):
    name: str
    scheduleInterval: str | None = None
    filterTable: str | None = None
    finishAllStage1First: bool | None = None
    runImportAndEtlSeparate: bool | None = None
    retries: int | None = None
    retriesStage1: int | None = None
    retriesStage2: int | None = None
    poolStage1: str | None = None
    poolStage2: str | None = None
    operatorNotes: str | None = None
    applicationNotes: str | None = None
    autoRegenerateDag: bool | None = None
    airflowNotes: str | None = None
    sudoUser: str | None = None
    metadataImport: bool | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    tags: str | None = None
    slaWarningTime: str | None = None
    retryExponentialBackoff: bool | None = None
    concurrency: int | None = None
    airflowLink: str | None = None
    tasks: list[airflowTask] = []


class airflowDagPK(BaseModel):
    name: str


class airflowDagBulkUpdate(BaseModel):
    scheduleInterval: str | None = None
    autoRegenerateDag: bool | None = None
    operatorNotes: str | None = None
    sudoUser: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None
    dags: list[airflowDagPK]
