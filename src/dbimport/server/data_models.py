from typing import TYPE_CHECKING

from pydantic import BaseModel, create_model

from dbimport.config_db import config_keys


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


class ChangeUser(BaseModel):
    disabled: bool | None = None
    fullname: str | None = None
    department: str | None = None
    email: str | None = None


class ChangePassword(BaseModel):
    old_password: str | None = None
    new_password: str


class Status(BaseModel):
    status: str
    version: str


class ImportDBs(BaseModel):
    name: str
    tables: int
    lastImport: str | None = None
    lastSize: int | None = None
    lastRows: int | None = None


# class testConnectionResponse(BaseModel):
# 	status: str


class ConnectionSearch(BaseModel):
    name: str | None = None
    connectionString: str | None = None
    timeWindowTimezone: str | None = None
    operatorNotes: str | None = None
    contactInformation: str | None = None
    description: str | None = None
    owner: str | None = None


class Connection(BaseModel):
    name: str
    connectionString: str | None = None
    serverType: str | None = None


class ConnectionDetails(BaseModel):
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


class EncryptCredential(BaseModel):
    connection: str
    username: str
    password: str


class GenerateJDBCConnectionString(BaseModel):
    databaseType: str
    version: str
    hostname: str
    port: int | None = None
    database: str


class GeneratedJDBCConnectionString(BaseModel):
    connectionString: str


class JDBCDriver(BaseModel):
    databaseType: str
    version: str
    driver: str
    classpath: str


if TYPE_CHECKING:
    # The fields are created at run time; mypy only knows the base class.
    Configuration = BaseModel
else:
    Configuration = create_model(
        "Configuration",
        **{
            key.name: (key.python_type | None, None)
            for key in config_keys.KEYS
            if key.rest
        },
    )


class DiscoverImportTable(BaseModel):
    database: str
    table: str
    connection: str
    sourceSchema: str
    sourceTable: str


class DiscoverImportTableOptions(BaseModel):
    database: str
    connection: str
    schemaFilter: str | None
    tableFilter: str | None
    addSchemaToTable: bool
    addCounterToTable: bool
    counterStart: int | None
    addCustomText: str | None


class ImportTable(BaseModel):
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


class ImportTableColumnsRead(BaseModel):
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


class ImportTableColumnsWrite(BaseModel):
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


class ImportTableDetailsRead(BaseModel):
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
    columns: list[ImportTableColumnsRead] = []


class ImportTableDetailsWrite(BaseModel):
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
    columns: list[ImportTableColumnsWrite] = []


class ImportTableSearch(BaseModel):
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


class ImportTablePK(BaseModel):
    database: str
    table: str


class ImportTableBulkUpdate(BaseModel):
    connection: str | None = None
    importPhaseType: str | None = None
    etlPhaseType: str | None = None
    importTool: str | None = None
    etlEngine: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None
    importTables: list[ImportTablePK]


class DefaultResultResponse(BaseModel):
    result: str


class ExportConnections(BaseModel):
    name: str
    tables: int
    lastExport: str | None = None
    lastRows: int | None = None


class ExportTableSearch(BaseModel):
    connection: str | None = None
    targetSchema: str | None = None
    targetTable: str | None = None
    database: str | None = None
    table: str | None = None
    exportType: str | None = None
    exportTool: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None


class DiscoverExportTable(BaseModel):
    database: str
    table: str
    connection: str
    targetSchema: str
    targetTable: str


class DiscoverExportTableOptions(BaseModel):
    connection: str
    targetSchema: str
    databaseFilter: str | None
    tableFilter: str | None
    addDBToTable: bool
    addCounterToTable: bool
    counterStart: int | None
    addCustomText: str | None


class ExportTable(BaseModel):
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


class ExportTableColumnsRead(BaseModel):
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


class ExportTableColumnsWrite(BaseModel):
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


class ExportTableDetailsRead(BaseModel):
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
    columns: list[ExportTableColumnsRead] = []


class ExportTableDetailsWrite(BaseModel):
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
    # 	tableRowcount: int | None = None
    # 	targetRowcount: int | None = None
    # 	validationCustomQueryHiveValue: str | None = None
    # 	validationCustomQueryTargetValue: str | None = None
    incrColumn: str | None = None
    incrValidationMethod: str | None = None
    # 	incrMinvalue: str | None = None
    # 	incrMaxvalue: str | None = None
    # 	incrMinvaluePending: str | None = None
    # 	incrMaxvaluePending: str | None = None
    sqoopOptions: str | None = None
    # 	lastSize: int | None = None
    # 	lastRows: int | None = None
    # 	lastSqlSessions: int | None = None
    # 	lastExecution: int | None = None
    hiveContainerSize: int | None = None
    createTargetTableSql: str | None = None
    operatorNotes: str | None = None
    columns: list[ExportTableColumnsWrite] = []


class ExportTablePK(BaseModel):
    connection: str
    targetSchema: str
    targetTable: str


class ExportTableBulkUpdate(BaseModel):
    exportType: str | None = None
    exportTool: str | None = None
    includeInAirflow: bool | None = None
    operatorNotes: str | None = None
    exportTables: list[ExportTablePK]


class AirflowAllDAGs(BaseModel):
    name: str
    type: str
    scheduleInterval: str | None = None
    autoRegenerateDag: bool


# 	operatorNotes: str | None = None
# 	applicationNotes: str | None = None


class AirflowImportDAGs(BaseModel):
    name: str
    filterTable: str | None = None
    scheduleInterval: str | None = None
    autoRegenerateDag: bool
    airflowLink: str | None = None
    # 	operatorNotes: str | None = None
    # 	applicationNotes: str | None = None
    operatorNotes: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None


class AirflowExportDAGs(BaseModel):
    name: str
    filterConnection: str | None = None
    filterTargetSchema: str | None = None
    filterTargetTable: str | None = None
    scheduleInterval: str | None = None
    autoRegenerateDag: bool
    airflowLink: str | None = None
    # 	operatorNotes: str | None = None
    # 	applicationNotes: str | None = None
    operatorNotes: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None


class AirflowCustomDAGs(BaseModel):
    name: str
    scheduleInterval: str | None = None
    autoRegenerateDag: bool
    airflowLink: str | None = None
    # 	operatorNotes: str | None = None
    # 	applicationNotes: str | None = None
    operatorNotes: str | None = None
    timezone: str | None = None
    email: str | None = None
    emailOnFailure: bool | None = None
    emailOnRetries: bool | None = None
    slaWarningTime: str | None = None
    concurrency: int | None = None


class AirflowTask(BaseModel):
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


class AirflowCustomDAG(BaseModel):
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
    tasks: list[AirflowTask] = []


class AirflowExportDAG(BaseModel):
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
    tasks: list[AirflowTask] = []


class AirflowImportDAG(BaseModel):
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
    tasks: list[AirflowTask] = []


class AirflowDAGPK(BaseModel):
    name: str


class AirflowDAGBulkUpdate(BaseModel):
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
    dags: list[AirflowDAGPK]
