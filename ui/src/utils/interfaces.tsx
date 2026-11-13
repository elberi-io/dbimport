import {
  AirflowDAGTaskPlacement,
  AirflowDAGTaskType,
  AnonymizationFunction,
  EtlEngine,
  EtlType,
  ExportIncrValidationMethod,
  ExportTool,
  ExportType,
  ExportValidationMethod,
  ImportTool,
  ImportType,
  IncrMode,
  IncrValidationMethod,
  MergeCompactionMethod,
  SettingType,
  ValidateSource,
  ValidationMethod
} from './enums'

export interface BulkField<T> {
  key: keyof T | 'groupingspace'
  label: string
  type: string
  enumOptions?: { [key: string]: string }
  maxChar?: number
  maxInt?: number
  infoText?: string
  isRequired?: boolean
}

export interface EncryptCredentials {
  connection: string
  username: string
  password: string
}

export interface ConnectionNames {
  name: string
}
export interface Connections extends ConnectionNames {
  connectionString: string
  serverType: string
}

export interface ConnectionSearchFilter {
  name: string | null
  connectionString: string | null
}

export interface UiConnectionSearchFilter {
  name: string | null
  connectionString: string | null
  serverType: string | null
}

export interface GenerateJDBCconnectionString {
  databaseType: string
  version: string
  hostname: string
  port: number | null
  database: string
}

export interface Connection {
  name: string
  connectionString: string
  privateKeyPath: string | null
  publicKeyPath: string | null
  credentials: string | null
  source: string | null
  forceString: number | null
  maxSessions: number | null
  createDatalakeImport: boolean | null
  timeWindowStart: string | null
  timeWindowStop: string | null
  timeWindowTimezone: string | null
  operatorNotes: string | null
  contactInformation: string | null
  description: string | null
  owner: string | null
  environment: string | null
  seedFile: string | null
  createForeignKey: boolean | null
  atlasDiscovery: boolean | null
  atlasIncludeFilter: string | null
  atlasExcludeFilter: string | null
  atlasLastDiscovery: string | null
}

// Table

export interface Database {
  name: string
  tables: number
  lastImport: string
  lastSize: number
  lastRows: number
}

export interface ImportSearchFilter {
  connection: string | null
  database: string | null
  table: string | null
  includeInAirflow: boolean | null
  importPhaseType: string | null
  etlPhaseType: string | null
  importTool: string | null
  etlEngine: string | null
}

export interface UiImportSearchFilter {
  connection: string | null
  database: string | null
  table: string | null
  includeInAirflow: string | null
  lastUpdateFromSource: string | null
  importPhaseType: string | null
  etlPhaseType: string | null
  importTool: string | null
  etlEngine: string | null
}

export interface ImportDiscoverSearch {
  connection: string
  database: string
  schemaFilter: string | null
  tableFilter: string | null
  addSchemaToTable: boolean
  addCounterToTable: boolean
  counterStart: number | null
  addCustomText: string | null
}

export interface UiImportDiscoverSearch {
  connection: string | null // required in search, null for input handling
  database: string | null // required in search, null for input handling
  schemaFilter: string | null
  tableFilter: string | null
  addSchemaToTable: boolean
  addCounterToTable: boolean
  counterStart: number | null
  addCustomText: string | null
}

export interface ImportDiscoverTable {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
}

export interface DbTable {
  connection: string
  database: string
  etlEngine: string
  etlPhaseType: string
  importPhaseType: string
  importTool: string
  lastUpdateFromSource: string
  sourceSchema: string
  sourceTable: string
  table: string
  includeInAirflow: boolean
  operatorNotes: string | null
}

export interface UiDbTable extends DbTable {
  importPhaseTypeDisplay: string
  etlPhaseTypeDisplay: string
  importToolDisplay: string
  etlEngineDisplay: string
}

export interface Table {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: string
  etlPhaseType: string
  importTool: string
  etlEngine: string
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: string
  validateSource: string
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean
  sqlSessions: 0
  softDeleteDuringMerge: boolean
  sourceRowcount: 0
  sourceRowcountIncr: 0
  targetRowcount: 0
  validationCustomQuerySourceValue: string
  validationCustomQueryHiveValue: string
  incrMode: string
  incrColumn: string
  incrValidationMethod: string
  incrMinvalue: string
  incrMaxvalue: string
  incrMinvaluePending: string
  incrMaxvaluePending: string
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  hiveContainerSize: 0
  splitCount: 0
  sparkExecutorMemory: string
  sparkExecutors: 0
  splitByColumn: string
  sqoopCustomQuery: string
  sqoopOptions: string
  lastSize: 0
  lastRows: 0
  lastSqlSessions: 0
  lastExecution: 0
  sqoopUseGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: 0
  comment: string
  generatedHiveColumnDefinition: string
  generatedSqoopQuery: string
  generatedSqoopOptions: string
  generatedPkColumns: string
  datalakeSource: string
  operatorNotes: string
  copyFinished: string
  copySlave: boolean
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: string
  mergeCompactionMethod: string
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface UITable {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: ValidationMethod
  validateSource: ValidateSource
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean // should stay true
  sqlSessions: 0
  softDeleteDuringMerge: boolean
  sourceRowcount: 0
  sourceRowcountIncr: 0
  targetRowcount: 0
  validationCustomQuerySourceValue: string
  validationCustomQueryHiveValue: string
  incrMode: IncrMode
  incrColumn: string
  incrValidationMethod: IncrValidationMethod
  incrMinvalue: string
  incrMaxvalue: string
  incrMinvaluePending: string
  incrMaxvaluePending: string
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  hiveContainerSize: 0
  splitCount: 0
  sparkExecutorMemory: string
  sparkExecutors: 0
  splitByColumn: string
  sqoopCustomQuery: string
  sqoopOptions: string
  lastSize: 0
  lastRows: 0
  lastSqlSessions: 0
  lastExecution: 0
  sqoopUseGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: 0
  comment: string
  generatedHiveColumnDefinition: string
  generatedSqoopQuery: string
  generatedSqoopOptions: string
  generatedPkColumns: string
  datalakeSource: string
  operatorNotes: string
  copyFinished: string
  copySlave: boolean
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: string
  mergeCompactionMethod: MergeCompactionMethod
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface UITableWithoutEnum {
  [key: string]: string | boolean | number | Columns[]
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: string
  etlPhaseType: string
  importTool: string
  etlEngine: string
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: string
  validateSource: string
  validateDiffAllowed: number
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean // should stay true
  sqlSessions: number
  softDeleteDuringMerge: boolean
  sourceRowcount: number
  sourceRowcountIncr: number
  targetRowcount: number
  validationCustomQuerySourceValue: string
  validationCustomQueryHiveValue: string
  incrMode: string
  incrColumn: string
  incrValidationMethod: string
  incrMinvalue: string
  incrMaxvalue: string
  incrMinvaluePending: string
  incrMaxvaluePending: string
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  hiveContainerSize: number
  splitCount: number
  sparkExecutorMemory: string
  sparkExecutors: number
  splitByColumn: string
  sqoopCustomQuery: string
  sqoopOptions: string
  lastSize: number
  lastRows: number
  lastSqlSessions: number
  lastExecution: number
  sqoopUseGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: number
  comment: string
  generatedHiveColumnDefinition: string
  generatedSqoopQuery: string
  generatedSqoopOptions: string
  generatedPkColumns: string
  datalakeSource: string
  operatorNotes: string
  copyFinished: string
  copySlave: boolean
  createForeignKeys: number
  invalidateImpala: number
  customMaxQuery: string
  mergeCompactionMethod: string
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface TableUpdate {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  lastUpdateFromSource: string
  sqlWhereAddition: string
  nomergeIngestionSqlAddition: string
  includeInAirflow: boolean
  airflowPriority: string
  validateImport: boolean
  validationMethod: ValidationMethod
  validateSource: ValidateSource
  validateDiffAllowed: 0
  validationCustomQuerySourceSQL: string
  validationCustomQueryHiveSQL: string
  validationCustomQueryValidateImportTable: boolean // should stay true
  sqlSessions: 0
  softDeleteDuringMerge: boolean
  incrMode: IncrMode
  incrColumn: string
  incrValidationMethod: IncrValidationMethod
  pkColumnOverride: string
  pkColumnOverrideMergeonly: string
  hiveContainerSize: 0
  splitCount: 0
  sparkExecutorMemory: string
  sparkExecutors: 0
  splitByColumn: string
  sqoopCustomQuery: string
  sqoopOptions: string
  sqoopUseGeneratedSql: boolean
  allowTextSplitter: boolean
  forceString: number
  comment: string
  datalakeSource: string
  operatorNotes: string
  createForeignKeys: 0
  invalidateImpala: 0
  customMaxQuery: string
  mergeCompactionMethod: MergeCompactionMethod
  sourceTableType: string
  importDatabase: string
  importTable: string
  historyDatabase: string
  historyTable: string
  columns: Columns[]
}

export interface TableCreateWithoutEnum {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: string
  etlPhaseType: string
  importTool: string
  etlEngine: string
  lastUpdateFromSource: null
  sqlWhereAddition: null
  nomergeIngestionSqlAddition: null
  includeInAirflow: boolean
  airflowPriority: null
  validateImport: null
  validationMethod: null
  validateSource: null
  validateDiffAllowed: null
  validationCustomQuerySourceSQL: null
  validationCustomQueryHiveSQL: null
  validationCustomQueryValidateImportTable: null
  sqlSessions: null
  softDeleteDuringMerge: null
  incrMode: null
  incrColumn: null
  incrValidationMethod: null
  pkColumnOverride: null
  pkColumnOverrideMergeonly: null
  hiveContainerSize: null
  splitCount: null
  sparkExecutorMemory: null
  sparkExecutors: null
  splitByColumn: null
  sqoopCustomQuery: null
  sqoopOptions: null
  sqoopUseGeneratedSql: null
  allowTextSplitter: null
  forceString: null
  comment: null
  datalakeSource: null
  operatorNotes: null
  createForeignKeys: null
  invalidateImpala: null
  customMaxQuery: null
  mergeCompactionMethod: null
  sourceTableType: null
  importDatabase: null
  importTable: null
  historyDatabase: null
  historyTable: null
  columns: Columns[]
}

export interface TableCreate {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  lastUpdateFromSource: null
  sqlWhereAddition: null
  nomergeIngestionSqlAddition: null
  includeInAirflow: boolean
  airflowPriority: null
  validateImport: null
  validationMethod: null
  validateSource: null
  validateDiffAllowed: null
  validationCustomQuerySourceSQL: null
  validationCustomQueryHiveSQL: null
  validationCustomQueryValidateImportTable: null
  sqlSessions: null
  softDeleteDuringMerge: null
  incrMode: null
  incrColumn: null
  incrValidationMethod: null
  pkColumnOverride: null
  pkColumnOverrideMergeonly: null
  hiveContainerSize: null
  splitCount: null
  sparkExecutorMemory: null
  sparkExecutors: null
  splitByColumn: null
  sqoopCustomQuery: null
  sqoopOptions: null
  sqoopUseGeneratedSql: null
  allowTextSplitter: null
  forceString: null
  comment: null
  datalakeSource: null
  operatorNotes: null
  createForeignKeys: null
  invalidateImpala: null
  customMaxQuery: null
  mergeCompactionMethod: null
  sourceTableType: null
  importDatabase: null
  importTable: null
  historyDatabase: null
  historyTable: null
  columns: Columns[]
}

export type TableCreateMapped = {
  [K in keyof Omit<TableCreate, 'columns'>]: TableCreate[K]
} & {
  columns: Columns[]
}

export interface HeadersRowInfo {
  contentLength: string
  contentMaxReturnedRows: string
  contentRows: string
  contentTotalRows: string
}

export interface Column<T> {
  header: string
  accessor?: keyof T
  isAction?:
    | 'edit'
    | 'delete'
    | 'editAndDelete'
    | 'generateDag'
    | 'generateDagAndRename'
    | 'repair'
    | 'reset'
    | 'copy'
    | 'rename'
    | 'repairAndResetAndRenameAndCopy'
    | 'copyAndDelete'
    | 'testAndCopyAndEncryptAndDelete'
  isLink?: 'connectionLink' | 'airflowLink'
}

// Import table Columns, it is different on Export
export interface Columns {
  columnName: string
  columnOrder: string | null
  sourceColumnName: string
  columnType: string
  sourceColumnType: string
  sourceDatabaseType: string | null
  columnNameOverride: string | null
  columnTypeOverride: string | null
  sqoopColumnType: string | null
  sqoopColumnTypeOverride: string | null
  forceString: number
  includeInImport: boolean
  sourcePrimaryKey: string | null
  lastUpdateFromSource: string
  comment: string | null
  operatorNotes: string | null
  anonymizationFunction: string
}

export type EnumTypes =
  | ImportType
  | EtlType
  | ImportTool
  | EtlEngine
  | ValidationMethod
  | ValidateSource
  | IncrMode
  | IncrValidationMethod
  | MergeCompactionMethod
  | AnonymizationFunction
  | AirflowDAGTaskType
  | AirflowDAGTaskPlacement
  | ExportType
  | ExportTool

export type EditSettingValueTypes = string | number | boolean | EnumTypes

export interface EditSetting {
  label: string
  value: EditSettingValueTypes | null
  type: SettingType
  enumOptions?: {
    [key: string]: string
  }
  maxChar?: number
  maxInt?: number
  infoText?: string
  isConditionsMet?: boolean
  isHidden?: boolean
  isRequired?: boolean
}

export interface TableCreate {
  database: string
  table: string
  connection: string
  sourceSchema: string
  sourceTable: string
  importPhaseType: ImportType
  etlPhaseType: EtlType
  importTool: ImportTool
  etlEngine: EtlEngine
  includeInAirflow: boolean
}

export interface ImportPKs {
  database: string
  table: string
}

export interface BulkUpdateImportTables {
  connection?: string | null
  importPhaseType?: string | null
  etlPhaseType?: string | null
  importTool?: string | null
  etlEngine?: string | null
  includeInAirflow?: boolean | null
  operatorNotes?: string | null
  importTables: ImportPKs[]
}

// Export

export interface ExportConnections {
  name: string
  tables: number
  lastExport: string | null
  lastRows: number | null
}

export interface ExportCnTablesWithoutEnum {
  connection: string
  targetSchema: string
  targetTable: string
  database: string
  table: string
  exportType: string
  exportTool: string
  lastUpdateFromHive: string | null
  includeInAirflow: boolean
  operatorNotes: string | null
}

export interface ExportCnTable {
  connection: string
  targetSchema: string
  targetTable: string
  database: string
  table: string
  exportType: ExportType
  exportTool: ExportTool
  lastUpdateFromHive: string | null
  includeInAirflow: boolean
  operatorNotes: string | null
}
export interface UIExportCnTables extends ExportCnTablesWithoutEnum {
  exportTypeDisplay: string
  exportToolDisplay: string
}

export interface ExportSearchFilter {
  connection: string | null
  targetSchema: string | null
  targetTable: string | null
  includeInAirflow: boolean | null
  exportType: string | null
  exportTool: string | null
}

export interface UiExportSearchFilter {
  connection: string | null
  targetSchema: string | null
  targetTable: string | null
  includeInAirflow: string | null
  lastUpdateFromHive: string | null
  exportType: string | null
  exportTool: string | null
}

export interface ExportDiscoverSearch {
  connection: string
  targetSchema: string
  databaseFilter: string | null
  tableFilter: string | null
  addDBToTable: boolean
  addCounterToTable: boolean
  counterStart: number | null
  addCustomText: string | null
}

export interface UiExportDiscoverSearch {
  connection: string | null // required in search, null for input handling
  targetSchema: string | null // required in search, null for input handling
  databaseFilter: string | null
  tableFilter: string | null
  addDBToTable: boolean
  addCounterToTable: boolean
  counterStart: number | null
  addCustomText: string | null
}

export interface ExportDiscoverTable {
  database: string
  table: string
  connection: string
  targetSchema: string
  targetTable: string
}

export interface ExportTable {
  connection: string
  targetSchema: string
  targetTable: string
  exportType: ExportType
  exportTool: ExportTool
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: string | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  sqlSessions: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: string | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastSqlSessions: number | null
  lastExecution: number | null
  hiveContainerSize: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

export interface ExportColumns {
  columnName: string
  columnType: string
  columnOrder: number | null
  targetColumnName: string | null
  targetColumnType: string | null
  lastUpdateFromHive: string | null
  includeInExport: boolean
  comment: string | null
  operatorNotes: string | null
}

export interface UIExportTable {
  connection: string
  targetSchema: string
  targetTable: string
  exportType: ExportType
  exportTool: ExportTool
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: ExportValidationMethod | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  sqlSessions: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: ExportIncrValidationMethod | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastSqlSessions: number | null
  lastExecution: number | null
  hiveContainerSize: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

export interface UIExportTableWithoutEnum {
  [key: string]: string | boolean | number | null | ExportColumns[]
  connection: string
  targetSchema: string
  targetTable: string
  exportType: string
  exportTool: string
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: string | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  sqlSessions: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: string | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastSqlSessions: number | null
  lastExecution: number | null
  hiveContainerSize: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

export interface ExportTableCreateWithoutEnum {
  connection: string
  targetSchema: string
  targetTable: string
  exportType: string
  exportTool: string
  database: string
  table: string
  lastUpdateFromHive: string | null
  sqlWhereAddition: string | null
  includeInAirflow: boolean | null
  airflowPriority: number | null
  forceCreateTempTable: boolean | null
  validateExport: boolean | null
  validationMethod: string | null
  validationCustomQueryHiveSQL: string | null
  validationCustomQueryTargetSQL: string | null
  uppercaseColumns: number | null
  truncateTarget: boolean | null
  sqlSessions: number | null
  tableRowcount: number | null
  targetRowcount: number | null
  validationCustomQueryHiveValue: string | null
  validationCustomQueryTargetValue: string | null
  incrColumn: string | null
  incrValidationMethod: string | null
  incrMinvalue: string | null
  incrMaxvalue: string | null
  incrMinvaluePending: string | null
  incrMaxvaluePending: string | null
  sqoopOptions: string | null
  lastSize: number | null
  lastRows: number | null
  lastSqlSessions: number | null
  lastExecution: number | null
  hiveContainerSize: number | null
  createTargetTableSql: string | null
  operatorNotes: string | null
  columns: ExportColumns[]
}

export interface ExportPKs {
  connection: string
  targetSchema: string
  targetTable: string
}

export interface BulkUpdateExportTables {
  exportType?: string | null
  exportTool?: string | null
  includeInAirflow?: boolean | null
  operatorNotes?: string | null
  exportTables: ExportPKs[]
}

// Airflow

export interface UiAirflowsSearchFilter {
  name: string | null
  scheduleInterval: string | null
  autoRegenerateDag: string | null
}

export interface BaseAirflowsData {
  name: string
  scheduleInterval: string
  autoRegenerateDag: boolean
  airflowLink: string | null
  timezone: string | null
  email: string | null
  emailOnFailure: boolean | null
  emailOnRetries: boolean | null
  concurrency: number | null
  slaWarningTime: string | null
  operatorNotes: string | null
}

export interface AirflowsData extends BaseAirflowsData {
  type: string
  operatorNotes: string
  applicationNotes: string
}

export interface AirflowsImportData extends BaseAirflowsData {
  filterTable: string
}

export interface UiAirflowsImportData extends AirflowsImportData {
  autoRegenerateDagDisplay: string
}

export interface AirflowsExportData extends BaseAirflowsData {
  filterConnection: string
  filterTargetSchema: string
  filterTargetTable: string
}

export interface UiAirflowsExportData extends AirflowsExportData {
  autoRegenerateDagDisplay: string
}

export type AirflowsCustomData = BaseAirflowsData

export interface UiAirflowsCustomData extends AirflowsCustomData {
  autoRegenerateDagDisplay: string
}

export type AirflowWithDynamicKeys<T> = T & {
  [key: string]: string | boolean | number | AirflowTask[]
}

export interface BaseAirflowDAG {
  name: string
  scheduleInterval: string
  retries: number
  operatorNotes: string
  applicationNotes: string
  autoRegenerateDag: boolean
  airflowNotes: string
  sudoUser: string
  timezone: string
  email: string
  emailOnFailure: boolean
  emailOnRetries: boolean
  tags: string
  slaWarningTime: string
  retryExponentialBackoff: boolean
  concurrency: number
  airflowLink: string | null
  tasks: AirflowTask[]
}

export interface ImportAirflowDAG extends BaseAirflowDAG {
  filterTable: string
  finishAllStage1First: boolean
  runImportAndEtlSeparate: boolean
  retriesStage1: number
  retriesStage2: number
  poolStage1: string
  poolStage2: string
  metadataImport: boolean
}

export interface ExportAirflowDAG extends BaseAirflowDAG {
  filterConnection: string
  filterTargetSchema: string
  filterTargetTable: string
}

export type CustomAirflowDAG = BaseAirflowDAG

export interface AirflowTask {
  name: string
  type: string
  placement: string
  connection: string | null
  airflowPool: string | null
  airflowPriority: number | null
  includeInAirflow: boolean
  taskDependencyDownstream: string | null
  taskDependencyUpstream: string | null
  taskConfig: string | null
  sensorPokeInterval: number | null
  sensorTimeoutMinutes: number | null
  sensorConnection: string | null
  sensorSoftFail: number | null
  sudoUser: string | null
}

export interface BaseCreateAirflowDAG {
  name: string
  scheduleInterval: string | null
  retries: number | null
  operatorNotes: string | null
  applicationNotes: string | null
  autoRegenerateDag: boolean
  airflowNotes: string | null
  sudoUser: string | null
  timezone: string | null
  email: string | null
  emailOnFailure: boolean | null
  emailOnRetries: boolean | null
  tags: string | null
  slaWarningTime: string | null
  retryExponentialBackoff: boolean | null
  concurrency: number | null
  tasks: AirflowTask[]
}

export interface ImportCreateAirflowDAG extends BaseCreateAirflowDAG {
  filterTable: string | null
  finishAllStage1First: boolean | null
  runImportAndEtlSeparate: boolean | null
  retriesStage1: number | null
  retriesStage2: number | null
  poolStage1: string | null
  poolStage2: string | null
  metadataImport: boolean | null
}

export interface ExportCreateAirflowDAG extends BaseCreateAirflowDAG {
  filterConnection: string | null
  filterTargetSchema: string | null
  filterTargetTable: string | null
}

export type CustomCreateAirflowDAG = BaseCreateAirflowDAG

export interface AirflowDagPk {
  name: string
}

export interface BulkUpdateAirflowDAG {
  scheduleInterval?: string | null
  autoRegenerateDag?: boolean | null
  operatorNotes?: string | null
  sudoUser?: string | null
  timezone?: string | null
  email?: string | null
  emailOnFailure?: boolean | null
  emailOnRetries?: boolean | null
  slaWarningTime?: string | null
  concurrency?: number | null
  dags: AirflowDagPk[]
}

export interface UiBulkAirflowDAG extends Omit<BulkUpdateAirflowDAG, 'dags'> {
  name: string
}

// Configuration

export interface ConfigGlobal {
  airflow_aws_instanceids: string | null
  airflow_aws_pool_to_instanceid: boolean | null
  airflow_create_pool_with_task: boolean | null
  airflow_dag_directory: string | null
  airflow_dag_file_group: string | null
  airflow_dag_file_permission: string | null
  airflow_dag_staging_directory: string | null
  airflow_dbimport_commandpath: string | null
  airflow_default_pool_size: number | null
  airflow_disable: boolean | null
  airflow_dummy_task_queue: string | null
  airflow_major_version: number | null
  airflow_sudo_user: string | null
  airflow_url: string | null
  atlas_discovery_interval: number | null
  cluster_name: string | null
  export_default_sessions: number | null
  export_max_sessions: number | null
  export_stage_disable: boolean | null
  export_staging_database: string | null
  export_start_disable: boolean | null
  hdfs_address: string | null
  hdfs_basedir: string | null
  hdfs_blocksize: string | null
  hive_acid_with_clusteredby: boolean | null
  hive_insert_only_tables: boolean | null
  hive_major_compact_after_merge: boolean | null
  hive_print_messages: boolean | null
  hive_remove_locks_by_force: boolean | null
  hive_validate_before_execution: boolean | null
  hive_validate_table: string | null
  impala_invalidate_metadata: boolean | null
  import_columnname_delete: string | null
  import_columnname_histtime: string | null
  import_columnname_import: string | null
  import_columnname_insert: string | null
  import_columnname_iud: string | null
  import_columnname_source: string | null
  import_columnname_update: string | null
  import_default_sessions: number | null
  import_history_database: string | null
  import_history_table: string | null
  import_max_sessions: number | null
  import_process_empty: boolean | null
  import_stage_disable: boolean | null
  import_staging_database: string | null
  import_staging_table: string | null
  import_start_disable: boolean | null
  import_work_database: string | null
  import_work_table: string | null
  kafka_brokers: string | null
  kafka_saslmechanism: string | null
  kafka_securityprotocol: string | null
  kafka_topic: string | null
  kafka_trustcafile: string | null
  post_airflow_dag_operations: boolean | null
  post_data_to_kafka_extended: boolean | null
  post_data_to_kafka: boolean | null
  post_data_to_rest_extended: boolean | null
  post_data_to_rest: boolean | null
  post_data_to_awssns_extended: boolean | null
  post_data_to_awssns: boolean | null
  post_data_to_awssns_topic: string | null
  restserver_admin_user: string | null
  restserver_authentication_method: string | null
  restserver_token_ttl: number | null
  rest_timeout: number | null
  rest_trustcafile: string | null
  rest_url: string | null
  rest_verifyssl: number | null
  spark_max_executors: number | null
  timezone: string | null
  timestamp_with_timezone: boolean | null
}

export interface ConfigGlobalWithIndex {
  [key: string]: string | boolean | number | null
  airflow_aws_instanceids: string | null
  airflow_aws_pool_to_instanceid: boolean | null
  airflow_create_pool_with_task: boolean | null
  airflow_dag_directory: string | null
  airflow_dag_file_group: string | null
  airflow_dag_file_permission: string | null
  airflow_dag_staging_directory: string | null
  airflow_dbimport_commandpath: string | null
  airflow_default_pool_size: number | null
  airflow_disable: boolean | null
  airflow_dummy_task_queue: string | null
  airflow_major_version: number | null
  airflow_sudo_user: string | null
  atlas_discovery_interval: number | null
  cluster_name: string | null
  export_default_sessions: number | null
  export_max_sessions: number | null
  export_stage_disable: boolean | null
  export_staging_database: string | null
  export_start_disable: boolean | null
  hdfs_address: string | null
  hdfs_basedir: string | null
  hdfs_blocksize: string | null
  hive_acid_with_clusteredby: boolean | null
  hive_insert_only_tables: boolean | null
  hive_major_compact_after_merge: boolean | null
  hive_print_messages: boolean | null
  hive_remove_locks_by_force: boolean | null
  hive_validate_before_execution: boolean | null
  hive_validate_table: string | null
  impala_invalidate_metadata: boolean | null
  import_columnname_delete: string | null
  import_columnname_histtime: string | null
  import_columnname_import: string | null
  import_columnname_insert: string | null
  import_columnname_iud: string | null
  import_columnname_source: string | null
  import_columnname_update: string | null
  import_default_sessions: number | null
  import_history_database: string | null
  import_history_table: string | null
  import_max_sessions: number | null
  import_process_empty: boolean | null
  import_stage_disable: boolean | null
  import_staging_database: string | null
  import_staging_table: string | null
  import_start_disable: boolean | null
  import_work_database: string | null
  import_work_table: string | null
  kafka_brokers: string | null
  kafka_saslmechanism: string | null
  kafka_securityprotocol: string | null
  kafka_topic: string | null
  kafka_trustcafile: string | null
  post_airflow_dag_operations: boolean | null
  post_data_to_kafka_extended: boolean | null
  post_data_to_kafka: boolean | null
  post_data_to_rest_extended: boolean | null
  post_data_to_rest: boolean | null
  post_data_to_awssns_extended: boolean | null
  post_data_to_awssns: boolean | null
  post_data_to_awssns_topic: string | null
  restserver_admin_user: string | null
  restserver_authentication_method: string | null
  restserver_token_ttl: number | null
  rest_timeout: number | null
  rest_trustcafile: string | null
  rest_url: string | null
  rest_verifyssl: number | null
  spark_max_executors: number | null
  timezone: string | null
}

export interface JDBCdrivers {
  databaseType: string
  version: string
  driver: string
  classpath: string
}

export interface JDBCdriversWithIndex {
  [key: string]: string | boolean | number | null
  databaseType: string
  version: string
  driver: string
  classpath: string
}

export interface ErrorDetail {
  input: string | null
  loc: string[]
  msg: string | null
  type: string | null
  url: string | null
}
export interface ErrorData {
  result?: string
  detail?: ErrorDetail[]
}
