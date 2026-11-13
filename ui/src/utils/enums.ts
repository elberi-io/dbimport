// Setting type

export enum SettingType {
  Boolean = 'boolean',
  BooleanNumber = 'booleanNumber',
  BooleanNumberOrAuto = 'booleanNumberOrAuto',
  BooleanOrDefaultFromConfig = 'booleanOrDefaultFromConfig(-1)',
  BooleanOrDefaultFromConnection = 'booleanOrDefaultFromConnection(-1)',
  Readonly = 'readonly',
  Text = 'text',
  TextTripletOctalValue = 'textTripletOctalValue',
  Textarea = 'textarea',
  Enum = 'enum',
  DataReference = 'dataReference',
  DataReferenceRequired = 'dataReferenceRequired',
  Version = 'version',
  Hidden = 'hidden', // Needed here or handled in other way?
  IntegerOneOrTwo = 'integerOneOrTwo',
  IntegerFromZero = 'integerFromZero',
  IntegerFromOne = 'integerFromOne',
  IntegerFromZeroOrNull = 'integerFromZeroOrNull',
  IntegerFromOneOrNull = 'integerFromOneOrNull',
  IntegerFromZeroOrAuto = 'integerFromZeroOrAuto(-1)',
  IntegerFromOneOrAuto = 'integerFromOneOrAuto(-1)',
  IntegerFromOneOrDefaultFromConfig = 'integerFromOneOrDefaultFromConfig(null)',
  Password = 'password',
  Time = 'time',
  TimeZone = 'timezone',
  Email = 'email',
  GroupingSpace = 'groupingSpace'
}

// Import Table

export enum ImportType {
  Full = 'full',
  Incremental = 'incr',
  OracleFlashback = 'oracle_flashback',
  MSSQLChangeTracking = 'mssql_change_tracking'
}

export enum EtlType {
  TruncateAndInsert = 'truncate_insert',
  InsertOnly = 'insert',
  Merge = 'merge',
  MergeHistoryAudit = 'merge_history_audit',
  External = 'external',
  None = 'none'
}

export enum ImportTool {
  Spark = 'spark',
  Sqoop = 'sqoop'
}

export enum EtlEngine {
  Hive = 'hive',
  Spark = 'spark'
}

export enum ValidationMethod {
  CustomQuery = 'customQuery',
  RowCount = 'rowCount'
}

export enum ValidateSource {
  Query = 'query',
  Sqoop = 'sqoop'
}

export enum IncrMode {
  Append = 'append',
  LastModified = 'lastmodified'
}

export enum IncrValidationMethod {
  Full = 'full',
  Incremental = 'incr'
}

export enum MergeCompactionMethod {
  Default = 'default',
  None = 'none',
  Minor = 'minor',
  MinorAndWait = 'minor_and_wait',
  Major = 'major',
  MajorAndWait = 'major_and_wait'
}

export enum AnonymizationFunction {
  None = 'None',
  Hash = 'Hash',
  ReplaceWithStar = 'Replace with star',
  ShowFirst4Chars = 'Show first 4 chars'
}

export enum AirflowDAGTaskType {
  ShellScript = 'shell script',
  HiveSQL = 'Hive SQL',
  HiveSQLScript = 'Hive SQL Script',
  JDBCSQL = 'JDBC SQL',
  TriggerDAG = 'Trigger DAG',
  DAGSensor = 'DAG Sensor',
  SQLSensor = 'SQL Sensor',
  DBImportCommand = 'DBImport command'
}

export enum AirflowDAGTaskPlacement {
  BeforeMain = 'before main',
  AfterMain = 'after main',
  InMain = 'in main'
}

// Export Table

export enum ExportType {
  Full = 'full',
  Incremental = 'incr'
}

export enum ExportTool {
  Spark = 'spark',
  Sqoop = 'sqoop'
}

export enum ExportValidationMethod {
  CustomQuery = 'customQuery',
  RowCount = 'rowCount'
}

export enum ExportIncrValidationMethod {
  Full = 'full',
  Incremental = 'incr'
}

// Configuration Global

export enum RestserverAuthenticationMethod {
  Local = 'local',
  Pam = 'pam'
}
