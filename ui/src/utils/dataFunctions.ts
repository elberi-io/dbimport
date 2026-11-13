import {
  EtlEngine,
  EtlType,
  ExportTool,
  ExportType,
  ImportTool,
  ImportType
} from './enums'
import {
  UITable,
  EditSetting,
  TableCreateWithoutEnum,
  Connection,
  Columns,
  UITableWithoutEnum,
  BaseAirflowDAG,
  ExportAirflowDAG,
  ImportAirflowDAG,
  AirflowTask,
  CustomAirflowDAG,
  AirflowWithDynamicKeys,
  ImportCreateAirflowDAG,
  CustomCreateAirflowDAG,
  ExportCreateAirflowDAG,
  UIExportTable,
  ExportColumns,
  UIExportTableWithoutEnum,
  ExportTableCreateWithoutEnum,
  ConfigGlobal,
  ConfigGlobalWithIndex,
  JDBCdriversWithIndex,
  JDBCdrivers,
  GenerateJDBCconnectionString,
  EncryptCredentials
} from './interfaces'
import {
  getKeyFromCustomAirflowLabel,
  getKeyFromExportAirflowLabel,
  getKeyFromExportLabel,
  getKeyFromGlobalConfigLabel,
  getKeyFromImportAirflowLabel,
  getKeyFromJDBCdriversLabel,
  // getKeyFromColumnLabel,
  // getKeyFromConnectionLabel,
  getKeyFromLabel,
  reverseMapEnumValue
} from './nameMappings'

import cloneDeep from 'lodash/cloneDeep'

// Connection

export const transformEncryptCredentialsSettings = (
  settings: EditSetting[]
): EncryptCredentials => {
  const settingsMap = settings.reduce<
    Record<string, string | number | boolean | null>
  >((acc, setting) => {
    acc[setting.label] = setting.value
    return acc
  }, {})

  return {
    connection: settingsMap['Connection'] || 'unknown',
    username: settingsMap['Username'] || 'default',
    password: settingsMap['Password'] || 'unknown'
  } as EncryptCredentials
}

export function createConnectionData(
  newConnectionSettings: EditSetting[]
): Connection {
  // Part 1: Keys that will get values from newConnectionSettings
  const part1: {
    name: string
    connectionString: string
    operatorNotes: string | null
    contactInformation: string | null
    description: string | null
    owner: string | null
  } = {
    name: 'unknown',
    connectionString: '',
    operatorNotes: null,
    contactInformation: null,
    description: null,
    owner: null
  }

  // Part 2: Default null values for all other fields in Connection
  const part2: Omit<Connection, keyof typeof part1> = {
    privateKeyPath: null,
    publicKeyPath: null,
    credentials: null,
    source: null,
    forceString: null,
    maxSessions: null,
    createDatalakeImport: null,
    timeWindowStart: null,
    timeWindowStop: null,
    timeWindowTimezone: null,
    seedFile: null,
    createForeignKey: null,
    atlasDiscovery: null,
    atlasIncludeFilter: null,
    atlasExcludeFilter: null,
    atlasLastDiscovery: null,
    environment: null
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    Name: 'name',
    'Connection string': 'connectionString',
    'Operator notes': 'operatorNotes',
    'Contact information': 'contactInformation',
    Description: 'description',
    Owner: 'owner'
  }

  const filteredSettings = newConnectionSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    const value = setting.value
    const key = labelToKeyMap[setting.label]

    if (key) {
      if (typeof value === 'string' || value === null) {
        ;(part1[key] as (typeof part1)[typeof key]) = value
      }
    }
  })

  const finalConnectionData: Connection = { ...part2, ...part1 }

  return finalConnectionData
}

export function updateConnectionData(
  originalData: Connection,
  updatedSettings: EditSetting[]
): Connection {
  // Part 1: Keys that will get values from updatedSettings
  const part1: {
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
    seedFile: string | null
    createForeignKey: boolean | null
    atlasDiscovery: boolean | null
    atlasIncludeFilter: string | null
    atlasExcludeFilter: string | null
    atlasLastDiscovery: string | null
  } = {
    connectionString: originalData.connectionString,
    privateKeyPath: originalData.privateKeyPath,
    publicKeyPath: originalData.publicKeyPath,
    credentials: originalData.credentials,
    source: originalData.source,
    forceString: originalData.forceString,
    maxSessions: originalData.maxSessions,
    createDatalakeImport: originalData.createDatalakeImport,
    timeWindowStart: originalData.timeWindowStart,
    timeWindowStop: originalData.timeWindowStop,
    timeWindowTimezone: originalData.timeWindowTimezone,
    operatorNotes: originalData.operatorNotes,
    contactInformation: originalData.contactInformation,
    description: originalData.description,
    owner: originalData.owner,
    seedFile: originalData.seedFile,
    createForeignKey: originalData.createForeignKey,
    atlasDiscovery: originalData.atlasDiscovery,
    atlasIncludeFilter: originalData.atlasIncludeFilter,
    atlasExcludeFilter: originalData.atlasExcludeFilter,
    atlasLastDiscovery: originalData.atlasLastDiscovery
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<Connection, keyof typeof part1> = {
    name: originalData.name,
    environment: originalData.environment
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    'Connection string': 'connectionString',
    'Private Key Path': 'privateKeyPath',
    'Public Key Path': 'publicKeyPath',
    Credentials: 'credentials',
    Source: 'source',
    'Force String': 'forceString',
    'Max Sessions': 'maxSessions',
    'Create Datalake Import': 'createDatalakeImport',
    'Time Window Start': 'timeWindowStart',
    'Time Window Stop': 'timeWindowStop',
    'Time Window Timezone': 'timeWindowTimezone',
    'Operator notes': 'operatorNotes',
    'Contact information': 'contactInformation',
    Description: 'description',
    Owner: 'owner',
    'Seed File': 'seedFile',
    'Create Foreign Key': 'createForeignKey',
    'Atlas Discovery': 'atlasDiscovery',
    'Atlas Include Filter': 'atlasIncludeFilter',
    'Atlas Exclude Filter': 'atlasExcludeFilter',
    'Atlas Last Discovery': 'atlasLastDiscovery'
  }

  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    const value = setting.value

    // To do: use getKeyFromConnectionLabel instead of labelToKeyMap if that is better
    // const key = getKeyFromConnectionLabel(setting.label)
    const key = labelToKeyMap[setting.label]

    if (key) {
      if (
        typeof value === 'string' ||
        typeof value === 'number' ||
        typeof value === 'boolean' ||
        value === null
      ) {
        ;(part1[key] as (typeof part1)[typeof key]) = value
      }
    }
  })

  if (part1.credentials === '') {
    part1.credentials = null
  }

  // To do: make generic conversion of empty string to null, WIP:
  // Object.keys(part1).forEach((key) => {
  //   const typedKey = key as keyof typeof part1;
  //   type CanBeNull = null extends typeof part1[typeof typedKey] ? true : false;

  //   if (part1[typedKey] === ''  && (true as CanBeNull)) {
  //     part1[typedKey] = null;
  //   }
  // });

  const finalConnectionData: Connection = { ...part2, ...part1 }

  return finalConnectionData
}

// Copy connection

export function newCopyCnData(cnName: string, cnData: Connection) {
  // Creates a copy of the existing cnData to avoid mutations
  const cnDataCopy = cloneDeep(cnData)

  const updatedDagData = { ...cnDataCopy, name: cnName }
  return updatedDagData
}

// Table

// Not edited read-only fields
const fieldsToRemove = [
  'sourceRowcount',
  'sourceRowcountIncr',
  'targetRowcount',
  'validationCustomQueryHiveValue',
  'validationCustomQuerySourceValue',
  'incrMinvalue',
  'incrMaxvalue',
  'incrMinvaluePending',
  'incrMaxvaluePending',
  'lastSize',
  'lastRows',
  'lastSqlSessions',
  'lastExecution',
  'generatedHiveColumnDefinition',
  'generatedSqoopQuery',
  'generatedSqoopOptions',
  'generatedPkColumns',
  'copyFinished'
]

function updateColumnData(
  tableData: UITableWithoutEnum,
  setting: EditSetting,
  indexInColumns: number
) {
  if (!tableData.columns) return

  const currentColumn = tableData.columns[indexInColumns]

  const part1: {
    columnNameOverride: string | null
    columnTypeOverride: string | null
    sqoopColumnTypeOverride: string | null
    forceString: number
    includeInImport: boolean
    comment: string | null
    operatorNotes: string | null
    anonymizationFunction: string
  } = {
    columnNameOverride: currentColumn.columnNameOverride,
    columnTypeOverride: currentColumn.columnTypeOverride,
    sqoopColumnTypeOverride: currentColumn.sqoopColumnTypeOverride,
    forceString: currentColumn.forceString,
    includeInImport: currentColumn.includeInImport,
    comment: currentColumn.comment,
    operatorNotes: currentColumn.operatorNotes,
    anonymizationFunction: currentColumn.anonymizationFunction
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<Columns, keyof typeof part1> = {
    columnName: currentColumn.columnName,
    columnOrder: currentColumn.columnOrder,
    sourceColumnName: currentColumn.sourceColumnName,
    columnType: currentColumn.columnType,
    sourceColumnType: currentColumn.sourceColumnType,
    sourceDatabaseType: currentColumn.sourceDatabaseType,
    sqoopColumnType: currentColumn.sqoopColumnType,
    sourcePrimaryKey: currentColumn.sourcePrimaryKey,
    lastUpdateFromSource: currentColumn.lastUpdateFromSource
  }

  const labelToColumnMap: Record<string, keyof typeof part1> = {
    'Column Name Override': 'columnNameOverride',
    'Column Type Override': 'columnTypeOverride',
    'Sqoop Column Type Override': 'sqoopColumnTypeOverride',
    'Force String': 'forceString',
    'Include in Import': 'includeInImport',
    Comment: 'comment',
    'Operator notes': 'operatorNotes',
    'Anonymization Function': 'anonymizationFunction'
  }

  const key = labelToColumnMap[setting.label]

  const settingValue = setting.value

  if (key) {
    if (
      typeof settingValue === 'string' ||
      typeof settingValue === 'number' ||
      typeof settingValue === 'boolean' ||
      settingValue === null
    ) {
      ;(part1[key] as (typeof part1)[typeof key]) = settingValue
    }
  }

  const finalColumnData: Columns = { ...part2, ...part1 }
  tableData.columns[indexInColumns] = finalColumnData
}

export function updateTableData(
  tableData: UITable,
  updatedSettings: EditSetting[],
  column?: boolean,
  indexInColumns?: number
): UITableWithoutEnum {
  const updatedTableData: UITableWithoutEnum = {
    ...tableData
  } as UITableWithoutEnum
  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    if (indexInColumns !== undefined && column === true) {
      updateColumnData(updatedTableData, setting, indexInColumns)
    } else {
      let value = setting.value as string

      if (setting.type === 'enum' && setting.enumOptions) {
        value = reverseMapEnumValue('import', setting.label, value as string)
      }

      const key = getKeyFromLabel(setting.label)

      if (key) {
        updatedTableData[key] = value
      }
    }
  })

  const finalTableData = Object.keys(updatedTableData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedTableData[key]
    }
    return acc
  }, {} as UITableWithoutEnum)

  return finalTableData
}

export function createTableData(
  newTableSettings: EditSetting[]
): TableCreateWithoutEnum {
  // Part 1: Keys that will get values from newTableSettings
  const part1: {
    database: string
    table: string
    connection: string
    sourceSchema: string
    sourceTable: string
    importPhaseType: string
    etlPhaseType: string
    importTool: string
    etlEngine: string
    includeInAirflow: boolean
  } = {
    database: '',
    table: '',
    connection: '',
    sourceSchema: '',
    sourceTable: '',
    importPhaseType: ImportType.Full,
    etlPhaseType: EtlType.TruncateAndInsert,
    importTool: ImportTool.Spark,
    etlEngine: EtlEngine.Spark,
    includeInAirflow: true
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<TableCreateWithoutEnum, keyof typeof part1> = {
    lastUpdateFromSource: null,
    sqlWhereAddition: null,
    nomergeIngestionSqlAddition: null,
    airflowPriority: null,
    validateImport: null,
    validationMethod: null,
    validateSource: null,
    validateDiffAllowed: null,
    validationCustomQuerySourceSQL: null,
    validationCustomQueryHiveSQL: null,
    validationCustomQueryValidateImportTable: null,
    sqlSessions: null,
    softDeleteDuringMerge: null,
    incrMode: null,
    incrColumn: null,
    incrValidationMethod: null,
    pkColumnOverride: null,
    pkColumnOverrideMergeonly: null,
    hiveContainerSize: null,
    splitCount: null,
    sparkExecutorMemory: null,
    sparkExecutors: null,
    splitByColumn: null,
    sqoopCustomQuery: null,
    sqoopOptions: null,
    sqoopUseGeneratedSql: null,
    allowTextSplitter: null,
    forceString: null,
    comment: null,
    datalakeSource: null,
    operatorNotes: null,
    createForeignKeys: null,
    invalidateImpala: null,
    customMaxQuery: null,
    mergeCompactionMethod: null,
    sourceTableType: null,
    importDatabase: null,
    importTable: null,
    historyDatabase: null,
    historyTable: null,
    columns: [] // Initialize columns explicitly as an empty array
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    Database: 'database',
    Table: 'table',
    Connection: 'connection',
    'Source Schema': 'sourceSchema',
    'Source Table': 'sourceTable',
    'Import Type': 'importPhaseType',
    'ETL Type': 'etlPhaseType',
    'Import Tool': 'importTool',
    'ETL Engine': 'etlEngine',
    'Include in Airflow': 'includeInAirflow'
  }

  const filteredSettings = newTableSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    let value = setting.value

    if (setting.type === 'enum' && setting.enumOptions) {
      value = reverseMapEnumValue('import', setting.label, value as string)
    }
    const key = labelToKeyMap[setting.label]

    if (key) {
      if (typeof value === 'string' || typeof value === 'boolean') {
        ;(part1[key] as (typeof part1)[typeof key]) = value
      }
    }
  })

  const finalCreateTableData: TableCreateWithoutEnum = { ...part2, ...part1 }
  // const finalCreateTableData: Omit<TableCreateMapped, 'includeInAirflow'> = { ...part2, ...part1 };

  return finalCreateTableData
}

export function newCopyImportTableData(
  tableData: UITableWithoutEnum,
  tablePrimaryKeysSettings: EditSetting[]
): UITableWithoutEnum {
  // Creates a copy of the existing tableData to avoid mutations
  const updatedTableData: UITableWithoutEnum = cloneDeep(tableData)

  for (const setting of tablePrimaryKeysSettings) {
    if (setting.label === 'Database' && typeof setting.value === 'string') {
      updatedTableData.database = setting.value
    } else if (setting.label === 'Table' && typeof setting.value === 'string') {
      updatedTableData.table = setting.value
    }
  }

  return updatedTableData
}

/////////////////////////////////////////////////////////////////////////////////////////
// Export

function updateExportColumnData(
  tableData: UIExportTableWithoutEnum,
  setting: EditSetting,
  indexInColumns: number
) {
  if (!tableData.columns) return

  const currentColumn = tableData.columns[indexInColumns]

  const part1: {
    targetColumnName: string | null
    targetColumnType: string | null
    includeInExport: boolean
    operatorNotes: string | null
  } = {
    targetColumnName: currentColumn.targetColumnName,
    targetColumnType: currentColumn.targetColumnType,
    includeInExport: currentColumn.includeInExport,
    operatorNotes: currentColumn.operatorNotes
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<ExportColumns, keyof typeof part1> = {
    columnName: currentColumn.columnName,
    columnType: currentColumn.columnType,
    columnOrder: currentColumn.columnOrder,
    lastUpdateFromHive: currentColumn.lastUpdateFromHive,
    comment: currentColumn.comment
  }

  const labelToColumnMap: Record<string, keyof typeof part1> = {
    'Target Column Name': 'targetColumnName',
    'Target Column Type': 'targetColumnType',
    'Include in Export': 'includeInExport',
    'Operator notes': 'operatorNotes'
  }

  const key = labelToColumnMap[setting.label]

  const settingValue = setting.value

  if (key) {
    if (
      typeof settingValue === 'string' ||
      typeof settingValue === 'boolean' ||
      settingValue === null
    ) {
      ;(part1[key] as (typeof part1)[typeof key]) = settingValue
    }
  }

  const finalColumnData: ExportColumns = { ...part2, ...part1 }
  tableData.columns[indexInColumns] = finalColumnData
}

export function updateExportTableData(
  tableData: UIExportTable,
  updatedSettings: EditSetting[],
  column?: boolean,
  indexInColumns?: number
): UIExportTableWithoutEnum {
  const updatedTableData: UIExportTableWithoutEnum = {
    ...tableData
  } as UIExportTableWithoutEnum
  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    if (indexInColumns !== undefined && column === true) {
      updateExportColumnData(updatedTableData, setting, indexInColumns)
    } else {
      let value = setting.value as string

      if (setting.type === 'enum' && setting.enumOptions) {
        value = reverseMapEnumValue('export', setting.label, value as string)
      }

      const key = getKeyFromExportLabel(setting.label)

      if (key) {
        updatedTableData[key] = value
      }
    }
  })

  const finalTableData = Object.keys(updatedTableData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedTableData[key]
    }
    return acc
  }, {} as UIExportTableWithoutEnum)

  return finalTableData
}

export function createExportTableData(
  newTableSettings: EditSetting[]
): ExportTableCreateWithoutEnum {
  // Part 1: Keys that will get values from newTableSettings
  const part1: {
    connection: string
    targetTable: string
    targetSchema: string
    database: string
    table: string
    exportType: string
    exportTool: string
    includeInAirflow: boolean
  } = {
    connection: '',
    targetTable: '',
    targetSchema: '',
    database: '',
    table: '',
    exportType: ExportType.Full,
    exportTool: ExportTool.Spark,
    includeInAirflow: true
  }

  // Part 2: Keys that will retain default values
  const part2: Omit<ExportTableCreateWithoutEnum, keyof typeof part1> = {
    lastUpdateFromHive: null,
    sqlWhereAddition: null,
    airflowPriority: null,
    forceCreateTempTable: null,
    validateExport: null,
    validationMethod: null,
    validationCustomQueryHiveSQL: null,
    validationCustomQueryTargetSQL: null,
    uppercaseColumns: null,
    truncateTarget: null,
    sqlSessions: null,
    tableRowcount: null,
    targetRowcount: null,
    validationCustomQueryHiveValue: null,
    validationCustomQueryTargetValue: null,
    incrColumn: null,
    incrValidationMethod: null,
    incrMinvalue: null,
    incrMaxvalue: null,
    incrMinvaluePending: null,
    incrMaxvaluePending: null,
    sqoopOptions: null,
    lastSize: null,
    lastRows: null,
    lastSqlSessions: null,
    lastExecution: null,
    hiveContainerSize: null,
    createTargetTableSql: null,
    operatorNotes: null,
    columns: []
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    Connection: 'connection',
    'Target Table': 'targetTable',
    'Target Schema': 'targetSchema',
    Database: 'database',
    Table: 'table',
    'Export Type': 'exportType',
    'Export Tool': 'exportTool',
    'Include in Airflow': 'includeInAirflow'
  }

  const filteredSettings = newTableSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    let value = setting.value

    if (setting.type === 'enum' && setting.enumOptions) {
      value = reverseMapEnumValue('export', setting.label, value as string)
    }
    const key = labelToKeyMap[setting.label]

    if (key) {
      if (typeof value === 'string' || typeof value === 'boolean') {
        ;(part1[key] as (typeof part1)[typeof key]) = value
      }
    }
  })

  const finalCreateTableData: ExportTableCreateWithoutEnum = {
    ...part2,
    ...part1
  }
  // const finalCreateTableData: Omit<TableCreateMapped, 'includeInAirflow'> = { ...part2, ...part1 };

  return finalCreateTableData
}

export function newCopyExportTableData(
  tableData: UIExportTableWithoutEnum,
  tablePrimaryKeysSettings: EditSetting[]
): UIExportTableWithoutEnum {
  // Creates a copy of the existing tableData to avoid mutations
  const updatedTableData: UIExportTableWithoutEnum = cloneDeep(tableData)

  for (const setting of tablePrimaryKeysSettings) {
    if (setting.label === 'Connection' && typeof setting.value === 'string') {
      updatedTableData.connection = setting.value
    } else if (
      setting.label === 'Target Table' &&
      typeof setting.value === 'string'
    ) {
      updatedTableData.targetTable = setting.value
    } else if (
      setting.label === 'Target Schema' &&
      typeof setting.value === 'string'
    ) {
      updatedTableData.targetSchema = setting.value
    }
  }

  return updatedTableData
}
/////////////////////////////////////////////////////////////////////////////////////////
// Airflow

function updateTasksData(
  dagData: ImportAirflowDAG | BaseAirflowDAG | ExportAirflowDAG,
  setting: EditSetting,
  indexInTasks: number
) {
  if (!dagData.tasks) return

  const currentTask = dagData.tasks[indexInTasks]

  const part1: {
    type: string
    placement: string
    connection: string | null
    airflowPriority: number | null
    includeInAirflow: boolean
    taskDependencyDownstream: string | null
    taskDependencyUpstream: string | null
    taskConfig: string | null
    sensorPokeInterval: number | null
    sensorConnection: string | null
    sensorSoftFail: number | null
    sudoUser: string | null
  } = {
    type: currentTask.type,
    placement: currentTask.placement,
    connection: currentTask.connection,
    airflowPriority: currentTask.airflowPriority,
    includeInAirflow: currentTask.includeInAirflow,
    taskDependencyDownstream: currentTask.taskDependencyDownstream,
    taskDependencyUpstream: currentTask.taskDependencyUpstream,
    taskConfig: currentTask.taskConfig,
    sensorPokeInterval: currentTask.sensorPokeInterval,
    sensorConnection: currentTask.sensorConnection,
    sensorSoftFail: currentTask.sensorSoftFail,
    sudoUser: currentTask.sudoUser
  }

  // Part 2: Readonly keys
  const part2: Omit<AirflowTask, keyof typeof part1> = {
    name: currentTask.name,
    airflowPool: currentTask.airflowPool,
    sensorTimeoutMinutes: currentTask.sensorTimeoutMinutes
  }

  const labelToColumnMap: Record<string, keyof typeof part1> = {
    Type: 'type',
    Placement: 'placement',
    Connection: 'connection',
    'Airflow Priority': 'airflowPriority',
    'Include in Airflow': 'includeInAirflow',
    'Task Dependency Downstream': 'taskDependencyDownstream',
    'Task Dependency Upstream': 'taskDependencyUpstream',
    'Task Config': 'taskConfig',
    'Sensor Poke Interval': 'sensorPokeInterval',
    'Sensor Connection': 'sensorConnection',
    'Sensor Soft Fail': 'sensorSoftFail',
    'Sudo User': 'sudoUser'
  }

  const key = labelToColumnMap[setting.label]

  const settingValue = setting.value

  if (key) {
    if (
      typeof settingValue === 'string' ||
      typeof settingValue === 'number' ||
      typeof settingValue === 'boolean' ||
      settingValue === null
    ) {
      ;(part1[key] as (typeof part1)[typeof key]) = settingValue
    }
  }

  const finalTasksData: AirflowTask = { ...part2, ...part1 }
  dagData.tasks[indexInTasks] = finalTasksData
}

export function updateImportDagData(
  dagData: AirflowWithDynamicKeys<ImportAirflowDAG>,
  updatedSettings: EditSetting[],
  addTask?: 'create' | 'copy' | null,
  editTask?: boolean,
  indexInTasks?: number
): ImportAirflowDAG {
  const updatedDagData: AirflowWithDynamicKeys<ImportAirflowDAG> = {
    ...dagData
  }
  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  if (addTask === 'create') {
    const part1 = {
      name: '',
      type: '',
      placement: '',
      connection: null,
      airflowPriority: null,
      includeInAirflow: true,
      taskDependencyDownstream: null,
      taskDependencyUpstream: null,
      taskConfig: null,
      sensorPokeInterval: null,
      sensorConnection: null,
      sensorSoftFail: null,
      sudoUser: null
    }

    const part2: Omit<AirflowTask, keyof typeof part1> = {
      airflowPool: null,
      sensorTimeoutMinutes: null
    }

    const labelToTaskMap: Record<string, keyof typeof part1> = {
      'Task Name': 'name',
      Type: 'type',
      Placement: 'placement',
      Connection: 'connection',
      'Airflow Priority': 'airflowPriority',
      'Include in Airflow': 'includeInAirflow',
      'Task Dependency Downstream': 'taskDependencyDownstream',
      'Task Dependency Upstream': 'taskDependencyUpstream',
      'Task Config': 'taskConfig',
      'Sensor Poke Interval': 'sensorPokeInterval',
      'Sensor Connection': 'sensorConnection',
      'Sensor Soft Fail': 'sensorSoftFail',
      'Sudo User': 'sudoUser'
    }

    filteredSettings.forEach((setting) => {
      const key = labelToTaskMap[setting.label]
      const settingValue = setting.value
      if (
        key &&
        (typeof settingValue === 'string' ||
          typeof settingValue === 'boolean' ||
          settingValue === null)
      ) {
        ;(part1[key] as (typeof part1)[typeof key]) = settingValue
      }
    })

    const finalTasksData: AirflowTask = { ...part2, ...part1 }
    updatedDagData.tasks.push(finalTasksData)
  } else if (addTask === 'copy') {
    if (
      indexInTasks === undefined ||
      !dagData.tasks ||
      !dagData.tasks[indexInTasks]
    ) {
      console.error('Invalid rowIndex or tasks array.')
      return dagData
    }

    // const taskCopy = { ...dagData.tasks[indexInTasks] }
    const taskCopy = cloneDeep(dagData.tasks[indexInTasks])

    const nameSetting = updatedSettings.find(
      (setting) => setting.label === 'Task Name'
    )
    if (nameSetting && typeof nameSetting.value === 'string') {
      taskCopy.name = nameSetting.value
    } else {
      console.error('Task Name setting is missing or invalid.')
      return dagData
    }

    updatedDagData.tasks.push(taskCopy)
  } else if (editTask && indexInTasks !== undefined) {
    filteredSettings.forEach((setting) => {
      updateTasksData(updatedDagData, setting, indexInTasks)
    })
  } else {
    filteredSettings.forEach((setting) => {
      const value = setting.value as string
      const key = getKeyFromImportAirflowLabel(setting.label)
      if (key) {
        updatedDagData[key] = value
      }
    })
  }

  const reducedDagData = Object.keys(updatedDagData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedDagData[key]
    }
    return acc
  }, {} as AirflowWithDynamicKeys<ImportAirflowDAG>)

  const finalDagData = reducedDagData as ImportAirflowDAG

  return finalDagData
}

/////////////////////////////////////////////////////////////////

export function updateExportDagData(
  dagData: AirflowWithDynamicKeys<ExportAirflowDAG>,
  updatedSettings: EditSetting[],
  addTask?: 'create' | 'copy' | null,
  editTask?: boolean,
  indexInTasks?: number
): ExportAirflowDAG {
  const updatedDagData: AirflowWithDynamicKeys<ExportAirflowDAG> = {
    ...dagData
  }
  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  if (addTask === 'create') {
    const part1 = {
      name: '',
      type: '',
      placement: '',
      connection: null,
      airflowPriority: null,
      includeInAirflow: true,
      taskDependencyDownstream: null,
      taskDependencyUpstream: null,
      taskConfig: null,
      sensorPokeInterval: null,
      sensorConnection: null,
      sensorSoftFail: null,
      sudoUser: null
    }

    const part2: Omit<AirflowTask, keyof typeof part1> = {
      airflowPool: null,
      sensorTimeoutMinutes: null
    }

    const labelToTaskMap: Record<string, keyof typeof part1> = {
      'Task Name': 'name',
      Type: 'type',
      Placement: 'placement',
      Connection: 'connection',
      'Airflow Priority': 'airflowPriority',
      'Include in Airflow': 'includeInAirflow',
      'Task Dependency Downstream': 'taskDependencyDownstream',
      'Task Dependency Upstream': 'taskDependencyUpstream',
      'Task Config': 'taskConfig',
      'Sensor Poke Interval': 'sensorPokeInterval',
      'Sensor Connection': 'sensorConnection',
      'Sensor Soft Fail': 'sensorSoftFail',
      'Sudo User': 'sudoUser'
    }

    filteredSettings.forEach((setting) => {
      const key = labelToTaskMap[setting.label]
      const settingValue = setting.value
      if (
        key &&
        (typeof settingValue === 'string' ||
          typeof settingValue === 'boolean' ||
          settingValue === null)
      ) {
        ;(part1[key] as (typeof part1)[typeof key]) = settingValue
      }
    })

    const finalTasksData: AirflowTask = { ...part2, ...part1 }
    updatedDagData.tasks.push(finalTasksData)
  } else if (addTask === 'copy') {
    if (
      indexInTasks === undefined ||
      !dagData.tasks ||
      !dagData.tasks[indexInTasks]
    ) {
      console.error('Invalid rowIndex or tasks array.')
      return dagData
    }

    // const taskCopy = { ...dagData.tasks[indexInTasks] }
    const taskCopy = cloneDeep(dagData.tasks[indexInTasks])

    const nameSetting = updatedSettings.find(
      (setting) => setting.label === 'Task Name'
    )
    if (nameSetting && typeof nameSetting.value === 'string') {
      taskCopy.name = nameSetting.value
    } else {
      console.error('Task Name setting is missing or invalid.')
      return dagData
    }

    updatedDagData.tasks.push(taskCopy)
  } else if (editTask && indexInTasks !== undefined) {
    filteredSettings.forEach((setting) => {
      updateTasksData(updatedDagData, setting, indexInTasks)
    })
  } else {
    filteredSettings.forEach((setting) => {
      const value = setting.value as string
      const key = getKeyFromExportAirflowLabel(setting.label)
      if (key) {
        updatedDagData[key] = value
      }
    })
  }

  const reducedDagData = Object.keys(updatedDagData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedDagData[key]
    }
    return acc
  }, {} as AirflowWithDynamicKeys<ExportAirflowDAG>)

  const finalDagData = reducedDagData as ExportAirflowDAG

  return finalDagData
}

export function updateCustomDagData(
  dagData: AirflowWithDynamicKeys<CustomAirflowDAG>,
  updatedSettings: EditSetting[],
  addTask?: 'create' | 'copy' | null,
  editTask?: boolean,
  indexInTasks?: number
): CustomAirflowDAG {
  const updatedDagData: AirflowWithDynamicKeys<CustomAirflowDAG> = {
    ...dagData
  }
  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  if (addTask === 'create') {
    const part1 = {
      name: '',
      type: '',
      placement: '',
      connection: null,
      airflowPriority: null,
      includeInAirflow: true,
      taskDependencyDownstream: null,
      taskDependencyUpstream: null,
      taskConfig: null,
      sensorPokeInterval: null,
      sensorConnection: null,
      sensorSoftFail: null,
      sudoUser: null
    }

    const part2: Omit<AirflowTask, keyof typeof part1> = {
      airflowPool: null,
      sensorTimeoutMinutes: null
    }

    const labelToTaskMap: Record<string, keyof typeof part1> = {
      'Task Name': 'name',
      Type: 'type',
      Placement: 'placement',
      Connection: 'connection',
      'Airflow Priority': 'airflowPriority',
      'Include in Airflow': 'includeInAirflow',
      'Task Dependency Downstream': 'taskDependencyDownstream',
      'Task Dependency Upstream': 'taskDependencyUpstream',
      'Task Config': 'taskConfig',
      'Sensor Poke Interval': 'sensorPokeInterval',
      'Sensor Connection': 'sensorConnection',
      'Sensor Soft Fail': 'sensorSoftFail',
      'Sudo User': 'sudoUser'
    }

    filteredSettings.forEach((setting) => {
      const key = labelToTaskMap[setting.label]
      const settingValue = setting.value
      if (
        key &&
        (typeof settingValue === 'string' ||
          typeof settingValue === 'boolean' ||
          settingValue === null)
      ) {
        ;(part1[key] as (typeof part1)[typeof key]) = settingValue
      }
    })

    const finalTasksData: AirflowTask = { ...part2, ...part1 }
    updatedDagData.tasks.push(finalTasksData)
  } else if (addTask === 'copy') {
    if (
      indexInTasks === undefined ||
      !dagData.tasks ||
      !dagData.tasks[indexInTasks]
    ) {
      console.error('Invalid rowIndex or tasks array.')
      return dagData
    }

    // const taskCopy = { ...dagData.tasks[indexInTasks] }
    const taskCopy = cloneDeep(dagData.tasks[indexInTasks])

    const nameSetting = updatedSettings.find(
      (setting) => setting.label === 'Task Name'
    )
    if (nameSetting && typeof nameSetting.value === 'string') {
      taskCopy.name = nameSetting.value
    } else {
      console.error('Task Name setting is missing or invalid.')
      return dagData
    }

    updatedDagData.tasks.push(taskCopy)
  } else if (editTask && indexInTasks !== undefined) {
    filteredSettings.forEach((setting) => {
      updateTasksData(updatedDagData, setting, indexInTasks)
    })
  } else {
    filteredSettings.forEach((setting) => {
      const value = setting.value as string
      const key = getKeyFromCustomAirflowLabel(setting.label)
      if (key) {
        updatedDagData[key] = value
      }
    })
  }

  const reducedDagData = Object.keys(updatedDagData).reduce((acc, key) => {
    if (!fieldsToRemove.includes(key)) {
      acc[key] = updatedDagData[key]
    }
    return acc
  }, {} as AirflowWithDynamicKeys<CustomAirflowDAG>)

  const finalDagData = reducedDagData as CustomAirflowDAG

  return finalDagData
}

export function createImportDagData(
  newImportAirflowData: EditSetting[]
): ImportCreateAirflowDAG {
  // Part 1: Keys that will get values from newImportAirflowData
  const part1: {
    name: string
    scheduleInterval: string | null
    autoRegenerateDag: boolean
    filterTable: string | null
  } = {
    name: '',
    scheduleInterval: null,
    autoRegenerateDag: true,
    filterTable: null
  }

  const part2: Omit<ImportCreateAirflowDAG, keyof typeof part1> = {
    retries: null,
    operatorNotes: null,
    applicationNotes: null,
    airflowNotes: null,
    sudoUser: null,
    timezone: null,
    email: null,
    emailOnFailure: null,
    emailOnRetries: null,
    tags: null,
    slaWarningTime: null,
    retryExponentialBackoff: null,
    concurrency: null,
    finishAllStage1First: null,
    runImportAndEtlSeparate: null,
    retriesStage1: null,
    retriesStage2: null,
    poolStage1: null,
    poolStage2: null,
    metadataImport: null,
    tasks: []
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    'DAG Name': 'name',
    'Schedule Interval': 'scheduleInterval',
    'Auto Regenerate DAG': 'autoRegenerateDag',
    'Filter Table': 'filterTable'
  }

  const filteredSettings = newImportAirflowData.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    const value = setting.value

    const key = labelToKeyMap[setting.label]

    if (key) {
      if (key === 'name' && typeof value === 'string') {
        part1[key] = value
      } else if (
        key === 'scheduleInterval' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      } else if (key === 'autoRegenerateDag' && typeof value === 'boolean') {
        part1[key] = value
      } else if (
        key === 'filterTable' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      }
    }
  })

  const finalCreateData: ImportCreateAirflowDAG = { ...part2, ...part1 }

  return finalCreateData
}

export function createExportDagData(
  newExportAirflowData: EditSetting[]
): ExportCreateAirflowDAG {
  // Part 1: Keys that will get values from newExportAirflowData
  const part1: {
    name: string
    scheduleInterval: string | null
    autoRegenerateDag: boolean
    filterConnection: string | null
    filterTargetSchema: string | null
    filterTargetTable: string | null
  } = {
    name: '',
    scheduleInterval: null,
    autoRegenerateDag: true,
    filterConnection: null,
    filterTargetSchema: null,
    filterTargetTable: null
  }

  const part2: Omit<ExportCreateAirflowDAG, keyof typeof part1> = {
    retries: null,
    operatorNotes: null,
    applicationNotes: null,
    airflowNotes: null,
    sudoUser: null,
    timezone: null,
    email: null,
    emailOnFailure: null,
    emailOnRetries: null,
    tags: null,
    slaWarningTime: null,
    retryExponentialBackoff: null,
    concurrency: null,
    tasks: []
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    'DAG Name': 'name',
    'Schedule Interval': 'scheduleInterval',
    'Auto Regenerate DAG': 'autoRegenerateDag',
    'Filter Connection': 'filterConnection',
    'Filter Target Schema': 'filterTargetSchema',
    'Filter Target Table': 'filterTargetTable'
  }

  const filteredSettings = newExportAirflowData.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    const value = setting.value

    const key = labelToKeyMap[setting.label]

    if (key) {
      if (key === 'name' && typeof value === 'string') {
        part1[key] = value
      } else if (
        key === 'scheduleInterval' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      } else if (key === 'autoRegenerateDag' && typeof value === 'boolean') {
        part1[key] = value
      } else if (
        key === 'filterConnection' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      } else if (
        key === 'filterTargetSchema' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      } else if (
        key === 'filterTargetTable' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      }
    }
  })

  const finalCreateData: ExportCreateAirflowDAG = { ...part2, ...part1 }

  return finalCreateData
}

export function createCustomDagData(
  newCustomAirflowData: EditSetting[]
): CustomCreateAirflowDAG {
  const part1: {
    name: string
    scheduleInterval: string | null
    autoRegenerateDag: boolean
    filterTable: string | null
  } = {
    name: '',
    scheduleInterval: 'None',
    autoRegenerateDag: true,
    filterTable: null
  }

  // Part 2: Rest of the keys required by the API
  const part2: Omit<CustomCreateAirflowDAG, keyof typeof part1> = {
    retries: null,
    operatorNotes: null,
    applicationNotes: null,
    airflowNotes: null,
    sudoUser: null,
    timezone: null,
    email: null,
    emailOnFailure: null,
    emailOnRetries: null,
    tags: null,
    slaWarningTime: null,
    retryExponentialBackoff: null,
    concurrency: null,
    tasks: []
  }

  const labelToKeyMap: Record<string, keyof typeof part1> = {
    'DAG Name': 'name',
    'Schedule Interval': 'scheduleInterval',
    'Auto Regenerate DAG': 'autoRegenerateDag'
  }

  const filteredSettings = newCustomAirflowData.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    const value = setting.value

    const key = labelToKeyMap[setting.label]

    if (key) {
      if (key === 'name' && typeof value === 'string') {
        part1[key] = value
      } else if (
        key === 'scheduleInterval' &&
        (typeof value === 'string' || value === null)
      ) {
        part1[key] = value
      } else if (key === 'autoRegenerateDag' && typeof value === 'boolean') {
        part1[key] = value
      }
    }
  })

  const finalCreateData: CustomCreateAirflowDAG = { ...part2, ...part1 }

  return finalCreateData
}

export function newCopyDagData(
  dagName: string,
  dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
): ImportCreateAirflowDAG | ExportCreateAirflowDAG | CustomCreateAirflowDAG {
  // Creates a copy of the existing dagData to avoid mutations
  const dagDataCopy = cloneDeep(dagData)

  const updatedDagData = { ...dagDataCopy, name: dagName } as
    | ImportCreateAirflowDAG
    | ExportCreateAirflowDAG
    | CustomCreateAirflowDAG

  return updatedDagData
}

// Configuration

export function updateGlobalConfigData(
  configData: ConfigGlobal,
  updatedSettings: EditSetting[]
): ConfigGlobalWithIndex {
  const updatedConfigData: ConfigGlobalWithIndex = {
    ...configData
  } as ConfigGlobalWithIndex

  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    let value = setting.value as string

    if (setting.type === 'enum' && setting.enumOptions) {
      value = reverseMapEnumValue('config', setting.label, value as string)
    }

    const key = getKeyFromGlobalConfigLabel(setting.label)

    if (key) {
      updatedConfigData[key] = value
    }
  })

  const finalConfigData = Object.keys(updatedConfigData).reduce((acc, key) => {
    if (
      filteredSettings.some(
        (setting) => getKeyFromGlobalConfigLabel(setting.label) === key
      ) &&
      !fieldsToRemove.includes(key)
    ) {
      acc[key] = updatedConfigData[key]
    }
    return acc
  }, {} as ConfigGlobalWithIndex)

  return finalConfigData
}

export function updateJDBCdriverData(
  configData: JDBCdrivers,
  updatedSettings: EditSetting[]
): JDBCdriversWithIndex {
  const updatedConfigData: JDBCdriversWithIndex = {
    ...configData
  } as JDBCdriversWithIndex

  const filteredSettings = updatedSettings.filter(
    (setting) => setting.type !== 'groupingSpace'
  )

  filteredSettings.forEach((setting) => {
    const value = setting.value as string

    const key = getKeyFromJDBCdriversLabel(setting.label)

    if (key) {
      updatedConfigData[key] = value
    }
  })

  const finalConfigData = Object.keys(updatedConfigData).reduce((acc, key) => {
    acc[key] = updatedConfigData[key]
    return acc
  }, {} as JDBCdriversWithIndex)

  return finalConfigData
}

// Bulk

// For bulkChanges with enum values
export function transformBulkChanges(
  type: 'import' | 'export',
  bulkChanges: Record<string, string | number | boolean | null>
): Record<string, string | number | boolean | null> {
  return Object.entries(bulkChanges).reduce((acc, [key, value]) => {
    let category: string
    if (key === 'importPhaseType') {
      category = 'importType'
    } else if (key === 'etlPhaseType') {
      category = 'etlType'
    } else {
      category = key
    }

    try {
      const backendValue = reverseMapEnumValue(
        type,
        category,
        String(value),
        true
      )
      acc[key] = backendValue
    } catch {
      acc[key] = value
    }

    return acc
  }, {} as Record<string, string | number | boolean | null>)
}

export const transformGenerateConnectionSettings = (
  settings: EditSetting[]
): GenerateJDBCconnectionString => {
  const settingsMap = settings.reduce<
    Record<string, string | number | boolean | null>
  >((acc, setting) => {
    acc[setting.label] = setting.value
    return acc
  }, {})

  return {
    databaseType: settingsMap['Database type'] || 'unknown',
    version: settingsMap['Version'] || 'default',
    hostname: settingsMap['Hostname'] || 'unknown',
    port: settingsMap['Port'] !== undefined ? settingsMap['Port'] : null,
    database: settingsMap['Database'] || 'unknown'
  } as GenerateJDBCconnectionString
}
