import { useMutation } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import {
  TableCreateWithoutEnum,
  Connection,
  UITableWithoutEnum,
  CustomAirflowDAG,
  ExportAirflowDAG,
  ImportAirflowDAG,
  CustomCreateAirflowDAG,
  ExportCreateAirflowDAG,
  ImportCreateAirflowDAG,
  UIExportTableWithoutEnum,
  ExportTableCreateWithoutEnum,
  ConfigGlobalWithIndex,
  JDBCdriversWithIndex,
  BulkUpdateExportTables,
  BulkUpdateImportTables,
  BulkUpdateAirflowDAG,
  ImportPKs,
  ExportPKs,
  AirflowDagPk,
  GenerateJDBCconnectionString,
  ImportDiscoverTable,
  ExportDiscoverTable,
  EncryptCredentials,
  ErrorData
} from './interfaces'
import { AxiosError } from 'axios'

// CONNECTION

// Encrypt credentials

const postEncryptCredentials = async (data: EncryptCredentials) => {
  const response = await axiosInstance.post(
    '/connection/encryptCredentials',
    data
  )

  return response.data
}

export const useEncryptCredentials = () => {
  return useMutation<void, AxiosError<ErrorData>, EncryptCredentials>({
    mutationFn: (data: EncryptCredentials) => {
      return postEncryptCredentials(data)
    }
  })
}

// Generate connection string

const postGenerateConnectionString = async (
  connectionStringData: GenerateJDBCconnectionString
) => {
  const response = await axiosInstance.post(
    '/connection/generateJDBCconnectionString',
    connectionStringData
  )
  return response.data
}

export const useGenerateConnectionString = () => {
  return useMutation({
    mutationFn: (connectionStringData: GenerateJDBCconnectionString) => {
      return postGenerateConnectionString(connectionStringData)
    }
  })
}

// Test connection

const getTestConnection = async (connection: string) => {
  const encodedConnection = encodeURIComponent(connection)
  const response = await axiosInstance.get(
    `/connection/testConnection/${encodedConnection}`
  )
  return response.data
}

export const useTestConnection = () => {
  return useMutation<void, AxiosError<ErrorData>, string>({
    mutationFn: (connection: string) => getTestConnection(connection)
  })
}

// Create or update connection

const postCreateOrUpdateConnection = async (connection: Connection) => {
  const response = await axiosInstance.post('/connection', connection)

  return response.data
}

export const useCreateOrUpdateConnection = () => {
  return useMutation({
    mutationFn: (connectionUpdated: Connection) => {
      return postCreateOrUpdateConnection(connectionUpdated)
    }
  })
}

// Delete connection

type DeleteConnectionArgs = {
  connectionName: string
}

const deleteConnection = async ({ connectionName }: DeleteConnectionArgs) => {
  const encodedConnection = encodeURIComponent(connectionName)

  const response = await axiosInstance.delete(
    `/connection/${encodedConnection}`
  )
  return response.data
}

export const useDeleteConnection = () => {
  return useMutation<void, AxiosError<ErrorData>, DeleteConnectionArgs>({
    mutationFn: (args: DeleteConnectionArgs) => deleteConnection(args)
  })
}

// IMPORT AND EXPORT

interface TablePkArgs {
  type: 'import' | 'export'
  primaryKeys: ImportPKs | ExportPKs
}

// Repair table

const postRepairTable = async (
  type: 'import' | 'export',
  primaryKeys: ImportPKs | ExportPKs
) => {
  switch (type) {
    case 'import': {
      const { database, table } = primaryKeys as ImportPKs
      const encodedDatabase = encodeURIComponent(database)
      const encodedTable = encodeURIComponent(table)

      const postUrl = `/${type}/table/${encodedDatabase}/${encodedTable}/repair`

      const response = await axiosInstance.post(postUrl)
      return response.data
    }
    case 'export': {
      const { connection, targetSchema, targetTable } = primaryKeys as ExportPKs
      const encodedConnection = encodeURIComponent(connection)
      const encodedTargetSchema = encodeURIComponent(targetSchema)
      const encodedTargetTable = encodeURIComponent(targetTable)

      const postUrl = `/${type}/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/repair`

      const response = await axiosInstance.post(postUrl)
      return response.data
    }
  }
}

export const useRepairTable = () => {
  return useMutation<void, AxiosError<ErrorData>, TablePkArgs>({
    mutationFn: ({ type, primaryKeys }) => {
      return postRepairTable(type, primaryKeys)
    }
  })
}

// Reset table

const postResetTable = async (
  type: 'import' | 'export',
  primaryKeys: ImportPKs | ExportPKs
) => {
  switch (type) {
    case 'import': {
      const { database, table } = primaryKeys as ImportPKs
      const encodedDatabase = encodeURIComponent(database)
      const encodedTable = encodeURIComponent(table)

      const postUrl = `/${type}/table/${encodedDatabase}/${encodedTable}/reset`

      const response = await axiosInstance.post(postUrl)
      return response.data
    }
    case 'export': {
      const { connection, targetSchema, targetTable } = primaryKeys as ExportPKs
      const encodedConnection = encodeURIComponent(connection)
      const encodedTargetSchema = encodeURIComponent(targetSchema)
      const encodedTargetTable = encodeURIComponent(targetTable)

      const postUrl = `/${type}/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/reset`

      const response = await axiosInstance.post(postUrl)
      return response.data
    }
  }
}

export const useResetTable = () => {
  return useMutation<void, AxiosError<ErrorData>, TablePkArgs>({
    mutationFn: ({ type, primaryKeys }) => postResetTable(type, primaryKeys)
  })
}

// Copy table

interface CopyTableProps {
  type: 'import' | 'export'
  table: UITableWithoutEnum | UIExportTableWithoutEnum
}

const postCopyTable = async (
  type: 'import' | 'export',
  table: UITableWithoutEnum | UIExportTableWithoutEnum
) => {
  if (type === 'import') {
    const {
      database,
      table: tableName,
      ...tableObject
    } = table as UITableWithoutEnum
    const encodedDatabase = encodeURIComponent(database)
    const encodedTable = encodeURIComponent(tableName)

    const response = await axiosInstance.post(
      `/import/table/${encodedDatabase}/${encodedTable}`,
      tableObject
    )
    return response.data
  } else if (type === 'export') {
    const { connection, targetSchema, targetTable, ...exportTableObject } =
      table as UIExportTableWithoutEnum
    const encodedConnection = encodeURIComponent(connection)
    const encodedTargetSchema = encodeURIComponent(targetSchema)
    const encodedTargetTable = encodeURIComponent(targetTable)

    const response = await axiosInstance.post(
      `/export/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`,
      exportTableObject
    )
    return response.data
  }
}

export const useCopyTable = () => {
  return useMutation<void, AxiosError<ErrorData>, CopyTableProps>({
    mutationFn: ({ type, table }) => postCopyTable(type, table)
  })
}

// Update table

interface PostTableProps {
  type: 'import' | 'export'
  table: UITableWithoutEnum | UIExportTableWithoutEnum
}

const postUpdateTable = async (
  type: 'import' | 'export',
  table: UITableWithoutEnum | UIExportTableWithoutEnum
) => {
  const { database, table: tableName, ...importTableObject } = table
  const encodedDatabase = encodeURIComponent(database)
  const encodedTable = encodeURIComponent(tableName)

  const { connection, targetSchema, targetTable, ...exportTableObject } = table
  const encodedConnection = encodeURIComponent(connection)
  const encodedTargetSchema = encodeURIComponent(targetSchema as string)
  const encodedTargetTable = encodeURIComponent(targetTable as string)

  const postUrl =
    type === 'import'
      ? `/${type}/table/${encodedDatabase}/${encodedTable}`
      : `/${type}/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`

  const tableObject = type === 'import' ? importTableObject : exportTableObject

  const response = await axiosInstance.post(postUrl, tableObject)

  return response.data
}

export const useUpdateTable = () => {
  return useMutation<
    unknown, // Return type of mutation function, specify it more?
    Error,
    PostTableProps
  >({
    mutationFn: ({ type, table }) => {
      return postUpdateTable(type, table)
    }
  })
}

// Delete table

const deleteTable = async (
  type: 'import' | 'export',
  primaryKeys: ImportPKs | ExportPKs
) => {
  switch (type) {
    case 'import': {
      const { database, table } = primaryKeys as ImportPKs
      const encodedDatabase = encodeURIComponent(database)
      const encodedTable = encodeURIComponent(table)

      const url = `/${type}/table/${encodedDatabase}/${encodedTable}`

      const response = await axiosInstance.delete(url)
      return response.data
    }
    case 'export': {
      const { connection, targetSchema, targetTable } = primaryKeys as ExportPKs
      const encodedConnection = encodeURIComponent(connection)
      const encodedTargetSchema = encodeURIComponent(targetSchema)
      const encodedTargetTable = encodeURIComponent(targetTable)

      const url = `/${type}/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`

      const response = await axiosInstance.delete(url)
      return response.data
    }
  }
}

export const useDeleteTable = () => {
  return useMutation<void, AxiosError<ErrorData>, TablePkArgs>({
    mutationFn: ({ type, primaryKeys }) => deleteTable(type, primaryKeys)
  })
}

// Import create table

const postCreateImportTable = async (table: TableCreateWithoutEnum) => {
  const { database, table: tableName, ...tableObject } = table
  const encodedDatabase = encodeURIComponent(database)
  const encodedTable = encodeURIComponent(tableName)

  const response = await axiosInstance.post(
    `/import/table/${encodedDatabase}/${encodedTable}`,
    tableObject
  )
  return response.data
}

export const useCreateImportTable = () => {
  return useMutation<void, AxiosError<ErrorData>, TableCreateWithoutEnum>({
    mutationFn: (tableUpdated: TableCreateWithoutEnum) => {
      return postCreateImportTable(tableUpdated)
    }
  })
}

// Add discovered tables

const postAddImportTables = async (tables: ImportDiscoverTable[]) => {
  const response = await axiosInstance.post('/import/discover/add', tables)
  return response.data
}

export const useAddImportTables = () => {
  return useMutation({
    mutationFn: (tables: ImportDiscoverTable[]) => postAddImportTables(tables)
  })
}

// Export create table

const postCreateExportTable = async (table: ExportTableCreateWithoutEnum) => {
  const { connection, targetSchema, targetTable, ...exportTableObject } = table
  const encodedConnection = encodeURIComponent(connection)
  const encodedTargetSchema = encodeURIComponent(targetSchema)
  const encodedTargetTable = encodeURIComponent(targetTable)

  const response = await axiosInstance.post(
    `/export/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`,
    exportTableObject
  )

  return response.data
}

export const useCreateExportTable = () => {
  return useMutation({
    mutationFn: (tableUpdated: ExportTableCreateWithoutEnum) => {
      return postCreateExportTable(tableUpdated)
    }
  })
}

// Add discovered tables

const postAddExportTables = async (tables: ExportDiscoverTable[]) => {
  const response = await axiosInstance.post('/export/discover/add', tables)
  return response.data
}

export const useAddExportTables = () => {
  return useMutation({
    mutationFn: (tables: ExportDiscoverTable[]) => postAddExportTables(tables)
  })
}

// Bulk update tables, Import or Export

const postBulkUpdateTable = async (
  type: 'import' | 'export',
  bulkUpdateJson: BulkUpdateImportTables | BulkUpdateExportTables
) => {
  const response = await axiosInstance.post(`/${type}/table`, bulkUpdateJson)

  return response.data
}

interface BulkPostTableProps {
  type: 'import' | 'export'
  bulkUpdateJson: BulkUpdateImportTables | BulkUpdateExportTables
}

export const useBulkUpdateTable = () => {
  return useMutation<
    unknown, // Return type of mutation function, specify it more?
    Error,
    BulkPostTableProps
  >({
    mutationFn: ({ type, bulkUpdateJson }) => {
      return postBulkUpdateTable(type, bulkUpdateJson)
    }
  })
}

// Bulk delete tables, Import or Export

type BulkDeleteTablesArgs = {
  type: 'import' | 'export'
  bulkDeleteRowsPks: ImportPKs[] | ExportPKs[]
}

const bulkDeleteTables = async ({
  type,
  bulkDeleteRowsPks
}: BulkDeleteTablesArgs) => {
  const response = await axiosInstance.delete(`/${type}/table`, {
    data: bulkDeleteRowsPks
  })
  return response.data
}

export const useBulkDeleteTables = () => {
  return useMutation({
    mutationFn: (args: BulkDeleteTablesArgs) => bulkDeleteTables(args)
  })
}

// AIRFLOW

// Generate DAG

const getGenerateDag = async (dagName: string) => {
  const encodedDagName = encodeURIComponent(dagName)
  const response = await axiosInstance.get(`/airflow/generate_dag`, {
    params: { dagname: encodedDagName }
  })
  return response.data
}

export const useGenerateDag = () => {
  return useMutation<void, AxiosError<ErrorData>, string>({
    mutationFn: (dagName: string) => getGenerateDag(dagName)
  })
}

// Update DAG

const updateAirflowDag = async (
  type: 'import' | 'export' | 'custom',
  dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
) => {
  const { name } = dagData
  const encodedDagName = encodeURIComponent(name)

  const response = await axiosInstance.post(
    `/airflow/dags/${type}/${encodedDagName}`,
    dagData
  )
  return response.data
}

export const useUpdateAirflowDag = () => {
  return useMutation<
    ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG,
    Error,
    {
      type: 'import' | 'export' | 'custom'
      dagData: ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => updateAirflowDag(type, dagData)
  })
}

// Create DAG

const postCreateAirflowDag = async (
  type: 'import' | 'export' | 'custom',
  dagData:
    | ImportCreateAirflowDAG
    | ExportCreateAirflowDAG
    | CustomCreateAirflowDAG
) => {
  const { name } = dagData
  const encodedDagName = encodeURIComponent(name)

  const response = await axiosInstance.post(
    `/airflow/dags/${type}/${encodedDagName}`,
    dagData
  )
  return response.data
}

export const useCreateAirflowDag = () => {
  return useMutation<
    ImportCreateAirflowDAG | ExportCreateAirflowDAG | CustomCreateAirflowDAG,
    Error,
    {
      type: 'import' | 'export' | 'custom'
      dagData:
        | ImportCreateAirflowDAG
        | ExportCreateAirflowDAG
        | CustomCreateAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => postCreateAirflowDag(type, dagData)
  })
}

// Delete DAG

type DeleteAirflowDAGArgs = {
  type: 'import' | 'export' | 'custom'
  dagName: string
}

const deleteAirflowDAG = async ({ type, dagName }: DeleteAirflowDAGArgs) => {
  const encodedDagName = encodeURIComponent(dagName)
  const response = await axiosInstance.delete(
    `/airflow/dags/${type}/${encodedDagName}`
  )
  return response.data
}

export const useDeleteAirflowDAG = () => {
  return useMutation({
    mutationFn: (args: DeleteAirflowDAGArgs) => deleteAirflowDAG(args)
  })
}

// Delete Task

type DeleteAirflowTaskArgs = {
  type: 'import' | 'export' | 'custom'
  dagName: string
  taskName: string
}

const deleteAirflowTask = async ({
  type,
  dagName,
  taskName
}: DeleteAirflowTaskArgs) => {
  const encodedDagName = encodeURIComponent(dagName)
  const encodedTaskName = encodeURIComponent(taskName)

  const response = await axiosInstance.delete(
    `/airflow/dags/${type}/${encodedDagName}/${encodedTaskName}`
  )
  return response.data
}

export const useDeleteAirflowTask = () => {
  return useMutation({
    mutationFn: (args: DeleteAirflowTaskArgs) => deleteAirflowTask(args)
  })
}

// Bulk update DAGs

const postBulkUpdateAirflowDag = async (
  type: 'import' | 'export' | 'custom',
  dagData: BulkUpdateAirflowDAG
) => {
  const response = await axiosInstance.post(`/airflow/dags/${type}`, dagData)
  return response.data
}

export const useBulkUpdateAirflowDag = () => {
  return useMutation<
    BulkUpdateAirflowDAG,
    Error,
    {
      type: 'import' | 'export' | 'custom'
      dagData: BulkUpdateAirflowDAG
    }
  >({
    mutationFn: ({ type, dagData }) => postBulkUpdateAirflowDag(type, dagData)
  })
}

// Bulk delete DAGs

type BulkDeleteArgs = {
  type: 'import' | 'export' | 'custom'
  bulkDeleteRowsPks: AirflowDagPk[]
}

const bulkDeleteAirflowDags = async ({
  type,
  bulkDeleteRowsPks
}: BulkDeleteArgs) => {
  const response = await axiosInstance.delete(`/airflow/dags/${type}`, {
    data: bulkDeleteRowsPks
  })
  return response.data
}

export const useBulkDeleteAirflowDags = () => {
  return useMutation({
    mutationFn: (args: BulkDeleteArgs) => bulkDeleteAirflowDags(args)
  })
}

// CONFIGURATION

const postGlobalConfig = async (config: ConfigGlobalWithIndex) => {
  const response = await axiosInstance.post('/config/updateConfig', config)

  return response.data
}

export const useUpdateGlobalConfig = () => {
  return useMutation({
    mutationFn: (globalConfigUpdated: ConfigGlobalWithIndex) => {
      return postGlobalConfig(globalConfigUpdated)
    }
  })
}

const postJDBCdriver = async (driver: JDBCdriversWithIndex) => {
  const response = await axiosInstance.post('/config/updateJDBCdriver', driver)

  return response.data
}

export const useUpdateJDBCdrivers = () => {
  return useMutation({
    mutationFn: (driverUpdated: JDBCdriversWithIndex) => {
      return postJDBCdriver(driverUpdated)
    }
  })
}
