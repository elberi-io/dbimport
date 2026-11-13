import { UseQueryResult, useQuery } from '@tanstack/react-query'
import axiosInstance from './axiosInstance'
import {
  AirflowsCustomData,
  AirflowsData,
  AirflowsExportData,
  AirflowsImportData,
  ConfigGlobal,
  Connection,
  ConnectionSearchFilter,
  Connections,
  CustomAirflowDAG,
  Database,
  DbTable,
  ErrorData,
  ExportAirflowDAG,
  ExportCnTable,
  ExportConnections,
  ExportDiscoverSearch,
  ExportDiscoverTable,
  ExportSearchFilter,
  ExportTable,
  HeadersRowInfo,
  ImportAirflowDAG,
  ImportDiscoverSearch,
  ImportDiscoverTable,
  ImportSearchFilter,
  JDBCdrivers,
  Table,
  UIExportCnTables,
  UIExportTable,
  UIExportTableWithoutEnum,
  UITable,
  UITableWithoutEnum,
  UiAirflowsCustomData,
  UiAirflowsExportData,
  UiAirflowsImportData,
  UiDbTable
} from './interfaces'
import { mapDisplayValue, mapEnumValue } from './nameMappings'
import {
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
  ValidateSource,
  ValidationMethod
} from './enums'
import {
  AxiosResponseHeaders,
  AxiosHeaderValue,
  RawAxiosResponseHeaders,
  AxiosError
} from 'axios'

// CONNECTION

// Get search connections

interface ConnectionsResponse {
  data: DbTable[]
  headers:
    | AxiosResponseHeaders
    | Partial<
        RawAxiosResponseHeaders & {
          Server: AxiosHeaderValue
          [key: string]: AxiosHeaderValue // Dynamic headers
        }
      >
}

interface SearchConnectionsResult {
  connections: Connections[]
  headersRowInfo: HeadersRowInfo
}

const getSearchConnections = async (filters: ConnectionSearchFilter) => {
  const response = await axiosInstance.post('/connection/search', filters)
  return {
    data: response.data,
    headers: response.headers
  }
}

export const useSearchConnections = (
  filters: ConnectionSearchFilter
): UseQueryResult<SearchConnectionsResult, Error> => {
  return useQuery({
    queryKey: ['connection', 'search', filters],
    queryFn: async () => {
      const { data, headers }: ConnectionsResponse = await getSearchConnections(
        filters
      )

      const headersRowInfo = {
        contentLength: headers['content-length'],
        contentMaxReturnedRows: headers['content-max-returned-rows'],
        contentRows: headers['content-rows'],
        contentTotalRows: headers['content-total-rows']
      }

      return { connections: data, headersRowInfo }
    },
    refetchOnWindowFocus: false
  })
}

// Get all connections

const getConnections = async (onlyNames?: boolean) => {
  const path =
    onlyNames === true ? '/connection?listOnlyName=true' : '/connection'
  const response = await axiosInstance.get(path)
  return response.data
}

export const useConnections = (
  onlyNames?: boolean
): UseQueryResult<Connections[], Error> => {
  const queryKey = onlyNames === true ? ['connection', 'names'] : ['connection']
  return useQuery({
    queryKey: queryKey,
    queryFn: () => getConnections(onlyNames)
  })
}

// Get connection

const getConnection = async (connection: string) => {
  const encodedConnection = encodeURIComponent(connection)
  const response = await axiosInstance.get(`/connection/${encodedConnection}`)
  return response.data
}

export const useConnection = (
  connection?: string
): UseQueryResult<Connection, AxiosError<ErrorData>> => {
  return useQuery({
    queryKey: ['connection', connection],
    queryFn: () => getConnection(connection!), // We are sure that connection is not null here because of the enabled flag
    enabled: !!connection
  })
}

// IMPORT

// Get databases for databases dropdown

const getDatabases = async () => {
  const response = await axiosInstance.get('/import/db')
  return response.data
}

export const useDatabases = (): UseQueryResult<Database[], Error> => {
  return useQuery({
    queryKey: ['databases'],
    queryFn: getDatabases,
    initialData: []
  })
}

// Get search filter import tables

interface ImportTablesResponse {
  data: DbTable[]
  headers:
    | AxiosResponseHeaders
    | Partial<
        RawAxiosResponseHeaders & {
          Server: AxiosHeaderValue
          [key: string]: AxiosHeaderValue // Dynamic headers
        }
      >
}

interface SearchImportTablesResult {
  tables: UiDbTable[]
  headersRowInfo: HeadersRowInfo
}

const getSearchImportTables = async (filters: ImportSearchFilter) => {
  const response = await axiosInstance.post('/import/search', filters)
  return {
    data: response.data,
    headers: response.headers
  }
}

export const useSearchImportTables = (
  filters: ImportSearchFilter | null,
  validationMode?: boolean
): UseQueryResult<SearchImportTablesResult, Error> => {
  return useQuery({
    queryKey:
      validationMode === true
        ? ['import', 'validation', filters?.database, filters?.table]
        : ['import', 'search', filters],
    queryFn: async () => {
      const { data, headers }: ImportTablesResponse =
        await getSearchImportTables(filters!) // We are sure that filters is not null here because of the enabled flag
      const enumMappedData = data.map(
        (row: {
          etlPhaseType: string
          importPhaseType: string
          importTool: string
          etlEngine: string
        }) => ({
          ...row,
          etlPhaseTypeDisplay: mapDisplayValue(
            'etlPhaseType',
            row.etlPhaseType
          ),
          importPhaseTypeDisplay: mapDisplayValue(
            'importPhaseType',
            row.importPhaseType
          ),
          importToolDisplay: mapDisplayValue('importTool', row.importTool),
          etlEngineDisplay: mapDisplayValue('etlEngine', row.etlEngine)
        })
      )

      const headersRowInfo = {
        contentLength: headers['content-length'],
        contentMaxReturnedRows: headers['content-max-returned-rows'],
        contentRows: headers['content-rows'],
        contentTotalRows: headers['content-total-rows']
      }

      return { tables: enumMappedData, headersRowInfo }
    },
    enabled: filters !== null, // Prevents query execution when filters is null
    refetchOnWindowFocus: false
  })
}

// Discover import tables

const postDiscoverImportTables = async (filters: ImportDiscoverSearch) => {
  const response = await axiosInstance.post('/import/discover/search', filters)
  return response.data
}

export const useDiscoverImportTables = (
  filters: ImportDiscoverSearch | null
): UseQueryResult<ImportDiscoverTable[], Error> => {
  return useQuery({
    queryKey: ['import', 'discover', filters],
    queryFn: async () => postDiscoverImportTables(filters!), // We are sure that filters is not null here because of the enabled flag
    enabled: filters !== null, // Prevents query execution when filters is null
    refetchOnWindowFocus: false
  })
}

// Get detailed table

const getTable = async (database: string, table: string) => {
  const encodedDatabase = encodeURIComponent(database)
  const encodedTable = encodeURIComponent(table)
  const response = await axiosInstance.get(
    `/import/table/${encodedDatabase}/${encodedTable}`
  )
  return response.data
}

export const fetchTableData = async (
  database: string,
  table: string
): Promise<UITable> => {
  if (!database || !table) {
    throw new Error(
      'Cannot fetch table because database or/and table params are not defined'
    )
  }
  const data: Table = await getTable(database, table)

  const dataWithEnumTypes: UITable = {
    ...data,
    importPhaseType: mapEnumValue(
      data.importPhaseType,
      Object.values(ImportType)
    ),
    etlPhaseType: mapEnumValue(data.etlPhaseType, Object.values(EtlType)),
    importTool: mapEnumValue(data.importTool, Object.values(ImportTool)),
    etlEngine: mapEnumValue(data.etlEngine, Object.values(EtlEngine)),
    validationMethod: mapEnumValue(
      data.validationMethod,
      Object.values(ValidationMethod)
    ),
    validateSource: mapEnumValue(
      data.validateSource,
      Object.values(ValidateSource)
    ),
    incrMode: mapEnumValue(data.incrMode, Object.values(IncrMode)),
    incrValidationMethod: mapEnumValue(
      data.incrValidationMethod,
      Object.values(IncrValidationMethod)
    ),
    mergeCompactionMethod: mapEnumValue(
      data.mergeCompactionMethod,
      Object.values(MergeCompactionMethod)
    )
  }

  return dataWithEnumTypes
}

export const useImportTable = (
  database?: string,
  table?: string
): UseQueryResult<UITable, AxiosError<ErrorData>> => {
  return useQuery({
    queryKey: ['import', database, table],
    queryFn: () => fetchTableData(database!, table!), // We are sure that database and table is not null here because of the enabled flag
    enabled: !!database && !!table
  })
}

// EXPORT

// Get connections with exports

const getExportConnections = async () => {
  const response = await axiosInstance.get('/export/connection')
  return response.data
}

export const useExportConnections = (): UseQueryResult<
  ExportConnections[],
  Error
> => {
  return useQuery({
    queryKey: ['export', 'connections'],
    queryFn: getExportConnections,
    initialData: []
  })
}

// Get export tables of a connection

interface ExportTablesResponse {
  data: ExportCnTable[]
  headers:
    | AxiosResponseHeaders
    | Partial<
        RawAxiosResponseHeaders & {
          Server: AxiosHeaderValue
          [key: string]: AxiosHeaderValue // Dynamic headers
        }
      >
}

interface SearchExportTablesResult {
  tables: UIExportCnTables[]
  headersRowInfo: HeadersRowInfo
}

const getSearchExportTables = async (filters: ExportSearchFilter) => {
  const response: ExportTablesResponse = await axiosInstance.post(
    '/export/search',
    filters
  )
  return {
    data: response.data,
    headers: response.headers
  }
}

export const useSearchExportTables = (
  filters: ExportSearchFilter | null,
  validationMode?: boolean
): UseQueryResult<SearchExportTablesResult, Error> => {
  return useQuery({
    queryKey:
      validationMode === true
        ? [
            'export',
            'validation',
            filters?.connection,
            filters?.targetTable,
            filters?.targetSchema
          ]
        : ['export', 'search', filters],
    queryFn: async () => {
      const { data, headers }: ExportTablesResponse =
        await getSearchExportTables(filters!) // We are sure that filters is not null here because of the enabled flag

      const enumMappedData = data.map((row) => ({
        ...row,
        exportTypeDisplay: mapDisplayValue(
          'exportType',
          row.exportType as string
        ),
        exportToolDisplay: mapDisplayValue(
          'exportTool',
          row.exportTool as string
        )
      }))

      const headersRowInfo = {
        contentLength: headers['content-length'],
        contentMaxReturnedRows: headers['content-max-returned-rows'],
        contentRows: headers['content-rows'],
        contentTotalRows: headers['content-total-rows']
      }

      return { tables: enumMappedData, headersRowInfo }
    },
    enabled: filters !== null, // Prevents query execution when filters is null
    refetchOnWindowFocus: false
  })
}

// Get detailed export table

const getExportTable = async (
  connection: string,
  targetSchema: string,
  targetTable: string
) => {
  const encodedConnection = encodeURIComponent(connection)
  const encodedTargetSchema = encodeURIComponent(targetSchema)
  const encodedTargetTable = encodeURIComponent(targetTable)

  const response = await axiosInstance.get(
    `/export/table/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}`
  )
  return response.data
}

export const fetchExportTableData = async (
  connection: string,
  targetSchema: string,
  targetTable: string
): Promise<UIExportTable> => {
  const data: ExportTable = await getExportTable(
    connection,
    targetSchema,
    targetTable
  )

  const dataWithEnumTypes: UIExportTable = {
    ...data,
    exportType: mapEnumValue(data.exportType, Object.values(ExportType)),
    exportTool: mapEnumValue(data.exportTool, Object.values(ExportTool)),
    validationMethod:
      typeof data.validationMethod === 'string'
        ? mapEnumValue(
            data.validationMethod,
            Object.values(ExportValidationMethod)
          )
        : null,
    incrValidationMethod:
      typeof data.incrValidationMethod === 'string'
        ? mapEnumValue(
            data.incrValidationMethod,
            Object.values(ExportIncrValidationMethod)
          )
        : null
  }

  return dataWithEnumTypes
}

export const useExportTable = (
  connection?: string,
  targetSchema?: string,
  targetTable?: string
): UseQueryResult<UIExportTable, AxiosError<ErrorData>> => {
  return useQuery({
    queryKey: ['export', connection, targetTable],
    queryFn: () =>
      fetchExportTableData(connection!, targetSchema!, targetTable!), // We are sure that database and table is not null here because of the enabled flag
    enabled: !!connection && !!targetSchema && !!targetTable
  })
}

// Discover export tables

const postDiscoverExportTables = async (filters: ExportDiscoverSearch) => {
  const response = await axiosInstance.post('/export/discover/search', filters)
  return response.data
}

export const useDiscoverExportTables = (
  filters: ExportDiscoverSearch | null
): UseQueryResult<ExportDiscoverTable[], Error> => {
  return useQuery({
    queryKey: ['export', 'discover', filters],
    queryFn: async () => postDiscoverExportTables(filters!), // We are sure that filters is not null here because of the enabled flag
    enabled: filters !== null, // Prevents query execution when filters is null
    refetchOnWindowFocus: false
  })
}

// Raw table, import and export
interface UseRawTableParams {
  type: 'import' | 'export'
  databaseOrConnection?: string // 'database' for import, 'connection' for export
  schema?: string // Used only for export
  table?: string
}

export const useRawTable = ({
  type,
  databaseOrConnection,
  schema,
  table
}: UseRawTableParams): UseQueryResult<
  UITableWithoutEnum | UIExportTableWithoutEnum,
  Error
> => {
  return useQuery({
    queryKey: [type, databaseOrConnection, table],
    queryFn: () => {
      if (type === 'import') {
        return getTable(databaseOrConnection!, table!)
      } else {
        return getExportTable(databaseOrConnection!, schema!, table!)
      }
    },
    enabled:
      !!databaseOrConnection && !!table && (type === 'import' || !!schema)
  })
}

// AIRFLOW

// Get all airflows

const getAllAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags')
  return response.data
}

export const useAllAirflows = (): UseQueryResult<AirflowsData[], Error> => {
  return useQuery({
    queryKey: ['airflows'],
    queryFn: getAllAirflows,
    initialData: []
  })
}

// // Get all type airflows WIP

// const getAirflows = async (type: string) => {
//   const response = await axiosInstance.get(`/airflow/dags/${type}`)
//   console.log('getAirflows response.data', response.data)
//   return response.data
// }

// export const useAirflows = (
//   type?: string
// ): UseQueryResult<
//   UiAirflowsImportData[] | UiAirflowsExportData[] | UiAirflowsCustomData[],
//   Error
// > => {
//   return useQuery({
//     queryKey: ['airflows', type],
//     queryFn: async () => {
//       const data:
//         | AirflowsImportData[]
//         | AirflowsExportData[]
//         | AirflowsCustomData[] = await getAirflows(type!) // We are sure that type is not null here because of the enabled flag
//       const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
//         ...row,
//         autoRegenerateDagDisplay:
//           row.autoRegenerateDag === true ? 'True' : 'False'
//       }))

//       return mappedData
//     },
//     enabled: !!type
//   })
// }

// Get all import airflows

const getImportAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags/import')
  return response.data
}

export const useImportAirflows = (): UseQueryResult<
  UiAirflowsImportData[],
  Error
> => {
  return useQuery({
    queryKey: ['airflows', 'import'],
    queryFn: async () => {
      const data: AirflowsImportData[] = await getImportAirflows()
      const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
        ...row,
        autoRegenerateDagDisplay:
          row.autoRegenerateDag === true ? 'True' : 'False'
      }))

      return mappedData
    },
    initialData: [],
    refetchOnWindowFocus: false
  })
}

// Get all export airflows

const getExportAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags/export')
  return response.data
}

export const useExportAirflows = (): UseQueryResult<
  UiAirflowsExportData[],
  Error
> => {
  return useQuery({
    queryKey: ['airflows', 'export'],
    queryFn: async () => {
      const data: AirflowsExportData[] = await getExportAirflows()
      const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
        ...row,
        autoRegenerateDagDisplay:
          row.autoRegenerateDag === true ? 'True' : 'False'
      }))

      return mappedData
    },
    initialData: [],
    refetchOnWindowFocus: false
  })
}

// Get all custom airflows

const getCustomAirflows = async () => {
  const response = await axiosInstance.get('/airflow/dags/custom')
  return response.data
}

export const useCustomAirflows = (): UseQueryResult<
  UiAirflowsCustomData[],
  Error
> => {
  return useQuery({
    queryKey: ['airflows', 'custom'],
    queryFn: async () => {
      const data: AirflowsCustomData[] = await getCustomAirflows()
      const mappedData = data.map((row: { autoRegenerateDag: boolean }) => ({
        ...row,
        autoRegenerateDagDisplay:
          row.autoRegenerateDag === true ? 'True' : 'False'
      }))

      return mappedData
    },
    initialData: [],
    refetchOnWindowFocus: false
  })
}

// Get an airlow DAG

const getAirflowDAG = async (type: string, dagName: string) => {
  const encodedDagName = encodeURIComponent(dagName)

  const response = await axiosInstance.get(
    `/airflow/dags/${type}/${encodedDagName}`
  )
  return response.data
}

export const useAirflowDAG = (
  type?: 'import' | 'export' | 'custom',
  dagName?: string
): UseQueryResult<
  ImportAirflowDAG | ExportAirflowDAG | CustomAirflowDAG,
  AxiosError<ErrorData>
> => {
  return useQuery({
    queryKey: ['airflows', type, dagName],
    queryFn: () => getAirflowDAG(type!, dagName!), // We are sure that type and dagName is not null here because of the enabled flag
    enabled: !!type && !!dagName
  })
}

// VERSION

// Get server status and version

const getStatusAndVersion = async () => {
  const response = await axiosInstance.get('/status')
  return response.data
}

export const useGetStatusAndVersion = (): UseQueryResult<
  { status: string; version: string },
  Error
> => {
  return useQuery({
    queryKey: ['status'],
    queryFn: () => getStatusAndVersion()
  })
}

// CONFIGURATION

const getGlobalConfig = async () => {
  const response = await axiosInstance.get('/config/getConfig')
  return response.data
}

export const useGlobalConfig = (): UseQueryResult<ConfigGlobal, Error> => {
  return useQuery({
    queryKey: ['configuration', 'global'],
    queryFn: () => getGlobalConfig(),
    refetchOnWindowFocus: false
  })
}

const getJDBCdrivers = async () => {
  const response = await axiosInstance.get('/config/getJDBCdrivers')
  return response.data
}

export const useJDBCDrivers = (): UseQueryResult<JDBCdrivers[], Error> => {
  return useQuery({
    queryKey: ['configuration', 'jdbcdrivers'],
    queryFn: () => getJDBCdrivers(),
    refetchOnWindowFocus: false
  })
}
