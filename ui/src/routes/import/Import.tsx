import { useLocation, useNavigate } from 'react-router-dom'
import { useSearchImportTables } from '../../utils/queries'
import './Import.scss'
import '../../components/Loading.scss'
import { useCallback, useEffect, useMemo } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { ImportSearchFilter } from '../../utils/interfaces'
import DbTables from './DbTables'

import { reverseMapEnumValue } from '../../utils/nameMappings'
import ImportActions from './ImportActions'

// const checkboxFilters = [
//   {
//     title: 'Import Type',
//     accessor: 'importPhaseType',
//     values: ['Full', 'Incremental', 'Oracle Flashback', 'MSSQL Change Tracking']
//   },
//   {
//     title: 'ETL Type',
//     accessor: 'etlPhaseType',
//     values: [
//       'Truncate and Insert',
//       'Insert only',
//       'Merge',
//       'Merge with History Audit',
//       'Only create external table',
//       'None'
//     ]
//   },
//   {
//     title: 'Import Tool',
//     accessor: 'importTool',
//     values: ['Spark', 'Sqoop']
//   },
//   {
//     title: 'ETL Engine',
//     accessor: 'etlEngine',
//     values: ['Hive', 'Spark']
//   }
// ]

// const radioFilters = [
//   {
//     title: 'Last update from source',
//     accessor: 'lastUpdateFromSource',
//     radioName: 'timestamp',
//     badgeContent: ['D', 'W', 'M', 'Y'],
//     values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
//   },
//   {
//     title: 'Include in Airflow',
//     accessor: 'includeInAirflow',
//     radioName: 'includeInAirflow',
//     badgeContent: ['t', 'f'],
//     values: ['True', 'False']
//   }
// ]

// const radioFilters = [
//   {
//     title: 'Last update from source',
//     accessor: 'lastUpdateFromSource',
//     radioName: 'timestamp',
//     badgeContent: ['D', 'W', 'M', 'Y'],
//     values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
//   }
// ]

function Import() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = [
    'connection',
    'database',
    'table',
    'includeInAirflow',
    'lastUpdateFromSource',
    'importType',
    'importTool',
    'etlType',
    'etlEngine'
  ]
  const allSearchParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allSearchParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/import', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allSearchParams, navigate])

  const connection = query.get('connection') || null
  const database = query.get('database') || null
  const table = query.get('table') || null
  const includeInAirflow = query.has('includeInAirflow')
    ? query.get('includeInAirflow') === 'True'
      ? true
      : query.get('includeInAirflow') === 'False'
      ? false
      : null
    : null
  const lastUpdateFromSource = query.get('lastUpdateFromSource') || null
  const importPhaseType = query.get('importType')
    ? reverseMapEnumValue(
        'import',
        'importType',
        query.get('importType') as string,
        true
      )
    : null
  const importTool = query.get('importTool')
    ? reverseMapEnumValue(
        'import',
        'importTool',
        query.get('importTool') as string,
        true
      )
    : null
  const etlPhaseType = query.get('etlType')
    ? reverseMapEnumValue(
        'import',
        'etlType',
        query.get('etlType') as string,
        true
      )
    : null
  const etlEngine = query.get('etlEngine')
    ? reverseMapEnumValue(
        'import',
        'etlEngine',
        query.get('etlEngine') as string,
        true
      )
    : null

  const filters: ImportSearchFilter = useMemo(
    () => ({
      connection,
      database,
      table,
      includeInAirflow,
      importPhaseType,
      importTool,
      etlPhaseType,
      etlEngine
    }),
    [
      connection,
      database,
      table,
      includeInAirflow,
      importPhaseType,
      importTool,
      etlPhaseType,
      etlEngine
    ]
  )

  const { data, isLoading: isSearchLoading } = useSearchImportTables(filters)

  // const handleSelect = (filterKey: string, items: string[]) => {
  //   setSelectedFilters((prevFilters) => ({
  //     ...prevFilters,
  //     [filterKey]: items
  //   }))
  // }

  const parseTimestamp = (timestamp: string | null): Date | null => {
    if (!timestamp) {
      return null
    }

    try {
      const isoDateString = timestamp.replace(' ', 'T') + 'Z'
      return new Date(isoDateString)
    } catch (error) {
      console.error('Failed to parse timestamp:', error)
      return null
    }
  }

  const getDateRange = useCallback((selection: string) => {
    const now = new Date()
    switch (selection) {
      case 'Last Day':
        return new Date(now.setDate(now.getDate() - 1))
      case 'Last Week':
        return new Date(now.setDate(now.getDate() - 7))
      case 'Last Month':
        return new Date(now.setMonth(now.getMonth() - 1))
      case 'Last Year':
        return new Date(now.setFullYear(now.getFullYear() - 1))
      default:
        return new Date(0)
    }
  }, [])

  const filteredData = useMemo(() => {
    if (!data || !Array.isArray(data.tables)) return []
    return data.tables.filter((row) => {
      const rowDate = parseTimestamp(row.lastUpdateFromSource)

      // return [...checkboxFilters, ...radioFilters].every((filter) => {

      if (lastUpdateFromSource === null) return true

      const selectedRange = lastUpdateFromSource
      const startDate = getDateRange(selectedRange)

      if (!rowDate) return false
      return rowDate >= startDate

      // if (filter.accessor === 'includeInAirflow') {
      //   const airflowValue = row[filter.accessor] === true ? 'True' : 'False'
      //   return selectedItems.includes(airflowValue)
      // }

      // const accessorKey = filter.accessor as keyof typeof row
      // const displayKey = `${String(accessorKey)}Display` as keyof typeof row
      // const rowValue = (row[displayKey] ?? row[accessorKey]) as string

      // return selectedItems.includes(rowValue)
    })
  }, [data, lastUpdateFromSource, getDateRange])

  return (
    <>
      <ViewBaseLayout>
        <div className="header-container">
          <h1>Import</h1>
          <ImportActions tables={data?.tables} filters={filters} />
        </div>
        {/* <div className="filters"> */}
        {/* {checkboxFilters.map((filter, index) => (
            <DropdownCheckbox
              key={index}
              items={filter.values}
              title={filter.title}
              selectedItems={selectedFilters[filter.accessor] || []}
              onSelect={(items) => handleSelect(filter.accessor, items)}
              isOpen={openDropdown === filter.accessor}
              onToggle={(isOpen) =>
                handleDropdownToggle(filter.accessor, isOpen)
              }
            />
          ))} */}
        {/* {radioFilters.map((filter, index) => (
            <DropdownRadio
              key={index}
              items={filter.values}
              title={filter.title}
              radioName={filter.radioName}
              badgeContent={filter.badgeContent}
              selectedItem={selectedFilters[filter.accessor]?.[0] || null}
              onSelect={(items) => handleSelect(filter.accessor, items)}
              isOpen={openDropdown === filter.accessor}
              onToggle={(isOpen) =>
                handleDropdownToggle(filter.accessor, isOpen)
              }
            />
          ))}
        </div> */}

        {data &&
        data.tables &&
        Array.isArray(data.tables) &&
        data.headersRowInfo ? (
          <>
            <DbTables
              data={filteredData}
              headersRowInfo={data.headersRowInfo}
              queryKeyFilters={filters}
              isLoading={isSearchLoading}
            />
          </>
        ) : isSearchLoading ? (
          <div className="loading">Loading...</div>
        ) : (
          <div className="text-block">
            <p>No import tables yet.</p>
          </div>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Import
