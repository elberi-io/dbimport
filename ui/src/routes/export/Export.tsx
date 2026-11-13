import { useLocation, useNavigate } from 'react-router-dom'
import { useSearchExportTables } from '../../utils/queries'
import '../import/Import.scss'
import { useCallback, useEffect, useMemo } from 'react'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import {
  ExportSearchFilter
  // ExportSearchFilter,
} from '../../utils/interfaces'

// import DropdownCheckbox from '../../components/DropdownCheckbox'
// import DropdownRadio from '../../components/DropdownRadio'
import ExportCnTables from './ExportCnTables'
import { reverseMapEnumValue } from '../../utils/nameMappings'
import ExportActions from './ExportActions'

// const checkboxFilters = [
//   {
//     title: 'Export Type',
//     accessor: 'exportType',
//     values: ['Full', 'Incremental']
//   },
//   {
//     title: 'Export Tool',
//     accessor: 'exportTool',
//     values: ['Spark', 'Sqoop']
//   }
// ]

// const radioFilters = [
//   {
//     title: 'Last update from source',
//     accessor: 'lastUpdateFromHive  ',
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
//     accessor: 'lastUpdateFromHive  ',
//     radioName: 'timestamp',
//     badgeContent: ['D', 'W', 'M', 'Y'],
//     values: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
//   }
// ]

function Export() {
  const location = useLocation()
  const navigate = useNavigate()
  const query = new URLSearchParams(location.search)

  const validParams = [
    'connection',
    'targetTable',
    'targetSchema',
    'includeInAirflow',
    'lastUpdateFromHive',
    'exportType',
    'exportTool'
  ]
  const allSearchParams = Array.from(query.keys())

  useEffect(() => {
    const hasInvalidParams = allSearchParams.some(
      (param) => !validParams.includes(param)
    )
    if (hasInvalidParams) {
      navigate('/export', { replace: true })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allSearchParams, navigate])

  const connection = query.get('connection') || null
  const targetTable = query.get('targetTable') || null
  const targetSchema = query.get('targetSchema') || null
  const includeInAirflow = query.has('includeInAirflow')
    ? query.get('includeInAirflow') === 'True'
      ? true
      : query.get('includeInAirflow') === 'False'
      ? false
      : null
    : null
  const lastUpdateFromHive = query.get('lastUpdateFromHive') || null
  const exportType = query.get('exportType')
    ? reverseMapEnumValue(
        'export',
        'exportType',
        query.get('exportType') as string,
        true
      )
    : null
  const exportTool = query.get('exportTool')
    ? reverseMapEnumValue(
        'export',
        'exportTool',
        query.get('exportTool') as string,
        true
      )
    : null

  const filters: ExportSearchFilter = useMemo(
    () => ({
      connection,
      targetTable,
      targetSchema,
      includeInAirflow,
      exportType,
      exportTool
    }),
    [
      connection,
      targetTable,
      targetSchema,
      includeInAirflow,
      exportType,
      exportTool
    ]
  )

  const { data, isLoading: isSearchLoading } = useSearchExportTables(filters)

  // const [selectedFilters, setSelectedFilters] = useAtom(exportCnListFiltersAtom)

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
      const rowDate = parseTimestamp(row.lastUpdateFromHive)

      // return [...checkboxFilters, ...radioFilters].every((filter) => {

      if (lastUpdateFromHive === null) return true

      const selectedRange = lastUpdateFromHive
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
  }, [data, lastUpdateFromHive, getDateRange])
  // const filteredData = useMemo(() => {
  //   if (!data || !Array.isArray(data.tables)) return []
  //   return data.tables.filter((row) => {
  //     const rowDate = parseTimestamp(row.lastUpdateFromHive)

  //     // return [...checkboxFilters, ...radioFilters].every((filter) => {
  //     return [...radioFilters].every((filter) => {
  //       const selectedItems =
  //         selectedFilters[filter.accessor]?.map((value) => value) || []

  //       if (selectedItems.length === 0) return true

  //       // Handling the date filter separately
  //       if (filter.accessor === 'lastUpdateFromHive') {
  //         const selectedRange = selectedItems[0]
  //         const startDate = getDateRange(selectedRange)

  //         if (!rowDate) return false
  //         return rowDate >= startDate
  //       }

  //       // if (filter.accessor === 'includeInAirflow') {
  //       //   const airflowValue = row[filter.accessor] === true ? 'True' : 'False'
  //       //   return selectedItems.includes(airflowValue)
  //       // }

  //       const accessorKey = filter.accessor as keyof typeof row
  //       const displayKey = `${String(accessorKey)}Display` as keyof typeof row
  //       const rowValue = (row[displayKey] ?? row[accessorKey]) as string

  //       return selectedItems.includes(rowValue)
  //     })
  //   })
  // }, [data, selectedFilters, getDateRange])

  return (
    <>
      <ViewBaseLayout>
        <div className="header-container">
          <h1>Export</h1>
          <ExportActions tables={data?.tables} filters={filters} />
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
            <ExportCnTables
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
            <p>No export tables yet.</p>
          </div>
        )}
      </ViewBaseLayout>
    </>
  )
}

export default Export
