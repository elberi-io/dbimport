import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import CreateImportTableModal from '../../components/modals/CreateImportTableModal'
import { useQueryClient } from '@tanstack/react-query'
import { createTableData } from '../../utils/dataFunctions'
import {
  EditSetting,
  ImportSearchFilter,
  UiDbTable,
  UiImportSearchFilter
} from '../../utils/interfaces'
import { useCreateImportTable } from '../../utils/mutations'
import { useAtom } from 'jotai'
import {
  clearRowSelectionAtom,
  importPersistStateAtom
} from '../../atoms/atoms'
import DropdownActions from '../../components/DropdownActions'
import DiscoverIcon from '../../assets/icons/DiscoverIcon'
import PlusIcon from '../../assets/icons/PlusIcon'
import DiscoverImportModal from '../../components/modals/DiscoverImportModal'
import ImportSearchFilterTables from './ImportSearchFilterTables'

interface ImportActionsProps {
  tables: UiDbTable[] | undefined
  filters: ImportSearchFilter
}

function ImportActions({ tables, filters }: ImportActionsProps) {
  const { mutate: createTable } = useCreateImportTable()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [, setImportPersistState] = useAtom(importPersistStateAtom)
  const [, setClearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [isDiscoverModalOpen, setIsDiscoverModalOpen] = useState(false)

  const mostCommonConnection = useMemo(() => {
    if (!tables || tables.length === 0) return null
    const connectionCounts = tables.reduce((acc, row) => {
      const connection = row.connection
      if (connection) {
        acc[connection] = (acc[connection] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    const keys = Object.keys(connectionCounts)
    if (keys.length === 0) return null

    return keys.reduce((a, b) =>
      connectionCounts[a] > connectionCounts[b] ? a : b
    )
  }, [tables])

  const mostCommonDatabase = useMemo(() => {
    if (!tables || tables.length === 0) return null
    const databaseCounts = tables.reduce((acc, row) => {
      const database = row.database
      if (database) {
        acc[database] = (acc[database] || 0) + 1
      }
      return acc
    }, {} as { [key: string]: number })

    const keys = Object.keys(databaseCounts)
    if (keys.length === 0) return null

    return keys.reduce((a, b) =>
      databaseCounts[a] > databaseCounts[b] ? a : b
    )
  }, [tables])

  const handleSave = (newTableData: EditSetting[]) => {
    const newTable = createTableData(newTableData)

    createTable(newTable, {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: ['import', 'search', filters]
        })
        queryClient.invalidateQueries({
          queryKey: ['databases']
        })
        console.log('Create successful')
        setIsCreateModalOpen(false)
      },
      onError: (error) => {
        console.log('Error creating table', error.message)
      }
    })
  }

  const handleShow = (uiFilters: UiImportSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: [keyof UiImportSearchFilter, string][] = [
      ['connection', 'connection'],
      ['database', 'database'],
      ['table', 'table'],
      ['includeInAirflow', 'includeInAirflow'],
      ['lastUpdateFromSource', 'lastUpdateFromSource'],
      ['importPhaseType', 'importType'],
      ['importTool', 'importTool'],
      ['etlPhaseType', 'etlType'],
      ['etlEngine', 'etlEngine']
    ]

    filterKeys.forEach(([key, paramName]) => {
      const value = uiFilters[key]
      if (value !== null && value !== undefined && String(value).length > 0) {
        params.set(paramName, String(value))
      } else {
        params.delete(paramName)
      }
    })

    const orderedSearch = filterKeys
      .map(([, paramName]) =>
        params.has(paramName)
          ? `${paramName}=${params.get(paramName) || ''}`
          : null
      )
      .filter((param) => param !== null)
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setImportPersistState(`/import?${orderedSearch}`)
      navigate(`/import?${orderedSearch}`, { replace: true })
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  return (
    <>
      <div className="header-buttons">
        <DropdownActions
          isDropdownActionsOpen={openDropdown === 'dropdownActions'}
          onToggle={(isDropdownActionsOpen: boolean) =>
            handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
          }
          maxWidth={90}
          items={[
            {
              icon: <PlusIcon />,
              label: 'Create table',
              onClick: () => {
                setClearRowSelectionTrigger((prev) => prev + 1)
                setIsCreateModalOpen(true)
                setOpenDropdown(null)
              }
            },
            {
              icon: <DiscoverIcon />,
              label: `Discover and Add tables`,
              onClick: () => {
                setClearRowSelectionTrigger((prev) => prev + 1)
                setIsDiscoverModalOpen(true)
                setOpenDropdown(null)
              }
            }
          ]}
          disabled={!tables}
        />
        <ImportSearchFilterTables
          isSearchFilterOpen={openDropdown === 'searchFilter'}
          onToggle={(isSearchFilterOpen: boolean) =>
            handleDropdownToggle('searchFilter', isSearchFilterOpen)
          }
          onShow={handleShow}
          disabled={!tables}
        />
      </div>
      {isCreateModalOpen && (
        <CreateImportTableModal
          isCreateModalOpen={isCreateModalOpen}
          prefilledDatabase={mostCommonDatabase ? mostCommonDatabase : null}
          prefilledConnection={
            mostCommonConnection ? mostCommonConnection : null
          }
          onSave={handleSave}
          onClose={() => setIsCreateModalOpen(false)}
        />
      )}

      {isDiscoverModalOpen && (
        <DiscoverImportModal
          title="Discover and Add tables"
          isDiscoverModalOpen={isDiscoverModalOpen}
          onClose={() => setIsDiscoverModalOpen(false)}
        />
      )}
    </>
  )
}

export default ImportActions
