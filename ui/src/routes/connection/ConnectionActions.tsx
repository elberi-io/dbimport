import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Button from '../../components/Button'
import { useQueryClient } from '@tanstack/react-query'
import {
  Connections,
  EditSetting,
  UiConnectionSearchFilter
} from '../../utils/interfaces'
import { useAtom } from 'jotai'
import {
  clearRowSelectionAtom,
  connectionPersistStateAtom
} from '../../atoms/atoms'
import ConnectionSearchFilterCns from './ConnectionSearchFilterCns'
import CreateConnectionModal from '../../components/modals/CreateConnectionModal'
import { createConnectionData } from '../../utils/dataFunctions'
import { useCreateOrUpdateConnection } from '../../utils/mutations'

interface ConnectionActionsProps {
  connections: Connections[] | undefined
}

function ConnectionActions({ connections }: ConnectionActionsProps) {
  const { mutate: createConnection } = useCreateOrUpdateConnection()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [, setConnectionPersistState] = useAtom(connectionPersistStateAtom)
  const [, setClearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  const handleSave = (newConnectionSettings: EditSetting[]) => {
    const newConnectionData = createConnectionData(newConnectionSettings)

    createConnection(newConnectionData, {
      onSuccess: () => {
        // For getting fresh data from database to the cache
        queryClient.invalidateQueries({
          queryKey: ['connection', 'search'],
          exact: false
        })
        queryClient.invalidateQueries({
          queryKey: ['connection'],
          exact: true
        })
        console.log('Create connection successful')
        setIsCreateModalOpen(false)
      },
      onError: (error) => {
        console.log('Error creating connection', error.message)
      }
    })
  }

  const handleShow = (uiFilters: UiConnectionSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof UiConnectionSearchFilter)[] = [
      'name',
      'connectionString',
      'serverType'
    ]

    filterKeys.forEach((key) => {
      const value = uiFilters[key]
      if (value !== null && value !== undefined && String(value).length > 0) {
        params.set(key, String(value))
      } else {
        params.delete(key)
      }
    })

    const orderedSearch = filterKeys
      .map((key) =>
        params.has(key) ? `${key}=${params.get(key) || ''}` : null
      )
      .filter((param) => param !== null)
      .join('&')

    // Only updates and navigates if query has changed
    if (orderedSearch !== location.search.slice(1)) {
      setConnectionPersistState(`/connection?${orderedSearch}`)
      navigate(`/connection?${orderedSearch}`, { replace: true })
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
        <Button
          title="+ Create"
          onClick={() => {
            setClearRowSelectionTrigger((prev) => prev + 1)

            setIsCreateModalOpen(true)
          }}
          fontFamily={`'Work Sans Variable', sans-serif`}
          fontSize="14px"
        />

        <ConnectionSearchFilterCns
          isSearchFilterOpen={openDropdown === 'searchFilter'}
          onToggle={(isSearchFilterOpen: boolean) =>
            handleDropdownToggle('searchFilter', isSearchFilterOpen)
          }
          onShow={handleShow}
          disabled={!connections}
        />
      </div>
      {isCreateModalOpen && (
        <CreateConnectionModal
          isCreateModalOpen={isCreateModalOpen}
          onSave={handleSave}
          onClose={() => setIsCreateModalOpen(false)}
        />
      )}
    </>
  )
}

export default ConnectionActions
