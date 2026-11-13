import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import Button from '../../components/Button'

import { useQueryClient } from '@tanstack/react-query'
import { createExportDagData } from '../../utils/dataFunctions'
import {
  EditSetting,
  UiAirflowsExportData,
  UiAirflowsSearchFilter
} from '../../utils/interfaces'
import { useCreateAirflowDag } from '../../utils/mutations'
import { useAtom } from 'jotai'
import {
  airflowExportDagsPersistStateAtom,
  clearRowSelectionAtom
} from '../../atoms/atoms'
import AirflowSearchFilterDags from '../_airflowShared/AirflowSearchFilterDags'
import CreateAirflowModal from '../../components/modals/CreateAirflowModal'

interface AirflowExportActionsProps {
  dags: UiAirflowsExportData[] | undefined
}

function AirflowExportActions({ dags }: AirflowExportActionsProps) {
  const { mutate: createDAG } = useCreateAirflowDag()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [, setAirfloExportDagsPersistState] = useAtom(
    airflowExportDagsPersistStateAtom
  )
  const [, setClearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  const handleSave = (newExportAirflowSettings: EditSetting[]) => {
    const newExportAirflowData = createExportDagData(newExportAirflowSettings)

    createDAG(
      { type: 'export', dagData: newExportAirflowData },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', 'export']
          })
          console.log('Create DAG successful')
          setIsCreateModalOpen(false)
        },
        onError: (error) => {
          console.log('Error creating DAG', error.message)
        }
      }
    )
  }

  const handleShow = (uiFilters: UiAirflowsSearchFilter) => {
    const params = new URLSearchParams(location.search)

    const filterKeys: (keyof UiAirflowsSearchFilter)[] = [
      'name',
      'scheduleInterval',
      'autoRegenerateDag'
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
      setAirfloExportDagsPersistState(`/airflow/export?${orderedSearch}`)
      navigate(`/airflow/export?${orderedSearch}`, { replace: true })
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
          fontSize="14px"
        />
        <AirflowSearchFilterDags
          isSearchFilterOpen={openDropdown === 'searchFilter'}
          type="export"
          onToggle={(isSearchFilterOpen: boolean) =>
            handleDropdownToggle('searchFilter', isSearchFilterOpen)
          }
          onShow={handleShow}
          disabled={!dags || dags.length < 1}
        />
      </div>
      {isCreateModalOpen && (
        <CreateAirflowModal
          isCreateModalOpen={isCreateModalOpen}
          type="export"
          onSave={handleSave}
          onClose={() => setIsCreateModalOpen(false)}
        />
      )}
    </>
  )
}

export default AirflowExportActions
