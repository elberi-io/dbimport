import {
  AirflowTask,
  BaseAirflowDAG,
  Column,
  CustomAirflowDAG,
  EditSetting,
  ExportAirflowDAG,
  ImportAirflowDAG,
  AirflowWithDynamicKeys
} from '../../utils/interfaces'
import TableList from '../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import cloneDeep from 'lodash/cloneDeep'
import { useAirflowDAG } from '../../utils/queries'
import EditTableModal from '../../components/modals/EditTableModal'
import { useNavigate, useParams } from 'react-router-dom'
import '../../components/Loading.scss'
import { airflowTaskRowDataEdit } from '../../utils/cardRenderFormatting'
import {
  updateCustomDagData,
  updateExportDagData,
  updateImportDagData
} from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useDeleteAirflowTask,
  useUpdateAirflowDag
} from '../../utils/mutations'
import Button from '../../components/Button'
import CreateAirflowTaskModal from '../../components/modals/CreateAirflowTaskModal'
import CopyAirflowTaskModal from '../../components/modals/CopyAirflowTaskModal'
import DeleteModal from '../../components/modals/DeleteModal'

function AirflowTasks({ type }: { type: 'import' | 'export' | 'custom' }) {
  const { dagName } = useParams<{
    dagName: string
  }>()

  const navigate = useNavigate()

  const {
    data: originalDagData,
    isLoading,
    isError,
    error
  } = useAirflowDAG(type, dagName)
  const queryClient = useQueryClient()
  const { mutate: updateDag } = useUpdateAirflowDag()
  const { mutate: deleteAirflowTask } = useDeleteAirflowTask()

  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [isCreateModalOpen, setCreateModalOpen] = useState(false)

  const [isCopyTaskModalOpen, setIsCopyTaskModalOpen] = useState(false)
  const [selectedCopyRow, setSelectedCopyRow] = useState<AirflowTask>()
  const taskName = selectedCopyRow ? selectedCopyRow.name : null

  const [selectedDeleteRow, setSelectedDeleteRow] = useState<AirflowTask>()
  const [selectedRow, setSelectedRow] = useState<EditSetting[] | []>([])
  const [rowIndex, setRowIndex] = useState<number>()
  const [dataRefreshTrigger, setDataRefreshTrigger] = useState(0)
  const [rowSelection, setRowSelection] = useState({})

  const tasksData = useMemo(
    () => [...(originalDagData?.tasks || [])],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [originalDagData, dataRefreshTrigger]
  )

  const columns: Column<AirflowTask>[] = useMemo(
    () => [
      { header: 'Task Name', accessor: 'name' },
      { header: 'Type', accessor: 'type' },
      { header: 'Placement', accessor: 'placement' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Airflow Pool', accessor: 'airflowPool' },
      { header: 'Airflow Priority', accessor: 'airflowPriority' },
      {
        header: 'Include in Airflow',
        accessor: 'includeInAirflow'
      },
      {
        header: 'Task Dependency Downstream',
        accessor: 'taskDependencyDownstream'
      },
      {
        header: 'Task Dependency Upstream',
        accessor: 'taskDependencyUpstream'
      },
      { header: 'Task Config', accessor: 'taskConfig' },
      {
        header: 'Sensor Poke Interval',
        accessor: 'sensorPokeInterval'
      },
      {
        header: 'Sensor Timeout Minutes',
        accessor: 'sensorTimeoutMinutes'
      },
      {
        header: 'Sensor Connection',
        accessor: 'sensorConnection'
      },
      { header: 'Sensor Soft Fail', accessor: 'sensorSoftFail' },
      { header: 'Sudo User', accessor: 'sudoUser' },
      { header: 'Actions', isAction: 'copyAndDelete' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: AirflowTask, rowIndex: number | undefined) => {
      if (!originalDagData) {
        return
      }

      const rowData: EditSetting[] = airflowTaskRowDataEdit(row)

      setRowIndex(rowIndex)
      setSelectedRow(rowData)
      setIsEditModalOpen(true)
    },
    [originalDagData]
  )

  if (isError) {
    if (error.status === 404) {
      console.log(
        `GET DAG: ${error.message} ${error.response?.statusText}, re-routing to /airflow/${type}`
      )
      navigate(`/airflow/${type}`, { replace: true })
    }
    return <div className="error">Server error occurred.</div>
  }

  if (!originalDagData && !isError)
    return <div className="loading">Loading...</div>

  const handleCopyIconClick = (
    row: AirflowTask,
    rowIndex: number | undefined
  ) => {
    setRowIndex(rowIndex)
    setSelectedCopyRow(row)
    setIsCopyTaskModalOpen(true)
  }

  const handleSave = (updatedSettings: EditSetting[]) => {
    const dagDataCopy = { ...originalDagData }

    let editedDagData:
      | ImportAirflowDAG
      | BaseAirflowDAG
      | ExportAirflowDAG
      | null = null
    if (
      type === 'import' &&
      typeof rowIndex !== 'undefined' &&
      rowIndex >= 0 &&
      rowIndex < dagDataCopy.tasks.length
    ) {
      editedDagData = updateImportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ImportAirflowDAG>,
        updatedSettings,
        null,
        true,
        rowIndex
      )
    } else if (
      type === 'export' &&
      typeof rowIndex !== 'undefined' &&
      rowIndex >= 0 &&
      rowIndex < dagDataCopy.tasks.length
    ) {
      editedDagData = updateExportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ExportAirflowDAG>,
        updatedSettings,
        null,
        true,
        rowIndex
      )
    } else if (
      type === 'custom' &&
      typeof rowIndex !== 'undefined' &&
      rowIndex >= 0 &&
      rowIndex < dagDataCopy.tasks.length
    ) {
      editedDagData = updateCustomDagData(
        dagDataCopy as AirflowWithDynamicKeys<CustomAirflowDAG>,
        updatedSettings,
        null,
        true,
        rowIndex
      )
    }

    if (editedDagData && type) {
      queryClient.setQueryData(['airflows', type, dagName], editedDagData)
      updateDag(
        { type, dagData: editedDagData },
        {
          onSuccess: () => {
            queryClient.invalidateQueries({
              queryKey: ['airflows', type, dagName]
            }) // For getting fresh data from database to the cache
            setDataRefreshTrigger((prev) => prev + 1)
            console.log('Update successful')
            setIsEditModalOpen(false)
          },
          onError: (error) => {
            queryClient.setQueryData(
              ['airflows', type, dagName],
              originalDagData
            )

            console.log('Error updating DAG', error.message)
          }
        }
      )
    }
  }

  const handleSaveCreateTask = (updatedSettings: EditSetting[]) => {
    const dagDataCopy = { ...originalDagData }

    let editedDagData:
      | ImportAirflowDAG
      | BaseAirflowDAG
      | ExportAirflowDAG
      | null = null
    if (type === 'import') {
      editedDagData = updateImportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ImportAirflowDAG>,
        updatedSettings,
        'create'
      )
    } else if (type === 'export') {
      editedDagData = updateExportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ExportAirflowDAG>,
        updatedSettings,
        'create'
      )
    } else if (type === 'custom') {
      editedDagData = updateCustomDagData(
        dagDataCopy as AirflowWithDynamicKeys<CustomAirflowDAG>,
        updatedSettings,
        'create'
      )
    }

    if (editedDagData && type) {
      queryClient.setQueryData(['airflows', type, dagName], editedDagData)
      updateDag(
        { type, dagData: editedDagData },
        {
          onSuccess: () => {
            queryClient.invalidateQueries({
              queryKey: ['airflows', type, dagName]
            }) // For getting fresh data from database to the cache
            setDataRefreshTrigger((prev) => prev + 1)
            console.log('Update successful')
            setIsEditModalOpen(false)
          },
          onError: (error) => {
            queryClient.setQueryData(
              ['airflows', type, dagName],
              originalDagData
            )

            console.log('Error updating DAG', error.message)
          }
        }
      )
    }
  }

  const handleSaveCopyTask = (updatedSettings: EditSetting[]) => {
    const dagDataCopy = cloneDeep(originalDagData)
    let editedDagData:
      | ImportAirflowDAG
      | BaseAirflowDAG
      | ExportAirflowDAG
      | null = null
    if (type === 'import' && typeof rowIndex !== 'undefined' && rowIndex >= 0) {
      editedDagData = updateImportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ImportAirflowDAG>,
        updatedSettings,
        'copy',
        false,
        rowIndex
      )
    } else if (
      type === 'export' &&
      typeof rowIndex !== 'undefined' &&
      rowIndex >= 0
    ) {
      editedDagData = updateExportDagData(
        dagDataCopy as AirflowWithDynamicKeys<ExportAirflowDAG>,
        updatedSettings,
        'copy',
        false,
        rowIndex
      )
    } else if (
      type === 'custom' &&
      typeof rowIndex !== 'undefined' &&
      rowIndex >= 0
    ) {
      editedDagData = updateCustomDagData(
        dagDataCopy as AirflowWithDynamicKeys<CustomAirflowDAG>,
        updatedSettings,
        'copy',
        false,
        rowIndex
      )
    }

    if (editedDagData && type) {
      queryClient.setQueryData(['airflows', type, dagName], editedDagData)
      updateDag(
        { type, dagData: editedDagData },
        {
          onSuccess: () => {
            queryClient.invalidateQueries({
              queryKey: ['airflows', type, dagName]
            }) // For getting fresh data from database to the cache
            setDataRefreshTrigger((prev) => prev + 1)
            console.log('Update successful')
            setIsEditModalOpen(false)
          },
          onError: (error) => {
            queryClient.setQueryData(
              ['airflows', type, dagName],
              originalDagData
            )

            console.log('Error updating DAG', error.message)
          }
        }
      )
    }
  }

  const handleDeleteIconClick = (row: AirflowTask) => {
    setShowDeleteConfirmation(true)
    setSelectedDeleteRow(row)
  }

  const handleDelete = async (row: AirflowTask) => {
    if (!dagName) return
    setShowDeleteConfirmation(false)

    const { name: taskNameDelete } = row

    deleteAirflowTask(
      { type, dagName, taskName: taskNameDelete },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows'], // Matches all related queries that starts the queryKey with 'airflows'
            exact: false
          })
          setRowSelection({})
          console.log('Delete successful')
        },
        onError: (error: Error) => {
          console.log('Error deleting item: ', error.message)
        }
      }
    )
  }

  return (
    <div style={{ marginTop: 0 }}>
      <div style={{ display: 'flex', justifyContent: 'end', marginBottom: 15 }}>
        <Button
          title="+ Create"
          onClick={() => {
            setRowSelection({})
            setCreateModalOpen(true)
          }}
          fontSize="14px"
        />
      </div>
      {tasksData.length > 0 ? (
        <TableList
          columns={columns}
          data={tasksData}
          onEdit={handleEditClick}
          onDelete={handleDeleteIconClick}
          onCopy={handleCopyIconClick}
          isLoading={isLoading}
          rowSelection={rowSelection}
          onRowSelectionChange={setRowSelection}
          enableMultiSelection={false}
        />
      ) : (
        <p
          style={{
            padding: ' 40px 50px 44px 50px',
            backgroundColor: 'white',
            borderRadius: 7,
            textAlign: 'center',
            fontSize: 14
          }}
        >
          No Tasks yet in this DAG.
        </p>
      )}

      {isEditModalOpen && selectedRow && (
        <EditTableModal
          isEditModalOpen={isEditModalOpen}
          title={`Edit Task ${selectedRow[0].value}`}
          settings={selectedRow}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleSave}
        />
      )}
      {isCreateModalOpen && (
        <CreateAirflowTaskModal
          isCreateModalOpen={isCreateModalOpen}
          type={type}
          tasksData={tasksData}
          onSave={handleSaveCreateTask}
          onClose={() => setCreateModalOpen(false)}
        />
      )}
      {isCopyTaskModalOpen && taskName && (
        <CopyAirflowTaskModal
          isCopyModalOpen={isCopyTaskModalOpen}
          type={type}
          taskName={taskName}
          tasksData={tasksData}
          onSave={handleSaveCopyTask}
          onClose={() => setIsCopyTaskModalOpen(false)}
        />
      )}
      {showDeleteConfirmation && selectedDeleteRow && (
        <DeleteModal
          title={`Delete ${selectedDeleteRow.name}`}
          message={`Are you sure that you want to delete task "${selectedDeleteRow.name}"?`}
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Delete"
          onConfirm={() => handleDelete(selectedDeleteRow)}
          onCancel={() => setShowDeleteConfirmation(false)}
          isActive={showDeleteConfirmation}
        />
      )}
    </div>
  )
}

export default AirflowTasks
