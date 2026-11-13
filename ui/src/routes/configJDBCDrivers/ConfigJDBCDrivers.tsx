import { useCallback, useMemo, useState } from 'react'
import { useJDBCDrivers } from '../../utils/queries'
import { Column, EditSetting, JDBCdrivers } from '../../utils/interfaces'
import TableList from '../../components/TableList'
import EditTableModal from '../../components/modals/EditTableModal'
import { JDBCdriversRowDataEdit } from '../../utils/cardRenderFormatting'
import { updateJDBCdriverData } from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateJDBCdrivers } from '../../utils/mutations'
import '../import/Import.scss'

function ConfigJDBCDrivers() {
  const queryClient = useQueryClient()
  const { mutate: updateDriver } = useUpdateJDBCdrivers()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [selectedRow, setSelectedRow] = useState<EditSetting[] | []>([])
  const [row, setRow] = useState<JDBCdrivers>()
  const [rowIndex, setRowIndex] = useState<number>()
  const [rowSelection, setRowSelection] = useState({})

  const { data: originalDriverData, isLoading, isError } = useJDBCDrivers()

  const columns: Column<JDBCdrivers>[] = useMemo(
    () => [
      { header: 'Database Type', accessor: 'databaseType' },
      { header: 'Version', accessor: 'version' },
      { header: 'Driver', accessor: 'driver' },
      { header: 'Class Path', accessor: 'classpath' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: JDBCdrivers, rowIndex: number | undefined) => {
      if (!originalDriverData) {
        return
      }

      const rowData: EditSetting[] = JDBCdriversRowDataEdit(row)

      setRowIndex(rowIndex)
      setRow(row)
      setSelectedRow(rowData)
      setIsEditModalOpen(true)
    },
    [originalDriverData]
  )

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (isLoading) {
    return <div className="loading">Loading...</div>
  }
  if (!originalDriverData) {
    return (
      <div className="text-block">
        <p>Error. No data from REST server.</p>
      </div>
    )
  }

  const handleSave = (updatedSettings: EditSetting[]) => {
    const originalDataCopy: JDBCdrivers[] = [...originalDriverData]

    const editedJDBCDriverData = updateJDBCdriverData(
      row!, // row is set at edit click before user can save
      updatedSettings
    )

    if (
      typeof rowIndex !== 'undefined' &&
      rowIndex >= 0 &&
      rowIndex < originalDataCopy.length
    ) {
      originalDataCopy[rowIndex] = editedJDBCDriverData
    } else {
      console.error('Invalid row index:', rowIndex)
      return
    }

    queryClient.setQueryData(['configuration', 'jdbcdrivers'], originalDataCopy)
    updateDriver(editedJDBCDriverData, {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: ['configuration', 'jdbcdrivers']
        }) // For getting fresh data from database to the cache
        console.log('Update successful')
        setIsEditModalOpen(false)
      },
      onError: (error) => {
        queryClient.setQueryData(
          ['configuration', 'jdbcdrivers'],
          originalDriverData
        )

        console.log('Error updating JDBC Drivers', error.message)
      }
    })
  }

  return (
    <>
      {originalDriverData ? (
        <TableList
          columns={columns}
          data={originalDriverData}
          onEdit={handleEditClick}
          isLoading={isLoading}
          rowSelection={rowSelection}
          onRowSelectionChange={setRowSelection}
          enableMultiSelection={false}
        />
      ) : isLoading ? (
        <div className="loading">Loading...</div>
      ) : (
        <div className="text-block">
          <p>No connections yet.</p>
        </div>
      )}

      {isEditModalOpen && selectedRow && row && (
        <EditTableModal
          isEditModalOpen={isEditModalOpen}
          title={`Edit ${row.databaseType} ${row.version}`}
          settings={selectedRow}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </>
  )
}

export default ConfigJDBCDrivers
