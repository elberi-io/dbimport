import { useCallback, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useNavigate, useParams } from 'react-router-dom'
import '../../../components/Loading.scss'
import { Column, EditSetting, ExportColumns } from '../../../utils/interfaces'
import { useUpdateTable } from '../../../utils/mutations'
import { useExportTable } from '../../../utils/queries'
import TableList from '../../../components/TableList'
import EditTableModal from '../../../components/modals/EditTableModal'
import { updateExportTableData } from '../../../utils/dataFunctions'
import { exportColumnRowDataEdit } from '../../../utils/cardRenderFormatting'

function ExportTableColumns() {
  const navigate = useNavigate()

  const { connection, targetSchema, targetTable } = useParams<{
    connection: string
    targetSchema: string
    targetTable: string
  }>()

  const {
    data: tableData,
    isLoading,
    isError,
    error
  } = useExportTable(connection, targetSchema, targetTable)

  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const [selectedRow, setSelectedRow] = useState<EditSetting[] | []>([])
  const [rowIndex, setRowIndex] = useState<number>()
  const [dataRefreshTrigger, setDataRefreshTrigger] = useState(0)
  const [rowSelection, setRowSelection] = useState({})

  const columnsData = useMemo(
    () => [...(tableData?.columns || [])],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [tableData, dataRefreshTrigger]
  )

  const columns: Column<ExportColumns>[] = useMemo(
    () => [
      { header: 'Column Name', accessor: 'columnName' },
      { header: 'Column Order', accessor: 'columnOrder' },
      { header: 'Column Type', accessor: 'columnType' },
      { header: 'Target Column Name', accessor: 'targetColumnName' },
      { header: 'Target Column Type', accessor: 'targetColumnType' },
      { header: 'Last update from Hive', accessor: 'lastUpdateFromHive' },
      { header: 'Include in Export', accessor: 'includeInExport' },
      { header: 'Comment', accessor: 'comment' },
      { header: 'Operator notes', accessor: 'operatorNotes' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: ExportColumns, rowIndex: number | undefined) => {
      if (!tableData) {
        return
      }

      const rowData: EditSetting[] = exportColumnRowDataEdit(row)

      setRowIndex(rowIndex)
      setSelectedRow(rowData)
      setIsEditModalOpen(true)
    },
    [tableData]
  )

  if (isError) {
    if (error.status === 404) {
      console.log(
        `GET table: ${error.message} ${error.response?.statusText}, re-routing to /export`
      )
      navigate(`/export`, { replace: true })
    }
    return <div className="error">Server error occurred.</div>
  }

  if (!tableData && !isError) return <div className="loading">Loading...</div>

  const handleSave = (updatedSettings: EditSetting[]) => {
    const editedTableData = updateExportTableData(
      tableData,
      updatedSettings,
      true,
      rowIndex
    )
    updateTable(
      { type: 'export', table: editedTableData },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['export', connection, targetTable]
          })
          setDataRefreshTrigger((prev) => prev + 1)
          console.log('Update successful')
          setIsEditModalOpen(false)
        },
        onError: (error) => {
          queryClient.invalidateQueries({
            queryKey: ['export', connection, targetTable]
          })
          console.log('Error updating table', error.message)
        }
      }
    )
  }

  return (
    <div style={{ marginTop: 40 }}>
      {columnsData.length > 0 ? (
        <TableList
          columns={columns}
          data={columnsData}
          onEdit={handleEditClick}
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
          No columns yet in this table.
        </p>
      )}

      {isEditModalOpen && selectedRow && (
        <EditTableModal
          isEditModalOpen={isEditModalOpen}
          title={`Edit column ${selectedRow[0].value}`}
          settings={selectedRow}
          onClose={() => setIsEditModalOpen(false)}
          onSave={handleSave}
        />
      )}
    </div>
  )
}

export default ExportTableColumns
