import { Column, Columns, EditSetting } from '../../../utils/interfaces'
import TableList from '../../../components/TableList'
import { useCallback, useMemo, useState } from 'react'
import { useImportTable } from '../../../utils/queries'
import EditTableModal from '../../../components/modals/EditTableModal'
import { updateTableData } from '../../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useUpdateTable } from '../../../utils/mutations'
import { useNavigate, useParams } from 'react-router-dom'
import '../../../components/Loading.scss'
import { importColumnRowDataEdit } from '../../../utils/cardRenderFormatting'

function TableColumns() {
  const navigate = useNavigate()

  const { database, table: tableParam } = useParams<{
    database: string
    table: string
  }>()

  const {
    data: tableData,
    isLoading,
    isError,
    error
  } = useImportTable(database, tableParam)

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

  const columns: Column<Columns>[] = useMemo(
    () => [
      { header: 'Column Name', accessor: 'columnName' },
      { header: 'Column Order', accessor: 'columnOrder' },
      { header: 'Source Column Name', accessor: 'sourceColumnName' },
      { header: 'Column Type', accessor: 'columnType' },
      { header: 'Source Column Type', accessor: 'sourceColumnType' },
      { header: 'Column Name Override', accessor: 'columnNameOverride' },
      { header: 'Column Type Override', accessor: 'columnTypeOverride' },
      { header: 'Sqoop Column Type', accessor: 'sqoopColumnType' },
      {
        header: 'Sqoop Column Type Override',
        accessor: 'sqoopColumnTypeOverride'
      },
      { header: 'Force String', accessor: 'forceString' },
      { header: 'Include in Import', accessor: 'includeInImport' },
      { header: 'Source Primary Key', accessor: 'sourcePrimaryKey' },
      { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
      { header: 'Comment', accessor: 'comment' },
      { header: 'Operator notes', accessor: 'operatorNotes' },
      { header: 'Anonymization Function', accessor: 'anonymizationFunction' }
    ],
    []
  )

  const handleEditClick = useCallback(
    (row: Columns, rowIndex: number | undefined) => {
      if (!tableData) {
        return
      }

      const rowData: EditSetting[] = importColumnRowDataEdit(row, tableData)

      setSelectedRow(rowData)
      setRowIndex(rowIndex)
      setIsEditModalOpen(true)
    },
    [tableData]
  )

  if (isError) {
    if (error.status === 404) {
      console.log(
        `GET table: ${error.message} ${error.response?.statusText}, re-routing to /import`
      )
      navigate(`/import`, { replace: true })
    }
    return <div className="error">Server error occurred.</div>
  }
  if (!tableData && !isError) return <div className="loading">Loading...</div>

  const handleSave = (updatedSettings: EditSetting[]) => {
    const editedTableData = updateTableData(
      tableData,
      updatedSettings,
      true,
      rowIndex
    )

    updateTable(
      { type: 'import', table: editedTableData },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['import', database, tableParam]
          })
          setDataRefreshTrigger((prev) => prev + 1)
          console.log('Update successful')
          setIsEditModalOpen(false)
        },
        onError: (error) => {
          queryClient.invalidateQueries({
            queryKey: ['import', database, tableParam]
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
          initWidth={587}
        />
      )}
    </div>
  )
}

export default TableColumns
