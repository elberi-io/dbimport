import { useState, useMemo, useCallback, useEffect } from 'react'
import {
  BulkUpdateImportTables,
  Column,
  DbTable,
  EditSetting,
  ExportPKs,
  // EditSetting,
  HeadersRowInfo,
  ImportPKs,
  ImportSearchFilter,
  UiDbTable,
  UITableWithoutEnum
  // UITable
} from '../../utils/interfaces'
// import { fetchTableData } from '../../utils/queries'
import './DbTables.scss'
// import EditTableModal from '../../components/modals/EditTableModal'
import {
  newCopyImportTableData,
  transformBulkChanges
  // updateTableData
} from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteTables,
  useBulkUpdateTable,
  useCopyTable,
  useDeleteTable
  // useDeleteImportTable,
  // useUpdateTable
} from '../../utils/mutations'
import {
  bulkImportFieldsData
  // importDbTablesEditSettings
} from '../../utils/cardRenderFormatting'
import TableList from '../../components/TableList'
import ListRowsInfo from '../../components/ListRowsInfo'
import Button from '../../components/Button'
import { useAtom } from 'jotai'
import { clearRowSelectionAtom } from '../../atoms/atoms'
import BulkEditModal from '../../components/modals/BulkEditModal'
import RepairTableModal from '../../components/modals/RepairTableModal'
import ResetTableModal from '../../components/modals/ResetTableModal'
import { useRawTable } from '../../utils/queries'
import CopyTableModal from '../../components/modals/CopyTableModal'
import RenameTableModal from '../../components/modals/RenameTableModal'
import DeleteModal from '../../components/modals/DeleteModal'

function DbTables({
  data,
  queryKeyFilters,
  isLoading,
  headersRowInfo
}: {
  data: UiDbTable[]
  queryKeyFilters: ImportSearchFilter
  isLoading: boolean
  headersRowInfo: HeadersRowInfo
}) {
  const queryClient = useQueryClient()

  const { mutate: copyTable } = useCopyTable()
  const { mutate: deleteTable } = useDeleteTable()

  const { mutate: bulkUpdateTable } = useBulkUpdateTable()
  const { mutate: bulkDeleteTable } = useBulkDeleteTables()

  const [isRepairTableModalOpen, setIsRepairTableModalOpen] = useState(false)
  const [isResetTableModalOpen, setIsResetTableModalOpen] = useState(false)
  const [isRenameTableModalOpen, setIsRenameTableModalOpen] = useState(false)
  const [isCopyTableModalOpen, setIsCopyTableModalOpen] = useState(false)

  const [selectedRow, setSelectedRow] = useState<UiDbTable>()

  const getPrimaryKeys = (
    row: UiDbTable | undefined
  ): { database: string; table: string } | null => {
    if (!row) return null
    const { database, table } = row
    return { database, table }
  }
  const primaryKeys = useMemo(() => getPrimaryKeys(selectedRow), [selectedRow])

  const { data: tableData } = useRawTable({
    type: 'import',
    databaseOrConnection: primaryKeys?.database,
    table: primaryKeys?.table
  })

  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)
  const [selectedRowsBulk, setSelectedRowsBulk] = useState<UiDbTable[] | []>([])

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [rowSelection, setRowSelection] = useState({})

  const [clearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  useEffect(() => {
    setRowSelection({})
  }, [clearRowSelectionTrigger])

  const columns: Column<DbTable>[] = useMemo(
    () => [
      { header: 'Table', accessor: 'table' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Database', accessor: 'database' },
      { header: 'Source Schema', accessor: 'sourceSchema' },
      { header: 'Source Table', accessor: 'sourceTable' },
      { header: 'Import Type', accessor: 'importPhaseType' },
      { header: 'Import Tool', accessor: 'importTool' },
      { header: 'ETL Type', accessor: 'etlPhaseType' },
      { header: 'ETL Engine', accessor: 'etlEngine' },
      { header: 'Last update from source', accessor: 'lastUpdateFromSource' },
      {
        header: 'Include in Airflow',
        accessor: 'includeInAirflow'
      },
      { header: 'Actions', isAction: 'repairAndResetAndRenameAndCopy' }
    ],
    []
  )

  const handleRepairIconClick = (row: UiDbTable) => {
    setSelectedRow(row)
    setIsRepairTableModalOpen(true)
  }

  const handleResetIconClick = (row: UiDbTable) => {
    setSelectedRow(row)
    setIsResetTableModalOpen(true)
  }

  const handleRenameIconClick = (row: UiDbTable) => {
    setSelectedRow(row)
    setIsRenameTableModalOpen(true)
  }

  const handleSaveRename = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return

    const newCopyTableData = newCopyImportTableData(
      tableData as UITableWithoutEnum,
      tablePrimaryKeysSettings
    )

    copyTable(
      { type, table: newCopyTableData },
      {
        onSuccess: () => {
          console.log('Save renamned copy successful')

          deleteTable(
            { type, primaryKeys: primaryKeys as ImportPKs | ExportPKs },
            {
              onSuccess: () => {
                queryClient.invalidateQueries({
                  queryKey: [type, 'search'],
                  exact: false
                })
                if (type === 'import') {
                  queryClient.invalidateQueries({
                    queryKey: ['databases'],
                    exact: false
                  })
                }

                console.log('Delete original successful, rename successful')

                setIsRenameTableModalOpen(false)
              },
              onError: (error) => {
                console.log('Error deleting table', error.message)
              }
            }
          )
        },
        onError: (error) => {
          console.log('Error copy table', error.message)
        }
      }
    )
  }

  const handleCopyIconClick = (row: UiDbTable) => {
    setSelectedRow(row)
    setIsCopyTableModalOpen(true)
  }

  const handleSaveCopy = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return

    const newCopyTableData = newCopyImportTableData(
      tableData as UITableWithoutEnum,
      tablePrimaryKeysSettings
    )

    copyTable(
      { type, table: newCopyTableData },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['import', 'search'],
            exact: false
          })
          queryClient.invalidateQueries({
            queryKey: ['databases'],
            exact: false
          })
          console.log('Save table copy successful')
          setIsCopyTableModalOpen(false)
        },
        onError: (error) => {
          console.log('Error copy table', error.message)
        }
      }
    )
  }

  const handleBulkEditClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    setSelectedRowsBulk(selectedRows)
    setIsBulkEditModalOpen(true)
  }, [rowSelection, data])

  const handleBulkEditSave = (
    bulkChanges: Record<string, string | number | boolean | null>
  ) => {
    const importTablesPks = selectedRowsBulk.map((row) => ({
      database: row.database,
      table: row.table
    }))

    // For bulkChanges with enum values
    const transformedChanges = transformBulkChanges('import', bulkChanges)

    const bulkUpdateJson: BulkUpdateImportTables = {
      ...transformedChanges,
      importTables: importTablesPks
    }

    bulkUpdateTable(
      { type: 'import', bulkUpdateJson },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['import', 'search', queryKeyFilters]
          })
          console.log('Update successful')
          setIsBulkEditModalOpen(false)
        },
        onError: (error) => {
          console.log('Error updating table', error.message)
        }
      }
    )
  }

  const handleBulkDeleteClick = useCallback(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    const selectedRows = selectedIndexes.map((index) => data[index])
    setSelectedRowsBulk(selectedRows)
    setShowBulkDeleteConfirmation(true)
  }, [rowSelection, data])

  const handleBulkDelete = async (rows: UiDbTable[]) => {
    setShowBulkDeleteConfirmation(false)

    const bulkDeleteRowsPks = rows.map(({ database, table }) => ({
      database,
      table
    }))

    bulkDeleteTable(
      { type: 'import', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['import'], // Matches all related queries that starts the queryKey with 'import'
            exact: false
          })
          console.log('Delete successful')
          setRowSelection({})
        },
        onError: (error) => {
          console.log('Error deleting item', error.message)
        }
      }
    )
  }

  const selectedRowsBulkData = useMemo(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    return selectedIndexes.map((index) => data[index])
  }, [rowSelection, data])

  const selectedRowsLength = useMemo(
    () => selectedRowsBulkData.length,
    [selectedRowsBulkData]
  )

  return (
    <div className="db-table-root">
      {data ? (
        <>
          <div className="list-top-info-and-edit">
            <Button
              title={`Edit ${
                selectedRowsLength > 0 ? selectedRowsLength : ''
              } table${
                selectedRowsLength > 1 || selectedRowsLength === 0 ? 's' : ''
              }`}
              onClick={handleBulkEditClick}
              disabled={selectedRowsLength < 1}
            />
            <Button
              title={`Delete ${
                selectedRowsLength > 0 ? selectedRowsLength : ''
              } table${
                selectedRowsLength > 1 || selectedRowsLength === 0 ? 's' : ''
              }`}
              onClick={handleBulkDeleteClick}
              deleteStyle={true}
              disabled={selectedRowsLength < 1}
            />
          </div>

          <ListRowsInfo
            filteredData={data}
            contentTotalRows={headersRowInfo.contentTotalRows}
            contentMaxReturnedRows={headersRowInfo.contentMaxReturnedRows}
            itemType="table"
          />
          <TableList
            columns={columns}
            data={data}
            onRepair={handleRepairIconClick}
            onReset={handleResetIconClick}
            onRename={handleRenameIconClick}
            onCopy={handleCopyIconClick}
            isLoading={isLoading}
            rowSelection={rowSelection}
            onRowSelectionChange={setRowSelection}
            enableMultiSelection={true}
          />
        </>
      ) : (
        <div>Loading....</div>
      )}
      {isBulkEditModalOpen && (
        <BulkEditModal
          isBulkEditModalOpen={isBulkEditModalOpen}
          title={`Edit the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }`}
          selectedRows={selectedRowsBulk}
          bulkFieldsData={bulkImportFieldsData}
          onSave={handleBulkEditSave}
          onClose={() => setIsBulkEditModalOpen(false)}
          initWidth={584}
        />
      )}
      {showBulkDeleteConfirmation && selectedRowsBulk && (
        <DeleteModal
          title={`Delete the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }`}
          message={`Are you sure that you want to delete the ${selectedRowsLength} selected table${
            selectedRowsLength > 1 ? 's' : ''
          }?`}
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Delete"
          onConfirm={() => handleBulkDelete(selectedRowsBulk)}
          onCancel={() => setShowBulkDeleteConfirmation(false)}
          isActive={showBulkDeleteConfirmation}
        />
      )}
      {isRepairTableModalOpen && primaryKeys && (
        <RepairTableModal
          type="import"
          primaryKeys={primaryKeys}
          isRepairTableModalOpen={isRepairTableModalOpen}
          onClose={() => setIsRepairTableModalOpen(false)}
        />
      )}
      {isResetTableModalOpen && primaryKeys && (
        <ResetTableModal
          type="import"
          primaryKeys={primaryKeys}
          isResetTableModalOpen={isResetTableModalOpen}
          onClose={() => setIsResetTableModalOpen(false)}
        />
      )}
      {isRenameTableModalOpen && primaryKeys && tableData && (
        <RenameTableModal
          type="import"
          primaryKeys={primaryKeys}
          isRenameTableModalOpen={isRenameTableModalOpen}
          onSave={handleSaveRename}
          onClose={() => setIsRenameTableModalOpen(false)}
        />
      )}
      {isCopyTableModalOpen && primaryKeys && selectedRow && tableData && (
        <CopyTableModal
          type="import"
          primaryKeys={primaryKeys}
          isCopyTableModalOpen={isCopyTableModalOpen}
          onSave={handleSaveCopy}
          onClose={() => setIsCopyTableModalOpen(false)}
        />
      )}
    </div>
  )
}

export default DbTables
