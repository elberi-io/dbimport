import { useState, useMemo, useCallback, useEffect } from 'react'
import TableList from '../../components/TableList'
import {
  BulkUpdateExportTables,
  Column,
  EditSetting,
  // EditSetting,
  ExportCnTablesWithoutEnum,
  ExportPKs,
  ExportSearchFilter,
  HeadersRowInfo,
  ImportPKs,
  UIExportCnTables,
  UIExportTableWithoutEnum
  // UIExportTable
} from '../../utils/interfaces'
// import { fetchExportTableData } from '../../utils/queries'
import '../import/DbTables.scss'
// import EditTableModal from '../../components/modals/EditTableModal'
import {
  newCopyExportTableData,
  transformBulkChanges
  // updateExportTableData
} from '../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import {
  useBulkDeleteTables,
  useBulkUpdateTable,
  useCopyTable,
  useDeleteTable
  // useDeleteExportTable,
  // useUpdateTable
} from '../../utils/mutations'

import {
  bulkExportFieldsData
  // exportCnTablesEditSettings
} from '../../utils/cardRenderFormatting'
import Button from '../../components/Button'
import ListRowsInfo from '../../components/ListRowsInfo'
import { useAtom } from 'jotai'
import { clearRowSelectionAtom } from '../../atoms/atoms'
import BulkEditModal from '../../components/modals/BulkEditModal'
import RepairTableModal from '../../components/modals/RepairTableModal'
import ResetTableModal from '../../components/modals/ResetTableModal'
import { useRawTable } from '../../utils/queries'
import CopyTableModal from '../../components/modals/CopyTableModal'
import RenameTableModal from '../../components/modals/RenameTableModal'
import DeleteModal from '../../components/modals/DeleteModal'

function ExportCnTables({
  data,
  queryKeyFilters,
  isLoading,
  headersRowInfo
}: {
  data: UIExportCnTables[]
  queryKeyFilters: ExportSearchFilter
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

  const [selectedRow, setSelectedRow] = useState<UIExportCnTables>()

  const getPrimaryKeys = (
    row: UIExportCnTables | undefined
  ): {
    connection: string
    targetSchema: string
    targetTable: string
  } | null => {
    if (!row) return null
    const { connection, targetSchema, targetTable } = row
    return { connection, targetSchema, targetTable }
  }
  const primaryKeys = useMemo(() => getPrimaryKeys(selectedRow), [selectedRow])

  const exportDatabase = selectedRow?.database

  const { data: tableData } = useRawTable({
    type: 'export',
    databaseOrConnection: primaryKeys?.connection,
    schema: primaryKeys?.targetSchema,
    table: primaryKeys?.targetTable
  })

  const [showBulkDeleteConfirmation, setShowBulkDeleteConfirmation] =
    useState(false)
  const [selectedRowsBulk, setSelectedRowsBulk] = useState<
    UIExportCnTables[] | []
  >([])

  const [isBulkEditModalOpen, setIsBulkEditModalOpen] = useState(false)
  const [rowSelection, setRowSelection] = useState({})

  const [clearRowSelectionTrigger] = useAtom(clearRowSelectionAtom)

  useEffect(() => {
    setRowSelection({})
  }, [clearRowSelectionTrigger])

  const columns: Column<ExportCnTablesWithoutEnum>[] = useMemo(
    () => [
      { header: 'Target Table', accessor: 'targetTable' },
      { header: 'Target Schema', accessor: 'targetSchema' },
      { header: 'Connection', accessor: 'connection' },
      { header: 'Database', accessor: 'database' },
      { header: 'Table', accessor: 'table' },
      { header: 'Export Type', accessor: 'exportType' },
      { header: 'Export Tool', accessor: 'exportTool' },
      {
        header: 'Last update from Hive',
        accessor: 'lastUpdateFromHive'
      },
      {
        header: 'Include in Airflow',
        accessor: 'includeInAirflow'
      },
      { header: 'Actions', isAction: 'repairAndResetAndRenameAndCopy' }
    ],
    []
  )

  const handleRepairIconClick = (row: UIExportCnTables) => {
    setSelectedRow(row)
    setIsRepairTableModalOpen(true)
  }

  const handleResetIconClick = (row: UIExportCnTables) => {
    setSelectedRow(row)
    setIsResetTableModalOpen(true)
  }

  const handleRenameIconClick = (row: UIExportCnTables) => {
    setSelectedRow(row)
    setIsRenameTableModalOpen(true)
  }

  const handleSaveRename = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return

    const newCopyTableData = newCopyExportTableData(
      tableData as UIExportTableWithoutEnum,
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

  const handleCopyIconClick = (row: UIExportCnTables) => {
    setSelectedRow(row)
    setIsCopyTableModalOpen(true)
  }

  const handleSaveCopy = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return

    const newCopyTableData = newCopyExportTableData(
      tableData as UIExportTableWithoutEnum,
      tablePrimaryKeysSettings
    )

    copyTable(
      { type, table: newCopyTableData },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['export', 'search'],
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
    const exportTablesPks = selectedRowsBulk.map((row) => ({
      connection: row.connection,
      targetSchema: row.targetSchema,
      targetTable: row.targetTable
    }))

    // For bulkChanges with enum values
    const transformedChanges = transformBulkChanges('export', bulkChanges)

    const bulkUpdateJson: BulkUpdateExportTables = {
      ...transformedChanges,
      exportTables: exportTablesPks
    }

    bulkUpdateTable(
      { type: 'export', bulkUpdateJson },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['export', 'search', queryKeyFilters]
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

  const handleBulkDelete = async (rows: UIExportCnTables[]) => {
    setShowBulkDeleteConfirmation(false)

    const bulkDeleteRowsPks = rows.map(
      ({ connection, targetSchema, targetTable }) => ({
        connection,
        targetSchema,
        targetTable
      })
    )

    bulkDeleteTable(
      { type: 'export', bulkDeleteRowsPks },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['export'], // Matches all related queries that starts the queryKey with 'export'
            exact: false
          })
          console.log('Delete successful')
          setRowSelection({})
        },
        onError: (error) => {
          console.log('Error deleting', error.message)
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
          <div
            className="list-top-info-and-edit"
            // style={{ visibility: 'hidden' }}
          >
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
            noLinkOnTableName={true}
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
          bulkFieldsData={bulkExportFieldsData}
          // onBulkChange={handleBulkChange}
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
          type="export"
          primaryKeys={primaryKeys}
          isRepairTableModalOpen={isRepairTableModalOpen}
          onClose={() => setIsRepairTableModalOpen(false)}
          exportDatabase={exportDatabase ? exportDatabase : ''}
        />
      )}
      {isResetTableModalOpen && primaryKeys && (
        <ResetTableModal
          type="export"
          primaryKeys={primaryKeys}
          isResetTableModalOpen={isResetTableModalOpen}
          onClose={() => setIsResetTableModalOpen(false)}
          exportDatabase={exportDatabase ? exportDatabase : ''}
        />
      )}
      {isRenameTableModalOpen && primaryKeys && tableData && (
        <RenameTableModal
          type="export"
          primaryKeys={primaryKeys}
          isRenameTableModalOpen={isRenameTableModalOpen}
          onSave={handleSaveRename}
          onClose={() => setIsRenameTableModalOpen(false)}
        />
      )}
      {isCopyTableModalOpen && primaryKeys && selectedRow && tableData && (
        <CopyTableModal
          type="export"
          primaryKeys={primaryKeys}
          isCopyTableModalOpen={isCopyTableModalOpen}
          onSave={handleSaveCopy}
          onClose={() => setIsCopyTableModalOpen(false)}
        />
      )}
    </div>
  )
}

export default ExportCnTables
