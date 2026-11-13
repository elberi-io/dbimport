import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import { useEffect, useMemo, useState } from 'react'
import '../tableDetailed/DetailedView.scss'
import DropdownActions from '../../../components/DropdownActions'
import RepairTableModal from '../../../components/modals/RepairTableModal'
import RepairIcon from '../../../assets/icons/RepairIcon'
import ResetIcon from '../../../assets/icons/ResetIcon'
import ResetTableModal from '../../../components/modals/ResetTableModal'
import CopyTableModal from '../../../components/modals/CopyTableModal'
import {
  newCopyExportTableData,
  newCopyImportTableData
} from '../../../utils/dataFunctions'
import {
  EditSetting,
  ExportPKs,
  ImportPKs,
  UIExportTableWithoutEnum,
  UITableWithoutEnum
} from '../../../utils/interfaces'
import { useCopyTable, useDeleteTable } from '../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { useRawTable } from '../../../utils/queries'
import CopyIcon from '../../../assets/icons/CopyIcon'
import DeleteIcon from '../../../assets/icons/DeleteIcon'
import RenameIcon from '../../../assets/icons/RenameIcon'
import RenameTableModal from '../../../components/modals/RenameTableModal'
import DeleteModal from '../../../components/modals/DeleteModal'

interface TableDetailedViewProps {
  type: 'import' | 'export'
}

function TableDetailedView({ type }: TableDetailedViewProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const { database, table, connection, targetSchema, targetTable } = useParams()

  const queryClient = useQueryClient()
  const { mutate: copyTable } = useCopyTable()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isRepairTableModalOpen, setIsRepairTableModalOpen] = useState(false)
  const [isResetTableModalOpen, setIsResetTableModalOpen] = useState(false)

  const [isRenameTableModalOpen, setIsRenameTableModalOpen] = useState(false)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)

  const [isCopyTableModalOpen, setIsCopyTableModalOpen] = useState(false)

  const { mutate: deleteTable } = useDeleteTable()
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)

  const primaryKeys = useMemo(() => {
    if (type === 'import') {
      return database && table ? { database, table } : null
    }
    if (type === 'export') {
      return connection && targetSchema && targetTable
        ? { connection, targetSchema, targetTable }
        : null
    }
    return null
  }, [type, database, table, connection, targetSchema, targetTable])

  const handleDeleteIconClick = () => {
    setShowDeleteConfirmation(true)
  }

  const handleDeleteTable = async () => {
    setShowDeleteConfirmation(false)

    deleteTable(
      { type, primaryKeys: primaryKeys as ImportPKs | ExportPKs },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: [type, 'search'],
            exact: false
          })
          console.log('Delete successful')
          navigate(`/${type}`, { replace: true })
        },
        onError: (error) => {
          console.log('Error deleting table', error.message)
        }
      }
    )
  }

  const { data: tableData } = useRawTable({
    type,
    databaseOrConnection: type === 'import' ? database : connection,
    schema: type === 'export' ? targetSchema : undefined,
    table: type === 'import' ? table : targetTable
  })

  const exportDatabase = type === 'export' ? tableData?.database : null

  const encodedDatabase = encodeURIComponent(database ? database : '')
  const encodedTable = encodeURIComponent(table ? table : '')
  const encodedConnection = encodeURIComponent(connection ? connection : '')
  const encodedTargetSchema = encodeURIComponent(
    targetSchema ? targetSchema : ''
  )
  const encodedTargetTable = encodeURIComponent(targetTable ? targetTable : '')

  const pathSegments =
    typeof location.pathname === 'string'
      ? location.pathname.split('/').filter(Boolean)
      : []

  const tab =
    type === 'export'
      ? pathSegments[4] || 'settings'
      : pathSegments[3] || 'settings'

  const validTabs = useMemo(() => ['settings', 'columns', 'statistics'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      if (type === 'import') {
        navigate(`/import/${encodedDatabase}/${encodedTable}/settings`, {
          replace: true
        })
      } else if (type === 'export') {
        navigate(
          `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/settings`,
          {
            replace: true
          }
        )
      }
    }
  }, [
    tab,
    database,
    table,
    navigate,
    validTabs,
    type,
    connection,
    encodedDatabase,
    encodedTable,
    encodedConnection,
    encodedTargetSchema,
    encodedTargetTable
  ])

  const handleTabClick = (tabName: string) => {
    if (type === 'import') {
      navigate(`/import/${encodedDatabase}/${encodedTable}/${tabName}`)
    } else if (type === 'export') {
      navigate(
        `/export/${encodedConnection}/${encodedTargetSchema}/${encodedTargetTable}/${tabName}`
      )
    }
  }

  const handleRepairTableClick = () => {
    setIsRepairTableModalOpen(true)
  }

  const handleResetTableClick = () => {
    setIsResetTableModalOpen(true)
  }

  const handleRenameTableClick = () => {
    setIsRenameTableModalOpen(true)
  }

  const handleSaveRename = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return

    const newCopyTableData =
      type === 'import'
        ? newCopyImportTableData(
            tableData as UITableWithoutEnum,
            tablePrimaryKeysSettings
          )
        : newCopyExportTableData(
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

                queryClient.removeQueries({
                  queryKey: [
                    type,
                    type === 'import'
                      ? primaryKeys?.database
                      : primaryKeys?.connection,
                    type === 'import'
                      ? primaryKeys?.table
                      : primaryKeys?.targetTable
                  ],
                  exact: true
                })

                console.log('Delete original successful, rename successful')

                const encodedNewTableName = encodeURIComponent(
                  newCopyTableData.table ? newCopyTableData.table : ''
                )
                const encodedNewTargetTableName = encodeURIComponent(
                  newCopyTableData.targetTable
                    ? (newCopyTableData.targetTable as string)
                    : ''
                )

                setIsRenameTableModalOpen(false)

                if (type === 'import') {
                  navigate(
                    `/import/${encodedDatabase}/${encodedNewTableName}/${selectedTab}`,
                    {
                      replace: true
                    }
                  )
                } else if (type === 'export') {
                  navigate(
                    `/export/${encodedConnection}/${encodedTargetSchema}/${encodedNewTargetTableName}/${selectedTab}`,
                    {
                      replace: true
                    }
                  )
                }
              },
              onError: (error) => {
                console.log('Error deleting table', error.message)
                setErrorMessage(error.message)
              }
            }
          )
        },
        onError: (error) => {
          console.log('Error creating rename', error.message)
          setErrorMessage(error.message)
        }
      }
    )
  }

  const handleCopyIconClick = () => {
    setIsCopyTableModalOpen(true)
  }

  const handleSaveCopy = (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => {
    if (!tableData) return

    const newCopyTableData =
      type === 'import'
        ? newCopyImportTableData(
            tableData as UITableWithoutEnum,
            tablePrimaryKeysSettings
          )
        : newCopyExportTableData(
            tableData as UIExportTableWithoutEnum,
            tablePrimaryKeysSettings
          )

    copyTable(
      { type, table: newCopyTableData },
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

          console.log('Save table copy successful')
        },
        onError: (error) => {
          console.log('Error copy table', error.message)
        }
      }
    )
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const dropdownActionsItems = useMemo(() => {
    if (!tableData) return []

    const items = [
      {
        icon: <RenameIcon />,
        label: 'Rename',
        onClick: handleRenameTableClick
      },
      {
        icon: <CopyIcon />,
        label: 'Copy',
        onClick: handleCopyIconClick
      },
      {
        icon: <DeleteIcon />,
        label: 'Delete',
        onClick: handleDeleteIconClick
      }
    ]

    if (
      (type === 'import' && tableData.importPhaseType === 'incr') ||
      (type === 'export' && tableData.exportType === 'incr')
    ) {
      items.unshift(
        {
          icon: <RepairIcon />,
          label: 'Repair',
          onClick: handleRepairTableClick
        },
        {
          icon: <ResetIcon />,
          label: 'Reset',
          onClick: handleResetTableClick
        }
      )
    }

    return items
  }, [tableData, type])

  const tableName =
    type === 'import' ? primaryKeys?.table : primaryKeys?.targetTable

  return (
    <>
      <ViewBaseLayout>
        <div className="detailed-view-header">
          {type === 'import' && <h1>{`${database}.${table}`}</h1>}
          {type === 'export' && (
            <h1>{`${connection}.${targetSchema}.${targetTable}`}</h1>
          )}
          <DropdownActions
            isDropdownActionsOpen={openDropdown === 'dropdownActions'}
            marginTop={5}
            onToggle={(isDropdownActionsOpen: boolean) =>
              handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
            }
            items={dropdownActionsItems}
          />
        </div>
        <div className="tabs">
          <h2
            className={selectedTab === 'settings' ? 'active-tab' : ''}
            onClick={() => handleTabClick('settings')}
          >
            Settings
          </h2>
          <h2
            className={selectedTab === 'columns' ? 'active-tab' : ''}
            onClick={() => handleTabClick('columns')}
          >
            Columns
          </h2>
          <h2
            style={{ color: '#a4a4a4', cursor: 'default' }}
            // className={selectedTab === 'statistics' ? 'active-tab' : ''}
            // onClick={() => handleTabClick('statistics')}
          >
            Statistics
          </h2>
        </div>
        <Outlet />
        {isRepairTableModalOpen && primaryKeys && (
          <RepairTableModal
            type={type}
            primaryKeys={primaryKeys}
            isRepairTableModalOpen={isRepairTableModalOpen}
            onClose={() => setIsRepairTableModalOpen(false)}
            exportDatabase={exportDatabase ? exportDatabase : ''}
          />
        )}
        {isResetTableModalOpen && primaryKeys && (
          <ResetTableModal
            type={type}
            primaryKeys={primaryKeys}
            isResetTableModalOpen={isResetTableModalOpen}
            onClose={() => setIsResetTableModalOpen(false)}
            exportDatabase={exportDatabase ? exportDatabase : ''}
          />
        )}
        {isRenameTableModalOpen && primaryKeys && tableData && (
          <RenameTableModal
            type={type}
            primaryKeys={primaryKeys}
            isRenameTableModalOpen={isRenameTableModalOpen}
            onSave={handleSaveRename}
            onClose={() => setIsRenameTableModalOpen(false)}
            errorMessage={errorMessage}
          />
        )}
        {isCopyTableModalOpen && primaryKeys && tableData && (
          <CopyTableModal
            type={type}
            primaryKeys={primaryKeys}
            isCopyTableModalOpen={isCopyTableModalOpen}
            onSave={handleSaveCopy}
            onClose={() => setIsCopyTableModalOpen(false)}
          />
        )}
        {showDeleteConfirmation && (
          <DeleteModal
            title={`Delete ${tableName}`}
            message={`Are you sure that you want to delete table "${tableName}"?`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={handleDeleteTable}
            onCancel={() => setShowDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default TableDetailedView
