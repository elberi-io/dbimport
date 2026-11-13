import { AxiosError } from 'axios'
import { useState } from 'react'
import EditTableModal from './EditTableModal'
import { tableNameReadonlySettings } from '../../utils/cardRenderFormatting'
import { useRepairTable } from '../../utils/mutations'
import { ErrorData, ExportPKs, ImportPKs } from '../../utils/interfaces'
import infoTexts from '../../infoTexts.json'
import { isImportPKs } from '../../utils/functions'

interface RepairTableModalProps {
  type: 'import' | 'export'
  primaryKeys: ImportPKs | ExportPKs
  isRepairTableModalOpen: boolean
  onClose: () => void
  exportDatabase?: string | null | undefined
}

function RepairTableModal({
  type,
  primaryKeys,
  isRepairTableModalOpen,
  onClose,
  exportDatabase
}: RepairTableModalProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [successMessage, setSuccessMessage] = useState<string | null>(null)

  const { mutate: repairTable } = useRepairTable()

  const tableName = isImportPKs(primaryKeys, type)
    ? primaryKeys.table // Narrowed to ImportPKs
    : primaryKeys.targetTable

  const databaseName = isImportPKs(primaryKeys, type)
    ? primaryKeys.database // Narrowed to ImportPKs
    : (exportDatabase as string)

  const connectionName = isImportPKs(primaryKeys, type)
    ? undefined
    : primaryKeys.connection

  const schemaName = isImportPKs(primaryKeys, type)
    ? undefined
    : primaryKeys.targetSchema

  const label: string = type === 'import' ? 'Table' : 'Target Table'

  const settings = tableNameReadonlySettings(
    label,
    databaseName,
    tableName,
    connectionName,
    schemaName ? schemaName : undefined
  )

  const handleRepairTable = () => {
    if (!tableName) {
      console.log('No selected row or table name')
      return
    }

    setIsLoading(true)

    repairTable(
      { type, primaryKeys },
      {
        onSuccess: () => {
          setIsLoading(false)
          setSuccessMessage('Repair table succeeded')
          console.log('Repair table succeeded')
        },
        onError: (error: AxiosError<ErrorData>) => {
          const errorMessage =
            error.response?.data?.result || 'An unknown error occurred'
          setIsLoading(false)
          setErrorMessage(errorMessage)
        }
      }
    )
  }

  return (
    <EditTableModal
      isEditModalOpen={isRepairTableModalOpen}
      title={`Repair table`}
      titleInfoText={infoTexts.actions.repairTable}
      settings={settings}
      onSave={handleRepairTable}
      onClose={() => {
        onClose()
        setErrorMessage(null)
        setSuccessMessage(null)
      }}
      isNoCloseOnSave={true}
      initWidth={300}
      isLoading={isLoading}
      loadingText="Repairing"
      successMessage={successMessage}
      errorMessage={errorMessage ? errorMessage : null}
      onResetErrorMessage={() => setErrorMessage(null)}
      submitButtonTitle="Repair"
      closeButtonTitle="Close"
    />
  )
}

export default RepairTableModal
