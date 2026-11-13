import { AxiosError } from 'axios'
import { useState } from 'react'
import EditTableModal from './EditTableModal'
import { tableNameReadonlySettings } from '../../utils/cardRenderFormatting'
import { useResetTable } from '../../utils/mutations'
import { ErrorData, ExportPKs, ImportPKs } from '../../utils/interfaces'
import infoTexts from '../../infoTexts.json'
import { isImportPKs } from '../../utils/functions'

interface ResetTableModalProps {
  type: 'import' | 'export'
  primaryKeys: ImportPKs | ExportPKs
  isResetTableModalOpen: boolean
  onClose: () => void
  exportDatabase?: string
}

function ResetTableModal({
  type,
  primaryKeys,
  isResetTableModalOpen,
  onClose,
  exportDatabase
}: ResetTableModalProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [successMessage, setSuccessMessage] = useState<string | null>(null)

  const { mutate: resetTable } = useResetTable()

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

  const warningMessage = `This will truncate the Target table and force the next ${type} to start from the beginning with a full ${type}.`

  const handleResetTable = () => {
    if (!tableName) {
      console.log('No selected row or table name')
      return
    }

    setIsLoading(true)

    resetTable(
      { type, primaryKeys },
      {
        onSuccess: () => {
          setIsLoading(false)
          setSuccessMessage('Reset table succeeded')
          console.log('Reset table succeeded')
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
      isEditModalOpen={isResetTableModalOpen}
      title={`Reset table`}
      titleInfoText={infoTexts.actions.resetTable}
      settings={settings}
      onSave={handleResetTable}
      onClose={() => {
        onClose()
        setErrorMessage(null)
        setSuccessMessage(null)
      }}
      isNoCloseOnSave={true}
      initWidth={300}
      isLoading={isLoading}
      loadingText="Reseting"
      successMessage={successMessage}
      errorMessage={errorMessage ? errorMessage : null}
      onResetErrorMessage={() => setErrorMessage(null)}
      warningMessage={warningMessage}
      submitButtonTitle="Reset"
      closeButtonTitle="Close"
    />
  )
}

export default ResetTableModal
