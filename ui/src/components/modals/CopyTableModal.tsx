import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'
import {
  EditSetting,
  EditSettingValueTypes,
  ExportPKs,
  ExportSearchFilter,
  ImportPKs,
  ImportSearchFilter
} from '../../utils/interfaces'
import {
  useSearchExportTables,
  useSearchImportTables
} from '../../utils/queries'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../../utils/TableInputFields'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import './Modals.scss'
import {
  copyExportTableSettings,
  copyImportTableSettings
} from '../../utils/cardRenderFormatting'
import InfoText from '../InfoText'
import { debounce } from 'lodash'
import { getUpdatedSettingValue } from '../../utils/functions'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { isMainSidebarMinimized } from '../../atoms/atoms'
import { useAtom } from 'jotai'
import infoTexts from '../../infoTexts.json'

interface CopyTableModalProps {
  type: 'import' | 'export'
  primaryKeys: ImportPKs | ExportPKs
  isCopyTableModalOpen: boolean
  onSave: (
    type: 'import' | 'export',
    tablePrimaryKeysSettings: EditSetting[]
  ) => void
  onClose: () => void
}

function CopyTableModal({
  type,
  primaryKeys,
  isCopyTableModalOpen,
  onSave,
  onClose
}: CopyTableModalProps) {
  const settings =
    type === 'import'
      ? copyImportTableSettings(
          (primaryKeys as ImportPKs).database,
          (primaryKeys as ImportPKs).table
        )
      : copyExportTableSettings(
          (primaryKeys as ExportPKs).connection,
          (primaryKeys as ExportPKs).targetTable,
          (primaryKeys as ExportPKs).targetSchema
        )

  const validationMessageTemplate =
    type === 'import'
      ? 'Table name already exists in the given database.'
      : 'Table name already exists in the given connection and target schema.'

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(settings)
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [isValidationError, setIsValidationError] = useState(true)
  const [validationMessage, setValidationMessage] = useState(
    validationMessageTemplate
  )
  const [pendingValidation, setPendingValidation] = useState(false)

  const [modalWidth, setModalWidth] = useState(584)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isCopyTableModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

  // For validating unique combination of pk's so it becomes a copy and not an update
  const [filter, setFilter] = useState<
    Partial<ImportSearchFilter & ExportSearchFilter>
  >({
    connection: null,
    database: null,
    table: null,
    targetSchema: null,
    targetTable: null,
    includeInAirflow: null,
    importPhaseType: null,
    etlPhaseType: null,
    importTool: null,
    etlEngine: null,
    exportType: null,
    exportTool: null
  })

  const updateFilter = useMemo(
    () =>
      debounce((updatedFilter) => {
        setFilter(updatedFilter)
        setPendingValidation(false)
      }, 500),
    [setFilter]
  )

  const {
    data: importData,
    isLoading: importLoading,
    isError: importError
  } = useSearchImportTables(
    type === 'import' && filter.database && filter.table
      ? (filter as ImportSearchFilter)
      : null,
    true
  )

  const {
    data: exportData,
    isLoading: exportLoading,
    isError: exportError
  } = useSearchExportTables(
    type === 'export' &&
      filter.connection &&
      filter.targetSchema &&
      filter.targetTable
      ? (filter as ExportSearchFilter)
      : null,
    true
  )

  const data = type === 'import' ? importData : exportData
  const validationIsLoading = type === 'import' ? importLoading : exportLoading
  const isError = type === 'import' ? importError : exportError

  useEffect(() => {
    if (!validationIsLoading && data) {
      setPendingValidation(false)
      if (data.tables.length > 0) {
        setIsValidationError(true)
        setValidationMessage(validationMessageTemplate)
      } else {
        setIsValidationError(false)
        setValidationMessage('')
      }
    } else if (isError) {
      setPendingValidation(false)
      setIsValidationError(true)
      setValidationMessage('Error validating table name. Please try again.')
    }
  }, [data, isError, validationIsLoading, validationMessageTemplate])

  const handleInputChange = (
    index: number,
    newValue: EditSettingValueTypes | null,
    isBlur?: boolean
  ) => {
    if (isBlur) return

    const trimmedValue =
      typeof newValue === 'string' ? newValue.trim() : newValue

    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    // Update the edited settings
    const updatedSettings = editedSettings?.map((setting, i) =>
      i === index ? { ...setting, value: trimmedValue } : setting
    )

    setEditedSettings(updatedSettings)
    setHasChanges(true)
    setPendingValidation(true)

    // Define fields to check based on the type
    if (type === 'import') {
      const updatedDatabase = getUpdatedSettingValue(
        'Database',
        updatedSettings
      )
      const updatedTable = getUpdatedSettingValue('Table', updatedSettings)

      if (updatedDatabase && updatedTable) {
        updateFilter({
          ...filter,
          database: updatedDatabase,
          table: updatedTable
        })
      }
    } else if (type === 'export') {
      const updatedConnection = getUpdatedSettingValue(
        'Connection',
        updatedSettings
      )
      const updatedTargetTable = getUpdatedSettingValue(
        'Target Table',
        updatedSettings
      )
      const updatedTargetSchema = getUpdatedSettingValue(
        'Target Schema',
        updatedSettings
      )

      if (updatedConnection && updatedTargetTable && updatedTargetSchema) {
        updateFilter({
          ...filter,
          connection: updatedConnection,
          targetTable: updatedTargetTable,
          targetSchema: updatedTargetSchema
        })
      }
    }
  }

  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string,
    isBlur?: boolean
  ) => {
    if (isBlur) return

    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    }
  }

  const handleSave = () => {
    onSave(type, editedSettings)
    onClose()
  }

  const handleCancelClick = () => {
    if (hasChanges) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  const handleMouseDown = (e: { clientX: SetStateAction<number> }) => {
    setIsResizing(true)
    setInitialMouseX(e.clientX)
    setInitialWidth(modalWidth)
    document.body.classList.add('resizing')
  }

  const handleMouseUp = useCallback(() => {
    setIsResizing(false)
    document.body.classList.remove('resizing')
  }, [])

  const handleMouseMove = useCallback(
    (e: { clientX: number }) => {
      if (isResizing) {
        const deltaX = e.clientX - initialMouseX
        setModalWidth(Math.max(initialWidth + deltaX, MIN_WIDTH))
      }
    },
    [isResizing, initialMouseX, initialWidth]
  )

  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    } else {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing, handleMouseMove, handleMouseUp])

  return (
    <div className="table-modal-backdrop">
      <div
        className={`table-modal-content ${
          mainSidebarMinimized ? 'sidebar-minimized' : ''
        }`}
        style={{ width: `${modalWidth}px` }}
        ref={containerRef}
      >
        {/* <div
          className="table-modal-resize-handle left"
          onMouseDown={handleMouseDown}
        ></div> */}
        <div
          className="table-modal-resize-handle right"
          onMouseDown={handleMouseDown}
        ></div>
        <h2 className="table-modal-h2">
          Copy table
          {infoTexts.actions.copyTable &&
            infoTexts.actions.copyTable.length > 0 && (
              <InfoText
                label="Copy table"
                infoText={infoTexts.actions.copyTable}
                iconPosition={{ marginLeft: 12 }}
                isInfoTextPositionRight={true}
                infoTextMaxWidth={300}
              />
            )}
        </h2>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            const activeElement = document.activeElement as HTMLElement

            // Triggers save only if Save button with type submit is clicked or focused+Enter
            if (
              activeElement &&
              activeElement.getAttribute('type') === 'submit'
            ) {
              handleSave()
            }
          }}
          autoComplete="off"
        >
          <div className="table-modal-body">
            {editedSettings &&
              editedSettings.map((setting, index) => (
                <div key={index} className="table-modal-setting">
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                  />
                  {setting.infoText && setting.infoText.length > 0 && (
                    <InfoText
                      label={setting.label}
                      infoText={setting.infoText}
                    />
                  )}
                </div>
              ))}
          </div>
          <RequiredFieldsInfo
            validation={true}
            isValidationSad={isValidationError}
            validationText={validationMessage}
            isRequiredFieldEmpty={isRequiredFieldEmpty}
          />
          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button
              type="submit"
              title="Save"
              disabled={
                isRequiredFieldEmpty ||
                isValidationError ||
                validationIsLoading ||
                pendingValidation
              }
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Copy table"
          message="Any unsaved changes will be lost."
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Cancel"
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          isActive={showConfirmation}
        />
      )}
    </div>
  )
}

export default CopyTableModal
