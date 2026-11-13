import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'
import ReactDOM from 'react-dom'
import {
  EditSetting,
  EditSettingValueTypes,
  ImportSearchFilter
} from '../../utils/interfaces'
import { useConnections, useSearchImportTables } from '../../utils/queries'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../../utils/TableInputFields'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import './Modals.scss'
import { initialCreateImportTableSettings } from '../../utils/cardRenderFormatting'
import InfoText from '../InfoText'
import { debounce } from 'lodash'
import { getUpdatedSettingValue } from '../../utils/functions'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { isMainSidebarMinimized } from '../../atoms/atoms'
import { useAtom } from 'jotai'

interface CreateTableModalProps {
  isCreateModalOpen: boolean
  prefilledDatabase: string | null
  prefilledConnection: string | null
  onSave: (newTableData: EditSetting[]) => void
  onClose: () => void
}

function CreateImportTableModal({
  isCreateModalOpen,
  prefilledDatabase,
  prefilledConnection,
  onSave,
  onClose
}: CreateTableModalProps) {
  const settings = initialCreateImportTableSettings(
    prefilledDatabase,
    prefilledConnection
  )
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(settings)
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [isValidationError, setIsValidationError] = useState(false)
  const [validationMessage, setValidationMessage] = useState('')
  const [pendingValidation, setPendingValidation] = useState(false)

  const [modalWidth, setModalWidth] = useState(700)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isCreateModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

  // For validating unique combination of pk's so it becomes a create and not an update
  const [filter, setFilter] = useState<ImportSearchFilter>({
    connection: null,
    database: null,
    table: null,
    includeInAirflow: null,
    importPhaseType: null,
    etlPhaseType: null,
    importTool: null,
    etlEngine: null
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
    data,
    isLoading: validationIsLoading,
    isError
  } = useSearchImportTables(
    filter.database && filter.table ? filter : null,
    true
  )

  useEffect(() => {
    if (!validationIsLoading && data) {
      setPendingValidation(false)
      if (data.tables.length > 0) {
        setIsValidationError(true)
        setValidationMessage('Table name already exists in the given database.')
      } else {
        setIsValidationError(false)
        setValidationMessage('')
      }
    } else if (isError) {
      setPendingValidation(false)
      setIsValidationError(true)
      setValidationMessage('Error validating table name. Please try again.')
    }
  }, [data, isError, validationIsLoading])

  const handleInputChange = (
    index: number,
    newValue: EditSettingValueTypes | null,
    isBlur?: boolean
  ) => {
    if (isBlur) return

    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    const updatedSettings = editedSettings?.map((setting, i) =>
      i === index ? { ...setting, value: newValue } : setting
    )

    setEditedSettings(updatedSettings)
    setHasChanges(true)
    setPendingValidation(false)

    const updatedDatabase = getUpdatedSettingValue('Database', updatedSettings)
    const updatedTable = getUpdatedSettingValue('Table', updatedSettings)

    if (updatedDatabase && updatedTable) {
      updateFilter({
        ...filter,
        database: updatedDatabase,
        table: updatedTable
      })
    }
  }

  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string
  ) => {
    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    }
  }

  const handleSave = () => {
    const newTableSettings = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      return editedSetting ? { ...setting, ...editedSetting } : { ...setting }
    })

    onSave(newTableSettings)
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

  return ReactDOM.createPortal(
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
        <h2 className="table-modal-h2">Create table</h2>
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
                    dataNames={connectionNames}
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
          title="Cancel Create table"
          message="Any unsaved changes will be lost."
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Cancel"
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          isActive={showConfirmation}
        />
      )}
    </div>,
    document.body
  )
}

export default CreateImportTableModal
