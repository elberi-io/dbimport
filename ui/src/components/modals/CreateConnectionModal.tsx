import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'
import ReactDOM from 'react-dom'
import { EditSetting, EditSettingValueTypes } from '../../utils/interfaces'
import { useConnections } from '../../utils/queries'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../../utils/TableInputFields'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import './Modals.scss'
import InfoText from '../InfoText'
import { initialCreateConnectionSettings } from '../../utils/cardRenderFormatting'
import GenerateConnectionStringModal from './GenerateConnectionStringModal'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../../atoms/atoms'

interface CreateConnectionModalProps {
  isCreateModalOpen: boolean
  onSave: (newTableData: EditSetting[]) => void
  onClose: () => void
}

function CreateConnectionModal({
  isCreateModalOpen,
  onSave,
  onClose
}: CreateConnectionModalProps) {
  const settings = initialCreateConnectionSettings

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(settings)
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [pendingValidation, setPendingValidation] = useState(false)
  const [duplicateConnectionName, setDuplicateConnectionName] = useState(false)

  const [modalWidth, setModalWidth] = useState(750)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(750)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isCreateModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const [isGenerateModalOpen, setIsGenerateModalOpen] = useState(false)
  const [generatedConnectionString, setGeneratedConnectionString] =
    useState<string>('')

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

  // For unique connection name validation
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(() => {
    return Array.isArray(connectionsData)
      ? connectionsData.map((connection) => connection.name)
      : []
  }, [connectionsData])

  const handleInputChange = (
    index: number,
    newValue: EditSettingValueTypes | null,
    isBlur?: boolean
  ) => {
    if (isBlur) return

    setPendingValidation(true)

    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    const updatedSettings = editedSettings.map((setting, i) =>
      i === index
        ? { ...setting, value: newValue === '' ? null : newValue }
        : setting
    )

    const connectionNameSetting = updatedSettings.find(
      (setting) => setting.label === 'Name'
    )

    if (
      connectionNameSetting &&
      connectionNames.includes(connectionNameSetting.value as string)
    ) {
      setDuplicateConnectionName(true)
    } else {
      setDuplicateConnectionName(false)
      setPendingValidation(false)
    }

    setEditedSettings(updatedSettings)
    setHasChanges(true)
  }

  // Not used yet in here, but required for TableList - maybe make handleSelect optional in TableList later
  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string
  ) => {
    console.log('item, keyLabel', item, keyLabel)
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

  useEffect(() => {
    if (generatedConnectionString) {
      setEditedSettings((prevSettings) =>
        prevSettings.map((setting) =>
          setting.label === 'Connection string'
            ? { ...setting, value: generatedConnectionString }
            : setting
        )
      )
      setHasChanges(true) // Mark as having changes for save functionality
    }
  }, [generatedConnectionString])

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
        <h2 className="table-modal-h2">Create connection</h2>
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
                    textareaMaxMinWidth="304px"
                  />
                  {setting.infoText && setting.infoText.length > 0 && (
                    <InfoText
                      label={setting.label}
                      infoText={setting.infoText}
                    />
                  )}
                  <div style={{ width: 100, marginLeft: 8 }}>
                    {setting.label === 'Connection string' && (
                      <Button
                        title="Generate"
                        onClick={() => setIsGenerateModalOpen(true)}
                        height="23px"
                      />
                    )}
                  </div>
                </div>
              ))}
          </div>
          <RequiredFieldsInfo
            isRequiredFieldEmpty={isRequiredFieldEmpty}
            validation={true}
            isValidationSad={duplicateConnectionName}
            validationText="Connection name already exists. Please choose a different name."
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
                duplicateConnectionName ||
                pendingValidation
              }
            />
          </div>
        </form>
      </div>
      {isGenerateModalOpen && (
        <GenerateConnectionStringModal
          isGenerateModalOpen={isGenerateModalOpen}
          setGeneratedConnectionString={setGeneratedConnectionString}
          onClose={() => setIsGenerateModalOpen(false)}
        />
      )}
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Create connection"
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

export default CreateConnectionModal
