import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'
import { EditSetting, EditSettingValueTypes } from '../../utils/interfaces'
import { useAllAirflows } from '../../utils/queries'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../../utils/TableInputFields'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import './Modals.scss'
import { renameAirflowSettings } from '../../utils/cardRenderFormatting'
import InfoText from '../InfoText'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../../atoms/atoms'

interface RenameAirflowModalProps {
  isRenameAirflowModalOpen: boolean
  type: 'import' | 'export' | 'custom'
  dagName: string
  onSave: (newDagName: string) => void
  onClose: () => void
}

function RenameAirflowModal({
  isRenameAirflowModalOpen,
  type,
  dagName,
  onSave,
  onClose
}: RenameAirflowModalProps) {
  const settings = renameAirflowSettings(type, dagName)

  const { data: airflowsData } = useAllAirflows()
  const airflowNames = useMemo(
    () =>
      Array.isArray(airflowsData)
        ? airflowsData.map((airflow) => airflow.name)
        : [],
    [airflowsData]
  )

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(
    settings ? settings : []
  )
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)

  // For validating unique pk so it becomes a create and not an update
  const [duplicateDagName, setDuplicateDagName] = useState(true)
  const [pendingValidation, setPendingValidation] = useState(false)

  const [modalWidth, setModalWidth] = useState(700)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isRenameAirflowModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

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

  if (!settings) {
    return
  }

  const handleInputChange = (
    index: number,
    newValue: EditSettingValueTypes | null
  ) => {
    setPendingValidation(true)

    if (index < 0 || index >= editedSettings.length) {
      console.warn(`Invalid index: ${index}`)
      return
    }

    const updatedSettings = editedSettings?.map((setting, i) =>
      i === index ? { ...setting, value: newValue } : setting
    )

    const dagNameSetting = updatedSettings.find(
      (setting) => setting.label === 'DAG Name'
    )
    if (
      dagNameSetting &&
      airflowNames.includes(dagNameSetting.value as string)
    ) {
      setDuplicateDagName(true)
    } else {
      setDuplicateDagName(false)
      setPendingValidation(false)
    }

    setEditedSettings(updatedSettings)
    setHasChanges(true)
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
    if (duplicateDagName) return

    const newDagName: string = editedSettings.find(
      (es) => es.label === 'DAG Name'
    )?.value as string

    onSave(newDagName)
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
        <h2 className="table-modal-h2">Rename DAG</h2>
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
            isRequiredFieldEmpty={isRequiredFieldEmpty}
            validation={true}
            isValidationSad={duplicateDagName}
            validationText="DAG Name already exists. Please choose a different name."
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
                isRequiredFieldEmpty || duplicateDagName || pendingValidation
              }
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Rename DAG"
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

export default RenameAirflowModal
