import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'
import { EditSetting, EditSettingValueTypes } from '../../utils/interfaces'
import { useConnections } from '../../utils/queries'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../../utils/TableInputFields'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import './Modals.scss'
import InfoText from '../InfoText'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../../atoms/atoms'

interface EditModalProps {
  isEditModalOpen: boolean
  title: string
  settings: EditSetting[]
  onSave: (newSettings: EditSetting[]) => void
  onClose: () => void
}

function EditConnectionModal({
  isEditModalOpen,
  title,
  settings,
  onSave,
  onClose
}: EditModalProps) {
  const editableSettings =
    settings?.filter((setting) => {
      const isReadonly = setting.type === 'readonly'
      const isHidden = setting.isHidden
      const isDatabaseOrTable =
        setting.label === 'Database' || setting.label === 'Table'

      return !isHidden && (!isReadonly || isDatabaseOrTable)
    }) ?? []
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData?.map((connection) => connection.name)
        : [],
    [connectionsData]
  )
  const [originalEditableSettings] = useState(editableSettings)
  const [editedSettings, setEditedSettings] = useState(editableSettings)
  const [changedSettings, setChangedSettings] = useState<
    Map<
      string,
      {
        original: EditSettingValueTypes | null
        new: string | number | boolean | null
      }
    >
  >(new Map())
  const [prevValue, setPrevValue] = useState<string | number | boolean>('')
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [modalWidth, setModalWidth] = useState(700)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isEditModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

  const airflowEmailOnFailure = editedSettings.find(
    (s) => s.label === 'Email on Failure'
  )
  const airflowEmailOnRetries = editedSettings.find(
    (s) => s.label === 'Email on Retries'
  )
  const isAirflowEmailDisabled =
    airflowEmailOnFailure?.value === false &&
    airflowEmailOnRetries?.value === false
      ? true
      : false

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean | null,
    isBlur?: boolean
  ) => {
    if (isBlur) return

    // Creates a new array, copying all elements of editedSettings
    const newSettings = [...editedSettings]
    const setting = newSettings[index]
    const currentValue = index === -1 ? null : newSettings[index].value
    const originalValue = originalEditableSettings[index]?.value

    if (
      (setting.type === 'integerFromOne' ||
        setting.type === 'integerFromZero') &&
      typeof originalValue === 'number' &&
      Number.isInteger(originalValue)
    ) {
      setPrevValue(originalValue)
    }

    if (newValue === -1) {
      if (
        typeof currentValue === 'number' &&
        currentValue !== -1 &&
        Number.isInteger(currentValue)
      ) {
        setPrevValue(currentValue)
      }
    }

    // Stores previous value if it's a whole number when newValue is null
    if (newValue === null) {
      if (
        typeof currentValue === 'number' &&
        currentValue !== null &&
        Number.isInteger(currentValue)
      ) {
        setPrevValue(currentValue)
      }
    }
    // Creates a new object for the setting being updated
    const updatedSetting = { ...newSettings[index], value: newValue }
    // Replaces the old object in the array with the new object
    newSettings[index] = updatedSetting

    const updatedChangedSettings = new Map(changedSettings)
    if (newValue !== originalValue) {
      updatedChangedSettings.set(setting.label, {
        original: originalValue,
        new: newValue
      })
    } else {
      updatedChangedSettings.delete(setting.label)
    }

    setChangedSettings(updatedChangedSettings)
    setEditedSettings(newSettings)
  }

  const handleSelect = (
    item: string | number | boolean | null,
    keyLabel?: string
  ) => {
    const index = editedSettings.findIndex(
      (setting) => setting.label === keyLabel
    )
    if (index !== -1) {
      handleInputChange(index, item)
    } else {
      handleInputChange(index, item)
    }
  }

  const handleSave = () => {
    // Creates a new updatedSettings array by merging editedSettings into the original settings, ensuring immutability.
    const updatedSettings = Array.isArray(settings)
      ? settings.map((setting) => {
          const editedSetting = editedSettings.find(
            (es) => es.label === setting.label
          )

          return editedSetting
            ? { ...setting, ...editedSetting }
            : { ...setting }
        })
      : []
    onSave(updatedSettings)
    onClose()
  }

  const handleCancelClick = () => {
    if (changedSettings.size > 0) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setEditedSettings(originalEditableSettings)
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
        <h2 className="table-modal-h2">{title}</h2>
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
            {Array.isArray(editedSettings) &&
              editedSettings.map((setting, index) => (
                <div key={index} className="table-modal-setting">
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                    prevValue={prevValue}
                    dataNames={connectionNames}
                    isAirflowEmailDisabled={isAirflowEmailDisabled}
                  />
                  {setting.infoText && setting.infoText.length > 0 && (
                    <InfoText
                      label={setting.label}
                      infoText={setting.infoText}
                      infoTextMaxWidth={
                        setting.type === 'text' ||
                        setting.type === 'textarea' ||
                        setting.type === 'booleanNumberOrAuto' ||
                        setting.type === 'booleanOrDefaultFromConfig(-1)' ||
                        setting.type === 'booleanOrDefaultFromConnection(-1)' ||
                        setting.type ===
                          'integerFromOneOrDefaultFromConfig(null)'
                          ? 430
                          : setting.type === 'enum'
                          ? 280
                          : setting.type === 'boolean' ||
                            setting.type === 'booleanNumber'
                          ? 380
                          : setting.type === 'integerOneOrTwo' ||
                            setting.type === 'integerFromZero' ||
                            setting.type === 'integerFromOne' ||
                            setting.type === 'integerFromZeroOrNull' ||
                            setting.type === 'integerFromOneOrNull' ||
                            setting.type === 'integerFromZeroOrAuto(-1)' ||
                            setting.type === 'integerFromOneOrAuto(-1)'
                          ? 343
                          : 270
                      }
                    />
                  )}
                </div>
              ))}
          </div>
          <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />

          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button
              type="submit"
              title="Save"
              disabled={isRequiredFieldEmpty}
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Edit"
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

export default EditConnectionModal
