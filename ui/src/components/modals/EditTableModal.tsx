import {
  CSSProperties,
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
import { AirflowDAGTaskType } from '../../utils/enums'
import { useLocation } from 'react-router-dom'
import { isValidOctal } from '../../utils/functions'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../../atoms/atoms'
import PulseLoader from 'react-spinners/PulseLoader'

interface EditModalProps {
  isEditModalOpen: boolean
  title: string
  settings: EditSetting[]
  onSave: (newSettings: EditSetting[]) => void
  onClose: () => void
  isNoCloseOnSave?: boolean
  initWidth?: number
  isLoading?: boolean
  loadingText?: string
  successMessage?: string | null
  errorMessage?: string | null
  onResetErrorMessage?: () => void
  warningMessage?: string
  submitButtonTitle?: string
  closeButtonTitle?: string
  titleInfoText?: string | null
}

function EditTableModal({
  isEditModalOpen,
  title,
  settings,
  onSave,
  onClose,
  isNoCloseOnSave,
  initWidth,
  isLoading = false,
  loadingText,
  successMessage,
  errorMessage,
  onResetErrorMessage,
  warningMessage,
  submitButtonTitle = 'Save',
  closeButtonTitle = 'Cancel',
  titleInfoText = null
}: EditModalProps) {
  const editableSettings =
    settings?.filter((setting) => {
      const isReadonly = setting.type === 'readonly'
      const isHidden = setting.isHidden
      const isDisplayReadonly =
        setting.label === 'Database' ||
        setting.label === 'Table' ||
        setting.label === 'Target Table' ||
        setting.label === 'Connection' ||
        setting.label === 'DAG Name'

      return !isHidden && (!isReadonly || isDisplayReadonly)
    }) ?? []
  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData?.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const location = useLocation()
  const pathnames = location.pathname.split('/').filter((x) => x)
  const [isNotTripleOctalValue, setIsNotTripleOctalValue] = useState(false)

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
  const [modalWidth, setModalWidth] = useState(initWidth ? initWidth : 584)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(initWidth ? initWidth : 584)
  const MIN_WIDTH = initWidth ? initWidth : 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isEditModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const validationMethodSetting = editedSettings.find(
    (s) => s.label === 'Validation Method'
  )

  const isValidationCustomQueryDisabled =
    validationMethodSetting?.value !== 'Custom Query'

  const airflowTypeValue = editedSettings.find((s) => s.label === 'Type')

  const isAirflowTasksConnectionDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.JDBCSQL

  const isAirflowTasksSensorPokeAndSoftDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.DAGSensor &&
    airflowTypeValue?.value !== AirflowDAGTaskType.SQLSensor

  const isAirflowTasksSensorConnectionDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.SQLSensor

  const isAirflowTasksSudoUserDisabled =
    airflowTypeValue?.value !== AirflowDAGTaskType.DBImportCommand &&
    airflowTypeValue?.value !== AirflowDAGTaskType.JDBCSQL &&
    airflowTypeValue?.value !== AirflowDAGTaskType.HiveSQL &&
    airflowTypeValue?.value !== AirflowDAGTaskType.HiveSQLScript

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

  const handleInputChange = (
    index: number,
    newValue: string | number | boolean | null,
    isBlur?: boolean
  ) => {
    if (isBlur) return

    if (errorMessage && onResetErrorMessage) {
      onResetErrorMessage()
    }
    // Creates a new array, copying all elements of editedSettings
    const newSettings = [...editedSettings]
    const setting = newSettings[index]
    const currentValue = newSettings[index].value
    const originalValue = originalEditableSettings[index]?.value

    if (setting.label === 'DAG File Permission') {
      const isValueValid = isValidOctal(newValue as string)
      setIsNotTripleOctalValue(!isValueValid)
    }

    if (typeof originalValue === 'string' && pathnames[1] === 'global') {
      setPrevValue(originalValue)
    }

    if (
      (setting.type === 'integerFromOne' ||
        setting.type === 'integerFromZero') &&
      typeof originalValue === 'number' &&
      Number.isInteger(originalValue)
    ) {
      setPrevValue(originalValue)
    }

    // Only stores previous value if it's a whole number
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
    }
  }

  const handleSave = () => {
    if (isNotTripleOctalValue) return

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
    if (!isNoCloseOnSave) {
      onClose()
    }
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
    [isResizing, initialMouseX, initialWidth, MIN_WIDTH]
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

  const override: CSSProperties = {
    display: 'inline',
    marginTop: 1.5
  }

  return (
    <div className="table-modal-backdrop">
      <div
        className={`table-modal-content ${
          mainSidebarMinimized ? 'sidebar-minimized' : ''
        }`}
        style={{ width: `${modalWidth}px`, minWidth: 'fit-content' }}
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
          {title}
          {titleInfoText && titleInfoText.length > 0 && (
            <InfoText
              label={title}
              infoText={titleInfoText}
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
            {Array.isArray(editedSettings) &&
              editedSettings.map((setting, index) => (
                <div key={index} className={'table-modal-setting'}>
                  <TableInputFields
                    index={index}
                    setting={setting}
                    handleInputChange={handleInputChange}
                    handleSelect={handleSelect}
                    prevValue={prevValue}
                    dataNames={connectionNames}
                    isValidationCustomQueryDisabled={
                      isValidationCustomQueryDisabled
                    }
                    isAirflowTasksSensorPokeAndSoftDisabled={
                      isAirflowTasksSensorPokeAndSoftDisabled
                    }
                    isAirflowTasksSensorConnectionDisabled={
                      isAirflowTasksSensorConnectionDisabled
                    }
                    isAirflowTasksSudoUserDisabled={
                      isAirflowTasksSudoUserDisabled
                    }
                    isLoading={isLoading}
                    disabled={isAirflowTasksConnectionDisabled}
                  />
                  {setting.infoText && setting.infoText.length > 0 && (
                    <InfoText
                      label={setting.label}
                      infoText={setting.infoText}
                      iconPosition={
                        setting.type === 'readonly'
                          ? { paddingTop: 0, paddingBottom: 2 }
                          : { paddingTop: 2 }
                      }
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
                      isInfoTextPositionUp={
                        setting.label === 'Spark Executors' ||
                        setting.label === 'Spark Max Executors'
                          ? true
                          : false
                      }
                    />
                  )}
                </div>
              ))}
          </div>
          <div className="modal-message-container">
            {warningMessage && (
              <div className="error-message warning">
                <p>Warning:</p>
                <p>{warningMessage}</p>
              </div>
            )}
            {isLoading ? (
              <div className="modal-loading-container">
                <div style={{ margin: 0 }}>{loadingText}</div>
                <PulseLoader
                  loading={isLoading}
                  cssOverride={override}
                  size={3}
                />
              </div>
            ) : errorMessage ? (
              <p className="error-message" style={{ margin: 0 }}>
                {errorMessage}
              </p>
            ) : successMessage ? (
              <p className="success-message" style={{ margin: 0 }}>
                {successMessage}
              </p>
            ) : location.pathname === '/configuration/global' ? (
              <RequiredFieldsInfo
                isRequiredFieldEmpty={isRequiredFieldEmpty}
                validation={true}
                isValidationSad={isNotTripleOctalValue}
                validationText="DAG File Permission must be a 3-digit or 4-digit octal value."
              />
            ) : (
              <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />
            )}
          </div>
          <div className="table-modal-footer">
            <Button
              title={closeButtonTitle}
              onClick={handleCancelClick}
              lightStyle={true}
              disabled={isLoading}
            />
            <Button
              type="submit"
              title={submitButtonTitle}
              disabled={isRequiredFieldEmpty || isLoading}
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

export default EditTableModal
