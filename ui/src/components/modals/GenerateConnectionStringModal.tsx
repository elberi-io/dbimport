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
  GenerateJDBCconnectionString
} from '../../utils/interfaces'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import TableInputFields from '../../utils/TableInputFields'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import './Modals.scss'
import InfoText from '../InfoText'
import { useJDBCDrivers } from '../../utils/queries'

import { generateConnectionStringFields } from '../../utils/cardRenderFormatting'
import { transformGenerateConnectionSettings } from '../../utils/dataFunctions'
import { useGenerateConnectionString } from '../../utils/mutations'
import { useFocusTrap, useIsRequiredFieldsEmpty } from '../../utils/hooks'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../../atoms/atoms'

interface GenerateConnectionStringModalProps {
  isGenerateModalOpen: boolean
  setGeneratedConnectionString: (value: string) => void
  onClose: () => void
}

function GenerateConnectionStringModal({
  isGenerateModalOpen,
  setGeneratedConnectionString,
  onClose
}: GenerateConnectionStringModalProps) {
  const { mutate: generateConnectionString } = useGenerateConnectionString()

  const { data: originalDriverData, isLoading, isError } = useJDBCDrivers()
  const databaseTypeNames = useMemo(() => {
    return Array.isArray(originalDriverData)
      ? Array.from(
          new Set(originalDriverData.map((driver) => driver.databaseType))
        )
      : []
  }, [originalDriverData])

  const settings = generateConnectionStringFields

  const [editedSettings, setEditedSettings] = useState<EditSetting[]>(settings)
  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)

  const [modalWidth, setModalWidth] = useState(700)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(700)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isGenerateModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const isRequiredFieldEmpty = useIsRequiredFieldsEmpty(editedSettings)

  const availableVersions = useMemo(() => {
    const selectedDatabaseType = editedSettings.find(
      (setting) => setting.label === 'Database type'
    )?.value

    return Array.isArray(originalDriverData)
      ? originalDriverData
          .filter((data) => data.databaseType === selectedDatabaseType)
          .map((data) => data.version)
      : []
  }, [editedSettings, originalDriverData])

  const isDropdownDisabled = useMemo(
    () => availableVersions.length <= 1,
    [availableVersions]
  )

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

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (isLoading) {
    return <div className="loading">Loading...</div>
  }
  if (!originalDriverData) {
    return (
      <div className="text-block">
        <p>Error. No data from REST server.</p>
      </div>
    )
  }

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

    const updatedSettings = editedSettings.map((setting, i) =>
      i === index ? { ...setting, value: newValue } : setting
    )

    setEditedSettings(updatedSettings)
    setHasChanges(true)

    // Resets Version when Database type changes
    if (editedSettings[index].label === 'Database type') {
      const versionIndex = updatedSettings.findIndex(
        (s) => s.label === 'Version'
      )
      if (versionIndex !== -1) {
        updatedSettings[versionIndex] = {
          ...updatedSettings[versionIndex],
          value: 'default'
        }
      }
    }

    setEditedSettings(updatedSettings)
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

  const handleGenerate = () => {
    const newGenerateSettings: EditSetting[] = settings.map((setting) => {
      const editedSetting = editedSettings.find(
        (es) => es.label === setting.label
      )

      return editedSetting ? { ...setting, ...editedSetting } : { ...setting }
    })

    const generateConnectionStringData: GenerateJDBCconnectionString =
      transformGenerateConnectionSettings(newGenerateSettings)

    generateConnectionString(generateConnectionStringData, {
      onSuccess: (response) => {
        console.log('Generate connection string successful')
        setGeneratedConnectionString(response.connectionString)
        onClose()
      },
      onError: (error) => {
        console.log('Error updating JDBC Drivers', error.message)
      }
    })
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
        <h2 className="table-modal-h2">Generate connection string</h2>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            const activeElement = document.activeElement as HTMLElement

            // Triggers save only if Save button with type submit is clicked or focused+Enter
            if (
              activeElement &&
              activeElement.getAttribute('type') === 'submit'
            ) {
              handleGenerate()
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
                    dataNames={databaseTypeNames}
                    versionNames={availableVersions}
                    disabled={setting.label === 'Version' && isDropdownDisabled}
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
          <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />
          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button
              type="submit"
              title="Generate"
              disabled={isRequiredFieldEmpty}
            />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Cancel Generate connection string"
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

export default GenerateConnectionStringModal
