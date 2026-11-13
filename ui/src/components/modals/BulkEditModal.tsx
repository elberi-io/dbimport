import { useCallback, useEffect, useRef, useState } from 'react'
import './Modals.scss'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../../atoms/atoms'
import BulkInputFields, {
  BulkInputFieldsProps
} from '../../utils/BulkInputFields'
import { useFocusTrap } from '../../utils/hooks'
import { BulkField } from '../../utils/interfaces'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'

interface BulkEditModalProps<T> {
  isBulkEditModalOpen: boolean
  title: string
  selectedRows: T[]
  bulkFieldsData: BulkField<T>[]
  onSave: (
    bulkChanges: Partial<Record<keyof T, string | number | boolean | null>>
  ) => void
  onClose: () => void
  initWidth?: number
}

function BulkEditModal<T>({
  isBulkEditModalOpen,
  title,
  selectedRows,
  bulkFieldsData,
  onSave,
  onClose,
  initWidth
}: BulkEditModalProps<T>) {
  const [bulkChanges, setBulkChanges] = useState<
    Partial<Record<keyof T, string | number | boolean | null>>
  >({})
  const [showConfirmation, setShowConfirmation] = useState(false)
  const [modalWidth, setModalWidth] = useState(initWidth ?? 584)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(initWidth ?? 584)
  const MIN_WIDTH = initWidth ?? 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isBulkEditModalOpen, showConfirmation)

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const handleBulkChangeInternal = useCallback(
    (
      fieldKey: keyof T,
      newValue: string | number | boolean | null | undefined
    ) => {
      setBulkChanges((prev) => {
        const updatedBulkChanges = { ...prev }

        if (newValue === undefined) {
          delete updatedBulkChanges[fieldKey]
        } else {
          updatedBulkChanges[fieldKey] = newValue === '' ? null : newValue
        }

        return updatedBulkChanges
      })
    },
    []
  )

  const handleSave = () => {
    onSave(bulkChanges)
    onClose()
  }

  const handleCancelClick = () => {
    const hasChanges = Object.keys(bulkChanges).length > 0
    if (hasChanges) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setBulkChanges({})
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  const handleMouseDown = (e: React.MouseEvent) => {
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
    (e: MouseEvent) => {
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

  const BulkInputFieldsTyped = BulkInputFields as React.FC<
    BulkInputFieldsProps<T>
  >

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
            <BulkInputFieldsTyped
              fields={bulkFieldsData}
              selectedRows={selectedRows}
              bulkChanges={bulkChanges}
              onBulkChange={handleBulkChangeInternal}
            />
          </div>

          <div className="table-modal-footer">
            <Button
              title="Cancel"
              onClick={handleCancelClick}
              lightStyle={true}
            />
            <Button
              type="submit"
              title="Save Changes"
              disabled={Object.keys(bulkChanges).length === 0}
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

export default BulkEditModal
