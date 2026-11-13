import { CSSProperties, useRef } from 'react'
import './Modals.scss'
import { useAtom } from 'jotai'
import PulseLoader from 'react-spinners/PulseLoader'
import { isMainSidebarMinimized } from '../../atoms/atoms'
import { useFocusTrap } from '../../utils/hooks'
import Button from '../Button'

interface DeleteModalProps {
  title: string
  message: string
  buttonTitleCancel: string
  onCancel: () => void
  isActive: boolean
  isLoading?: boolean
  errorInfo?: string | null
  buttonTitleConfirm?: string
  onConfirm?: () => void
}

function DeleteModal({
  title,
  message,
  buttonTitleCancel,
  onCancel,
  isActive,
  isLoading = false,
  errorInfo = null,
  buttonTitleConfirm,
  onConfirm
}: DeleteModalProps) {
  const DeleteModalRef = useRef<HTMLDivElement>(null)
  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  useFocusTrap(DeleteModalRef, isActive)

  const override: CSSProperties = {
    display: 'inline',
    marginTop: 5
  }

  return (
    <div className="confirmation-modal-backdrop">
      <div
        className={`confirmation-modal-content ${
          mainSidebarMinimized ? 'sidebar-minimized' : ''
        }`}
        ref={DeleteModalRef}
      >
        <div style={{ display: 'flex' }}>
          <h3 className="confirmation-modal-h3">{title}</h3>
          {isLoading && !onConfirm && (
            <PulseLoader loading={isLoading} cssOverride={override} size={3} />
          )}
        </div>

        {errorInfo ? (
          <p>Delete failed:</p>
        ) : (
          <>
            <p style={{ marginBottom: 5 }}>{message}</p>
            <p style={{ margin: 0, color: '#e30505ed' }}>
              Delete is irreversable.
            </p>
          </>
        )}
        <div className="modal-message-container">
          {isLoading ? (
            <div className="modal-loading-container">
              <div style={{ margin: 0 }}>Deleting</div>
              <PulseLoader
                loading={isLoading}
                cssOverride={override}
                size={3}
              />
            </div>
          ) : errorInfo ? (
            <p className="error-message" style={{ marginTop: 0 }}>
              {errorInfo}
            </p>
          ) : null}
        </div>

        <div className="confirmation-modal-footer">
          <Button
            title={!errorInfo ? buttonTitleCancel : 'Close'}
            onClick={onCancel}
            lightStyle={true}
            disabled={isLoading}
          />
          {onConfirm && buttonTitleConfirm && !errorInfo && (
            <Button
              title={buttonTitleConfirm}
              onClick={onConfirm}
              disabled={isLoading}
            />
          )}
        </div>
      </div>
    </div>
  )
}

export default DeleteModal
