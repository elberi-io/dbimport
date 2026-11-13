import { useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../../components/ViewBaseLayout'
import '../../_shared/tableDetailed/DetailedView.scss'
import '../../_shared/tableDetailed/settings/TableSettings.scss'
import '../../../components/Loading.scss'
import ConnectionSettings from './ConnectionSettings'
import { useState } from 'react'
import DropdownActions from '../../../components/DropdownActions'
import TestConnectionIcon from '../../../assets/icons/TestConnectionIcon'
import EncryptIcon from '../../../assets/icons/EncryptIcon'
import EditTableModal from '../../../components/modals/EditTableModal'
import { encryptCredentialsSettings } from '../../../utils/cardRenderFormatting'
import { EditSetting, ErrorData } from '../../../utils/interfaces'
import {
  newCopyCnData,
  transformEncryptCredentialsSettings
} from '../../../utils/dataFunctions'
import {
  useCreateOrUpdateConnection,
  useDeleteConnection,
  useEncryptCredentials,
  useTestConnection
} from '../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { AxiosError } from 'axios'
import ConfirmationModal from '../../../components/modals/ConfirmationModal'
import Button from '../../../components/Button'
import ImportIconSmall from '../../../assets/icons/ImportIconSmall'
import ExportIconSmall from '../../../assets/icons/ExportIconSmall'
import DeleteIcon from '../../../assets/icons/DeleteIcon'
import CopyConnectionModal from '../../../components/modals/CopyConnectionModal'
import { useConnection } from '../../../utils/queries'
import CopyIcon from '../../../assets/icons/CopyIcon'
import DeleteModal from '../../../components/modals/DeleteModal'

function ConnectionDetailedView() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const { connection: connectionParam } = useParams<{
    connection: string
  }>()

  const settings = encryptCredentialsSettings(
    connectionParam ? connectionParam : ''
  )

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [isCopyCnModalOpen, setIsCopyCnModalOpen] = useState(false)
  const { mutate: createConnection } = useCreateOrUpdateConnection()

  const { data: cnData } = useConnection(connectionParam)

  const { mutate: deleteConnection, isSuccess: isDeleteSuccess } =
    useDeleteConnection()
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)
  const [isDeleteLoading, setIsDeleteLoading] = useState(false)
  const [errorMessageDelete, setErrorMessageDelete] = useState<string | null>(
    null
  )

  const [isEncryptModalOpen, setIsEncryptModalOpen] = useState(false)
  const [isEncryptLoading, setIsEncryptLoading] = useState(false)
  const [errorMessageEncrypt, setErrorMessageEncrypt] = useState<string | null>(
    null
  )
  const { mutate: encryptCredentials } = useEncryptCredentials()

  const [isTestCnModalOpen, setIsTestCnModalOpen] = useState(false)
  const [isTestLoading, setIsTestLoading] = useState(true)
  const [errorMessageTest, setErrorMessageTest] = useState<string | null>(null)
  const { mutate: testConnection, isSuccess: isTestSuccess } =
    useTestConnection()

  const handleTestConnection = () => {
    if (!connectionParam) {
      console.log('No connectionParam')
      return
    }

    setIsTestCnModalOpen(true)

    testConnection(connectionParam, {
      onSuccess: () => {
        setIsTestLoading(false)
        console.log('Connection test succeeded')
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.data?.result || 'An unknown error occurred'
        setIsTestLoading(false)
        setErrorMessageTest(errorMessage)
        console.log('Connection test failed', error.message)
      }
    })
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleCopyClick = () => {
    setIsCopyCnModalOpen(true)
  }

  const handleSaveCopy = (newCnName: string) => {
    if (!connectionParam || !cnData) return

    const newCnDataCopy = newCopyCnData(newCnName, cnData)

    createConnection(newCnDataCopy, {
      onSuccess: () => {
        console.log('Save connection copy successful')

        queryClient.invalidateQueries({
          queryKey: ['connection', 'search'],
          exact: false
        })
      },
      onError: (error) => {
        console.log('Error copy connection', error.message)
      }
    })
  }

  const handleSave = (updatedSettings: EditSetting[]) => {
    setIsEncryptLoading(true)
    const encryptCredentialsData =
      transformEncryptCredentialsSettings(updatedSettings)

    encryptCredentials(encryptCredentialsData, {
      onSuccess: () => {
        // For getting fresh data from database to the cache
        queryClient.invalidateQueries({
          queryKey: ['connection'],
          exact: false
        })
        console.log('Update successful')
        setIsEncryptLoading(false)
        setIsEncryptModalOpen(false)
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.data?.result || 'An unknown error occurred'

        setErrorMessageEncrypt(errorMessage)
        setIsEncryptLoading(false)
      }
    })
  }

  const handleDeleteClick = () => {
    if (!connectionParam) return

    setShowDeleteConfirmation(true)
  }

  const handleDeleteCn = async () => {
    if (!connectionParam) return
    setIsDeleteLoading(true)

    deleteConnection(
      { connectionName: connectionParam },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['connection', 'search'], // Matches all related queries that starts the queryKey with 'connection'
            exact: false
          })
          console.log('Delete successful')
          setShowDeleteConfirmation(false)

          navigate(`/connection`, { replace: true })
        },
        onError: (error: AxiosError<ErrorData>) => {
          const errorMessage =
            error.response?.data?.result ||
            error.response?.statusText ||
            'An unknown error occurred'
          setIsDeleteLoading(false)
          setErrorMessageDelete(errorMessage)
        }
      }
    )
  }

  const handleLinkClick = (type: 'import' | 'export', item: string) => {
    const encodedItem = encodeURIComponent(item)

    if (type === 'import') {
      navigate(`/import?connection=${encodedItem}`)
    } else if (type === 'export') {
      navigate(`/export?connection=${encodedItem}`)
    }
  }

  return (
    <>
      <ViewBaseLayout>
        {connectionParam && (
          <div className="detailed-view-header">
            <h1>{`${connectionParam}`}</h1>
            <div className="detailed-view-header-buttons">
              <Button
                title="Imports"
                icon={<ImportIconSmall />}
                onClick={() => handleLinkClick('import', connectionParam)}
              />
              <Button
                title="Exports"
                icon={<ExportIconSmall />}
                onClick={() => handleLinkClick('export', connectionParam)}
              />
              <DropdownActions
                isDropdownActionsOpen={openDropdown === 'dropdownActions'}
                onToggle={(isDropdownActionsOpen: boolean) =>
                  handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
                }
                items={[
                  {
                    icon: <TestConnectionIcon />,
                    label: 'Test',
                    onClick: handleTestConnection
                  },
                  {
                    icon: <CopyIcon />,
                    label: 'Copy',
                    onClick: handleCopyClick
                  },
                  {
                    icon: <EncryptIcon />,
                    label: 'Encrypt credentials',
                    onClick: () => {
                      setIsEncryptModalOpen(true)
                      setOpenDropdown(null)
                    }
                  },
                  {
                    icon: <DeleteIcon />,
                    label: 'Delete',
                    onClick: handleDeleteClick
                  }
                ]}
                disabled={!connectionParam}
              />
            </div>
          </div>
        )}
        <ConnectionSettings />

        {isTestCnModalOpen && (
          <ConfirmationModal
            title={
              isTestLoading
                ? 'Testing'
                : `Test ${isTestSuccess ? 'successful' : 'failed'}`
            }
            message={
              isTestLoading
                ? ''
                : `Contact with connection ${connectionParam} ${
                    isTestSuccess ? 'verified' : 'failed'
                  }.`
            }
            isLoading={isTestLoading}
            errorInfo={isTestSuccess ? null : errorMessageTest}
            buttonTitleCancel="Close"
            onCancel={() => {
              setIsTestLoading(true)
              setIsTestCnModalOpen(false)
            }}
            isActive={isTestCnModalOpen}
          />
        )}
        {isCopyCnModalOpen && connectionParam && cnData && (
          <CopyConnectionModal
            connectionName={connectionParam}
            isCopyCnModalOpen={isCopyCnModalOpen}
            onSave={handleSaveCopy}
            onClose={() => setIsCopyCnModalOpen(false)}
          />
        )}
        {isEncryptModalOpen && connectionParam && (
          <EditTableModal
            isEditModalOpen={isEncryptModalOpen}
            title={`Encrypt credentials`}
            settings={settings}
            onSave={handleSave}
            onClose={() => setIsEncryptModalOpen(false)}
            isNoCloseOnSave={true}
            initWidth={400}
            isLoading={isEncryptLoading}
            loadingText="Encrypting"
            errorMessage={errorMessageEncrypt ? errorMessageEncrypt : null}
            onResetErrorMessage={() => setErrorMessageEncrypt(null)}
            submitButtonTitle="Encrypt"
          />
        )}
        {showDeleteConfirmation && connectionParam && (
          <DeleteModal
            title={`Delete ${connectionParam}`}
            message={`Are you sure that you want to delete connection "${connectionParam}"?`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            isLoading={isDeleteLoading}
            errorInfo={isDeleteSuccess ? null : errorMessageDelete}
            onConfirm={handleDeleteCn}
            onCancel={() => {
              setShowDeleteConfirmation(false)
              setErrorMessageDelete(null)
            }}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default ConnectionDetailedView
