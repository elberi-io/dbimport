import { Outlet, useLocation, useNavigate, useParams } from 'react-router-dom'
import ViewBaseLayout from '../../components/ViewBaseLayout'
import { useEffect, useMemo, useState } from 'react'
import '../_shared/tableDetailed/DetailedView.scss'
import GenerateDAGIcon from '../../assets/icons/GenerateDAGIcon'
import DropdownActions from '../../components/DropdownActions'
import GenerateDagModal from '../../components/modals/GenerateDagModal'
import { useAirflowDAG } from '../../utils/queries'
import Button from '../../components/Button'
import ApacheAirflowIconSmall from '../../assets/icons/ApacheAirflowIconSmall'
import { useQueryClient } from '@tanstack/react-query'
import { useCreateAirflowDag, useDeleteAirflowDAG } from '../../utils/mutations'
import DeleteIcon from '../../assets/icons/DeleteIcon'
import RenameIcon from '../../assets/icons/RenameIcon'
import RenameAirflowModal from '../../components/modals/RenameAirflowModal'
import { newCopyDagData } from '../../utils/dataFunctions'
import DeleteModal from '../../components/modals/DeleteModal'

function AirflowDetailedView({
  type
}: {
  type: 'import' | 'export' | 'custom'
}) {
  const { dagName } = useParams()
  const navigate = useNavigate()
  const location = useLocation()
  const queryClient = useQueryClient()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const [isGenDagModalOpen, setIsGenDagModalOpen] = useState(false)

  const [isRenameAirflowDagModalOpen, setIsRenameAirflowDagModalOpen] =
    useState(false)

  const { mutate: createDAG } = useCreateAirflowDag()

  const { mutate: deleteDag } = useDeleteAirflowDAG()
  const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false)

  const encodedType = encodeURIComponent(type)
  const encodedDagName = encodeURIComponent(dagName ? dagName : '')

  // const pathSegments = location.pathname.split('/').filter(Boolean)
  const pathSegments =
    typeof location.pathname === 'string'
      ? location.pathname.split('/').filter(Boolean)
      : []

  const tab = pathSegments[3] || 'settings'
  const validTabs = useMemo(() => ['settings', 'tasks'], [])
  const selectedTab = validTabs.includes(tab) ? tab : 'settings'

  useEffect(() => {
    if (!validTabs.includes(tab)) {
      navigate(`/airflow/${encodedType}/${encodedDagName}/settings`, {
        replace: true
      })
    }
  }, [tab, navigate, validTabs, dagName, type, encodedType, encodedDagName])

  const { data: dagData } = useAirflowDAG(type, dagName)

  const handleTabClick = (tabName: string) => {
    navigate(`/airflow/${encodedType}/${encodedDagName}/${tabName}`)
  }

  const handleGenerateDagClick = () => {
    setIsGenDagModalOpen(true)
  }

  const handleLinkClick = () => {
    if (!dagData) {
      return
    }
    if (dagData.airflowLink) {
      window.open(dagData.airflowLink, '_blank')
    } else {
      console.error('No Airlow link on DAG.')
    }
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  const handleRenameDagClick = () => {
    setIsRenameAirflowDagModalOpen(true)
  }

  const handleSaveRename = (newDagName: string) => {
    if (!dagName || !dagData) return

    const newDagDataCopy = newCopyDagData(newDagName, dagData)

    createDAG(
      { type, dagData: newDagDataCopy },
      {
        onSuccess: () => {
          console.log('Save renamned copy successful')

          deleteDag(
            { type, dagName },
            {
              onSuccess: () => {
                queryClient.invalidateQueries({
                  queryKey: ['airflows', type],
                  exact: true
                })

                queryClient.removeQueries({
                  queryKey: ['airflows', type, dagName],
                  exact: true
                })

                console.log('Delete original successful, rename successful')

                const encodedNewDagName = encodeURIComponent(
                  newDagName ? newDagName : ''
                )

                setIsRenameAirflowDagModalOpen(false)

                navigate(
                  `/airflow/${type}/${encodedNewDagName}/${selectedTab}`,
                  {
                    replace: true
                  }
                )
              },
              onError: (error) => {
                console.log('Error deleting DAG:', error.message)
              }
            }
          )
        },
        onError: (error) => {
          console.log('Error creating rename:', error.message)
        }
      }
    )
  }

  const handleDeleteDagClick = () => {
    if (!dagName) return
    setShowDeleteConfirmation(true)
  }

  const handleDeleteDag = async () => {
    if (!dagName) return

    setShowDeleteConfirmation(false)

    deleteDag(
      { type, dagName },
      {
        onSuccess: () => {
          queryClient.invalidateQueries({
            queryKey: ['airflows', type],
            exact: true
          })
          queryClient.removeQueries({
            queryKey: ['airflows', type, dagName],
            exact: true
          })
          console.log('Delete successful')
          navigate(`/airflow/${type}`, { replace: true })
        },
        onError: (error) => {
          console.log('Error deleting DAG:', error.message)
        }
      }
    )
  }

  return (
    <>
      <ViewBaseLayout>
        <div className="detailed-view-header">
          <h1>{`${dagName}`}</h1>
          <div className="detailed-view-header-buttons">
            <Button
              title="Airflow"
              icon={<ApacheAirflowIconSmall />}
              onClick={handleLinkClick}
            />
            <div className="actions-dropdown-generated-br">
              <DropdownActions
                isDropdownActionsOpen={openDropdown === 'dropdownActions'}
                maxWidth={107}
                onToggle={(isDropdownActionsOpen: boolean) =>
                  handleDropdownToggle('dropdownActions', isDropdownActionsOpen)
                }
                items={[
                  {
                    icon: <GenerateDAGIcon />,
                    label: 'Generate',
                    onClick: handleGenerateDagClick
                  },
                  {
                    icon: <RenameIcon />,
                    label: 'Rename',
                    onClick: handleRenameDagClick
                  },
                  {
                    icon: <DeleteIcon />,
                    label: `Delete`,
                    onClick: handleDeleteDagClick
                  }
                ]}
              />
            </div>
          </div>
        </div>

        <div className="tabs">
          <h2
            className={selectedTab === 'settings' ? 'active-tab' : ''}
            onClick={() => handleTabClick('settings')}
          >
            Settings
          </h2>
          <h2
            className={selectedTab === 'tasks' ? 'active-tab' : ''}
            onClick={() => handleTabClick('tasks')}
          >
            Tasks
          </h2>
        </div>
        <Outlet />
        {isGenDagModalOpen && dagName && (
          <GenerateDagModal
            dagName={dagName}
            isGenDagModalOpen={isGenDagModalOpen}
            onClose={() => setIsGenDagModalOpen(false)}
          />
        )}
        {isRenameAirflowDagModalOpen && dagName && dagData && (
          <RenameAirflowModal
            type={type}
            dagName={dagName}
            isRenameAirflowModalOpen={isRenameAirflowDagModalOpen}
            onSave={handleSaveRename}
            onClose={() => setIsRenameAirflowDagModalOpen(false)}
          />
        )}
        {showDeleteConfirmation && (
          <DeleteModal
            title={`Delete ${dagName}`}
            message={`Are you sure that you want to delete DAG "${dagName}"?`}
            buttonTitleCancel="No, Go Back"
            buttonTitleConfirm="Yes, Delete"
            onConfirm={handleDeleteDag}
            onCancel={() => setShowDeleteConfirmation(false)}
            isActive={showDeleteConfirmation}
          />
        )}
      </ViewBaseLayout>
    </>
  )
}

export default AirflowDetailedView
