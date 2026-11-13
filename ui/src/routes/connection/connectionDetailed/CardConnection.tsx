import { useState } from 'react'
import '../../_shared/tableDetailed/settings/Card.scss'
import { useParams } from 'react-router-dom'
import { Connection, EditSetting } from '../../../utils/interfaces'
import Button from '../../../components/Button'
import Setting from '../../_shared/tableDetailed/settings/Setting'
import EditConnectionModal from '../../../components/modals/EditConnectionModal'
import { updateConnectionData } from '../../../utils/dataFunctions'
import { useQueryClient } from '@tanstack/react-query'
import { useCreateOrUpdateConnection } from '../../../utils/mutations'

interface CardProps {
  title: string
  settings: EditSetting[]
  originalData: Connection
  isNotEditable?: boolean
  isDisabled?: boolean
}
function CardConnection({
  title,
  settings,
  originalData,
  isNotEditable,
  isDisabled
}: CardProps) {
  const { connection: connectionParam } = useParams<{ connection: string }>()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const queryClient = useQueryClient()
  const { mutate: updateConnection } = useCreateOrUpdateConnection()

  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  const handleSave = (updatedSettings: EditSetting[]) => {
    const originalDataCopy: Connection = { ...originalData }
    const editedConnectionData = updateConnectionData(
      originalDataCopy,
      updatedSettings
    )

    queryClient.setQueryData(
      ['connection', connectionParam],
      editedConnectionData
    )
    updateConnection(editedConnectionData, {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: ['connection', connectionParam]
        }) // For getting fresh data from database to the cache
        console.log('Update successful')
        setIsEditModalOpen(false)
      },
      onError: (error) => {
        queryClient.setQueryData(['connection', connectionParam], originalData)

        console.log('Error updating connection', error.message)
      }
    })
  }

  return (
    <div className="card-connection" style={{ width: 1000 }}>
      <div className="card-head">
        <h3 className="card-h3">{title}</h3>
        <Button title="Edit" onClick={handleOpenModal} />
      </div>
      <dl className="card-dl">
        {Array.isArray(settings) &&
          settings.map((setting, index) => (
            <Setting
              key={index}
              {...setting}
              valueFieldWidth="739px"
              columnSetting={true}
            />
          ))}
      </dl>

      {isEditModalOpen && !isNotEditable && !isDisabled && (
        <EditConnectionModal
          isEditModalOpen={isEditModalOpen}
          title={`Edit ${connectionParam}`}
          settings={settings}
          onSave={handleSave}
          onClose={handleCloseModal}
        />
      )}
    </div>
  )
}

export default CardConnection
