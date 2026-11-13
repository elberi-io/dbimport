import { useState } from 'react'
import '../_shared/tableDetailed/settings/Card.scss'
import Button from '../../components/Button'
import EditTableModal from '../../components/modals/EditTableModal'

import { EditSetting, ConfigGlobal } from '../../utils/interfaces'
import { useQueryClient } from '@tanstack/react-query'
import Setting from '../_shared/tableDetailed/settings/Setting'
import { updateGlobalConfigData } from '../../utils/dataFunctions'
import { useUpdateGlobalConfig } from '../../utils/mutations'

interface CardConfigProps {
  title: string
  settings: EditSetting[]
  originalData: ConfigGlobal
}

function CardConfig({ title, settings, originalData }: CardConfigProps) {
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)

  const queryClient = useQueryClient()
  const { mutate: updateGlobalConfig } = useUpdateGlobalConfig()

  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  const handleSave = (updatedSettings: EditSetting[]) => {
    const originalDataCopy: ConfigGlobal = { ...originalData }

    const editedGlobalConfigData = updateGlobalConfigData(
      originalDataCopy,
      updatedSettings
    )

    updateGlobalConfig(editedGlobalConfigData, {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: ['configuration', 'global']
        }) // For getting fresh data from database to the cache
        console.log('Update successful')
        setIsEditModalOpen(false)
      },
      onError: (error) => {
        console.log('Error updating configuration', error.message)
      }
    })
  }

  const getInitWidth = () => {
    const widths: { [key: string]: number } = {
      'Disable Operations Settings': 410,
      'Misc Settings': 515,
      Performance: 370
    }
    return widths[`${title}`] || 584
  }

  return (
    <div
      className="card-config"
      style={title === 'Performance' ? { marginBottom: 240 } : {}}
    >
      <div className="card-head">
        <h3 className="card-h3">{title}</h3>
        <Button title="Edit" onClick={handleOpenModal} />
      </div>
      <dl className="card-dl">
        {Array.isArray(settings) &&
          settings.map((setting, index) => (
            <Setting key={index} {...setting} />
          ))}
      </dl>

      {isEditModalOpen && (
        <EditTableModal
          isEditModalOpen={isEditModalOpen}
          title={`Edit ${title}`}
          settings={settings}
          onSave={handleSave}
          onClose={handleCloseModal}
          initWidth={getInitWidth()}
        />
      )}
    </div>
  )
}

export default CardConfig
