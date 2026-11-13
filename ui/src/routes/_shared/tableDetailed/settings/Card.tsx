import { useState } from 'react'
import './Card.scss'
import Button from '../../../../components/Button'
import EditTableModal from '../../../../components/modals/EditTableModal'
import Setting from './Setting'
import {
  EditSetting,
  UIExportTable,
  UIExportTableWithoutEnum,
  UITable,
  UITableWithoutEnum
} from '../../../../utils/interfaces'
import { useUpdateTable } from '../../../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import { useParams } from 'react-router-dom'
import {
  updateExportTableData,
  updateTableData
} from '../../../../utils/dataFunctions'

interface CardProps {
  type: 'import' | 'export'
  title: string
  settings: EditSetting[]
  tableData: UITable | UIExportTable
  isNotEditable?: boolean
  isDisabled?: boolean
}

function Card({
  type,
  title,
  settings,
  tableData,
  isNotEditable,
  isDisabled
}: CardProps) {
  const { table: tableParam, database, connection, targetTable } = useParams()
  const [isEditModalOpen, setIsEditModalOpen] = useState(false)
  const queryClient = useQueryClient()
  const { mutate: updateTable } = useUpdateTable()
  const handleOpenModal = () => setIsEditModalOpen(true)
  const handleCloseModal = () => setIsEditModalOpen(false)

  const handleSave = (updatedSettings: EditSetting[]) => {
    const tableDataCopy = { ...tableData }

    let editedTableData: UITableWithoutEnum | UIExportTableWithoutEnum | null =
      null
    if (type === 'import') {
      editedTableData = updateTableData(
        tableDataCopy as UITable,
        updatedSettings
      )
    } else if (type === 'export') {
      editedTableData = updateExportTableData(
        tableDataCopy as UIExportTable,
        updatedSettings
      )
    }

    if (editedTableData && type) {
      const secondQueryKey = type === 'import' ? database : connection
      const thirdQueryKey = type === 'import' ? tableParam : targetTable

      queryClient.setQueryData(
        [type, secondQueryKey, thirdQueryKey],
        editedTableData
      )
      updateTable(
        { type, table: editedTableData },
        {
          onSuccess: () => {
            // For getting fresh data from database to the cache
            queryClient.invalidateQueries({
              queryKey: [type, secondQueryKey, thirdQueryKey]
            })
            console.log('Update successful')
            setIsEditModalOpen(false)
          },
          onError: (error) => {
            queryClient.setQueryData(
              [type, secondQueryKey, thirdQueryKey],
              tableData
            )

            console.log('Error updating table', error.message)
          }
        }
      )
    }
  }

  const getInitWidth = () => {
    const widths: { [key: string]: number } = {
      'export:Main Settings': 650,
      'import:Main Settings': 650,
      'import:Import Options': 592,
      'import:ETL Options': 622,
      'import:Site-to-site Copy': 400
    }
    return widths[`${type}:${title}`] || 584
  }

  return (
    <div
      className={isDisabled ? 'card-disabled' : 'card'}
      style={title === 'Site-to-site Copy' ? { marginBottom: 90 } : {}}
    >
      <div className="card-head">
        <h3 className="card-h3">{title}</h3>
        {!isNotEditable && (
          <Button title="Edit" onClick={handleOpenModal} marginRight="12px" />
        )}
      </div>
      <dl className="card-dl">
        {Array.isArray(settings) &&
          settings.map((setting, index) => (
            <Setting key={index} {...setting} />
          ))}
      </dl>

      {isEditModalOpen && !isNotEditable && !isDisabled && (
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

export default Card
