import { AxiosError } from 'axios'
import { useState } from 'react'
import EditTableModal from './EditTableModal'
import { generateDagSettings } from '../../utils/cardRenderFormatting'
import { useGenerateDag } from '../../utils/mutations'
import { ErrorData } from '../../utils/interfaces'
import infoTexts from '../../infoTexts.json'

interface GenerateDagModalProps {
  dagName: string
  isGenDagModalOpen: boolean
  onClose: () => void
}

function GenerateDagModal({
  dagName,
  isGenDagModalOpen,
  onClose
}: GenerateDagModalProps) {
  const [isLoading, setIsLoading] = useState(false)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [successMessage, setSuccessMessage] = useState<string | null>(null)

  const { mutate: generateDag } = useGenerateDag()

  const settings = generateDagSettings(dagName ? dagName : '')

  const handleGenerateDag = () => {
    if (!dagName) {
      console.log('No selected row or DAG name')
      return
    }

    setIsLoading(true)

    generateDag(dagName, {
      onSuccess: () => {
        setIsLoading(false)
        setSuccessMessage('Generate DAG succeeded')
        console.log('Generating DAG succeeded')
      },
      onError: (error: AxiosError<ErrorData>) => {
        const errorMessage =
          error.response?.statusText &&
          error.response?.data.detail !== undefined
            ? `${error.message} ${error.response?.statusText}, ${error.response?.data.detail[0].msg}: ${error.response?.data.detail[0].type}`
            : error.status === 500
            ? `${error.message} ${error.response?.statusText}: ${error.response?.data}`
            : 'An unknown error occurred'
        setIsLoading(false)
        setErrorMessage(errorMessage)
      }
    })
  }

  return (
    <EditTableModal
      isEditModalOpen={isGenDagModalOpen}
      title={`Generate DAG`}
      titleInfoText={infoTexts.actions.generateDag}
      settings={settings}
      onSave={handleGenerateDag}
      onClose={() => {
        onClose()
        setErrorMessage(null)
        setSuccessMessage(null)
      }}
      isNoCloseOnSave={true}
      initWidth={300}
      isLoading={isLoading}
      loadingText="Generating"
      successMessage={successMessage}
      errorMessage={errorMessage ? errorMessage : null}
      onResetErrorMessage={() => setErrorMessage(null)}
      submitButtonTitle="Generate"
      closeButtonTitle="Close"
    />
  )
}

export default GenerateDagModal
