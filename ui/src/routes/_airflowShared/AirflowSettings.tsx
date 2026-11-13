import { useNavigate, useParams } from 'react-router-dom'
import { useAirflowDAG } from '../../utils/queries'
import '../_shared/tableDetailed/DetailedView.scss'
import CardAirflow from './CardAirflow'
import { airflowCardRenderSettings } from '../../utils/cardRenderFormatting'
import '../../components/Loading.scss'
import { useEffect } from 'react'

function AirflowSettings({ type }: { type: 'import' | 'export' | 'custom' }) {
  const { dagName } = useParams<{
    dagName: string
  }>()

  const navigate = useNavigate()

  const { data: dagData, isError, error } = useAirflowDAG(type, dagName)

  useEffect(() => {
    if (isError && error.status === 404) {
      console.log(
        `GET airflow: ${error.message} ${error.response?.statusText}, re-routing to /airflow/${type}`
      )
      navigate(`/airflow/${type}`, { replace: true })
    }
  }, [isError, error, navigate, type])

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }
  if (!dagName && !dagData && !isError)
    return <div className="loading">Loading...</div>

  if (!dagData) {
    return
  }

  const airflowDagSettings = airflowCardRenderSettings(type, dagData)

  if (!airflowDagSettings) {
    return
  }

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div>
            <div className="cards-container">
              <CardAirflow
                type={type}
                title="Settings"
                settings={airflowDagSettings}
                originalData={dagData}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default AirflowSettings
