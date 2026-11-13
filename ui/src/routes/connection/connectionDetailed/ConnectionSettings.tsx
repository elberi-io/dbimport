import { useNavigate, useParams } from 'react-router-dom'
import '../../_shared/tableDetailed/DetailedView.scss'
import '../../_shared/tableDetailed/settings/TableSettings.scss'
import { useConnection } from '../../../utils/queries'
import { connectionCardRenderSettings } from '../../../utils/cardRenderFormatting'
import CardConnection from './CardConnection'
import '../../../components/Loading.scss'
import { useEffect } from 'react'

function ConnectionSettings() {
  const { connection: connectionParam } = useParams<{
    connection: string
  }>()

  const navigate = useNavigate()

  const { data: connection, isError, error } = useConnection(connectionParam)

  useEffect(() => {
    if (isError && error.status === 404) {
      console.log(
        `GET connection: ${error.message} ${error.response?.statusText}, re-routing to /connection`
      )
      navigate(`/connection`, { replace: true })
    }
  }, [isError, error, navigate])

  if (isError) {
    return <div className="error">Server error occurred.</div>
  }

  if (!connection && !isError) return <div className="loading">Loading...</div>

  const connectionSettings = connectionCardRenderSettings(connection)

  return (
    <div className="block-container" style={{ margin: 0 }}>
      <div className="block-container-2">
        <div>
          <div className="cards-container">
            <CardConnection
              title="Settings"
              settings={connectionSettings}
              originalData={connection}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export default ConnectionSettings
