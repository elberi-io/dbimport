import '../../_shared/tableDetailed/settings/TableSettings.scss'
import { useNavigate, useParams } from 'react-router-dom'
import '../../../components/Loading.scss'
import { importCardRenderSettings } from '../../../utils/cardRenderFormatting'
import { useImportTable } from '../../../utils/queries'
import Card from '../../_shared/tableDetailed/settings/Card'

function ImportTableSettings() {
  const navigate = useNavigate()

  const { database, table: tableParam } = useParams<{
    database: string
    table: string
  }>()
  const { data: table, isError, error } = useImportTable(database, tableParam)

  if (isError) {
    if (error.status === 404) {
      console.log(
        `GET table: ${error.message} ${error.response?.statusText}, re-routing to /import`
      )
      navigate(`/import`, { replace: true })
    }
    return <div className="error">Server error occurred.</div>
  }
  if (!table && !isError) return <div className="loading">Loading...</div>

  const importCards = importCardRenderSettings(table)

  return (
    <>
      <div className="block-container">
        <div className="block-container-2">
          <div className="cards">
            <div className="cards-container">
              <Card
                type="import"
                title="Main Settings"
                settings={importCards.mainSettings}
                tableData={table}
              />
              <Card
                type="import"
                title="Performance"
                settings={importCards.performance}
                tableData={table}
              />
              <Card
                type="import"
                title="Validation"
                settings={importCards.validation}
                tableData={table}
              />
              <Card
                type="import"
                title="Schedule"
                settings={importCards.schedule}
                tableData={table}
              />
              <Card
                type="import"
                title="Site-to-site Copy"
                settings={importCards.siteToSiteCopy}
                tableData={table}
              />
            </div>
            <div className="cards-container">
              <Card
                type="import"
                title="Import Options"
                settings={importCards.importOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="ETL Options"
                settings={importCards.etlOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="Incremental Imports"
                settings={importCards.incrementalImports}
                tableData={table}
                isNotEditable={table.importPhaseType !== 'incr'}
                isDisabled={table.importPhaseType !== 'incr'}
              />
            </div>
          </div>
          <div className="cards-narrow">
            <div className="cards-container">
              <Card
                type="import"
                title="Main Settings"
                settings={importCards.mainSettings}
                tableData={table}
              />
              <Card
                type="import"
                title="Import Options"
                settings={importCards.importOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="ETL Options"
                settings={importCards.etlOptions}
                tableData={table}
              />
              <Card
                type="import"
                title="Incremental Imports"
                settings={importCards.incrementalImports}
                tableData={table}
                isNotEditable={table.importPhaseType !== 'incr'}
                isDisabled={table.importPhaseType !== 'incr'}
              />
              <Card
                type="import"
                title="Performance"
                settings={importCards.performance}
                tableData={table}
              />
              <Card
                type="import"
                title="Validation"
                settings={importCards.validation}
                tableData={table}
              />
              <Card
                type="import"
                title="Schedule"
                settings={importCards.schedule}
                tableData={table}
              />
              <Card
                type="import"
                title="Site-to-site Copy"
                settings={importCards.siteToSiteCopy}
                tableData={table}
              />
            </div>
          </div>
        </div>
      </div>
    </>
  )
}

export default ImportTableSettings
