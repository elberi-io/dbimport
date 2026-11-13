import '../_shared/tableDetailed/DetailedView.scss'

import ViewBaseLayout from '../../components/ViewBaseLayout'
import ConfigJDBCDrivers from './ConfigJDBCDrivers'

function ConfigJDBCDriversBase() {
  return (
    <>
      <ViewBaseLayout>
        <div className="detailed-view-header">
          <h1>JDBC Drivers</h1>
        </div>
        <ConfigJDBCDrivers />
      </ViewBaseLayout>
    </>
  )
}

export default ConfigJDBCDriversBase
