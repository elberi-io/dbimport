import '../_shared/tableDetailed/DetailedView.scss'

import ConfigurationGlobal from './ConfigGlobal'
import ViewBaseLayout from '../../components/ViewBaseLayout'

function ConfigGlobalBase() {
  return (
    <>
      <ViewBaseLayout>
        <div className="detailed-view-header">
          <h1>Global Configuration</h1>
        </div>
        <ConfigurationGlobal />
      </ViewBaseLayout>
    </>
  )
}

export default ConfigGlobalBase
