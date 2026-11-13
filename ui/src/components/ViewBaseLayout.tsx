import './ViewBaseLayout.scss'
import { ReactNode } from 'react'
import Breadcrumbs from './Breadcrumbs'

interface ViewLayoutProps {
  children: ReactNode
}

function ViewBaseLayout({ children }: ViewLayoutProps) {
  return (
    <>
      <div className="view-layout-root">
        <Breadcrumbs />

        {children}
      </div>
    </>
  )
}

export default ViewBaseLayout
