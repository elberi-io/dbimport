import { Outlet } from 'react-router-dom'
import MainMenuSidebar from './MainMenuSidebar'
import './MainLayout.scss'
import { useAtom } from 'jotai'
import { isMainSidebarMinimized } from '../atoms/atoms'

function MainLayout() {
  const [minimized] = useAtom(isMainSidebarMinimized)

  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <MainMenuSidebar />
      <div className={`outlet-container ${minimized ? 'minimized' : ''}`}>
        <Outlet />
      </div>
    </div>
  )
}

export default MainLayout
