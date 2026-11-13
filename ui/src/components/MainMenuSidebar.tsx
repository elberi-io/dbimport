import ChevronDoubleLeft from '../assets/icons/ChevronDoubleLeft'
import ImportIcon from '../assets/icons/ImportIcon'
import ExportIcon from '../assets/icons/ExportIcon'
import ChevronDoubleRight from '../assets/icons/ChevronDoubleRight'
import { NavLink, useNavigate } from 'react-router-dom'
import ApacheAirflowIcon from '../assets/icons/ApacheAirflowIcon'
import ConnectionIcon from '../assets/icons/ConnectionIcon'
import ConfigurationIcon from '../assets/icons/ConfigurationIcon'
// import DBImportIconTextLogo from '../assets/icons/DBImportIconTextLogo' // For using the svgs instead of programmed text logo
// import DBImportIconLogo from '../assets/icons/DBImportIconLogo' // For using the svgs instead of programmed text logo
import './MainMenuSidebar.scss'
import LogoWithText from './LogoWithText'
import { useAtom } from 'jotai'
import {
  importPersistStateAtom,
  isAirflowSubmenuActiveAtom,
  isConfigurationSubmenuActiveAtom,
  exportPersistStateAtom,
  usernameAtom,
  isMainSidebarMinimized
} from '../atoms/atoms'
import { useState } from 'react'
import AirflowImportIcon from '../assets/icons/AirflowImportIcon'
import AirflowExportIcon from '../assets/icons/AirflowExportIcon'
import AirflowCustomIcon from '../assets/icons/AirflowCustomIcon'
import UserIcon from '../assets/icons/UserIcon'
import LogoutIcon from '../assets/icons/LogoutIcon'
import { useGetStatusAndVersion } from '../utils/queries'
import { deleteCookie } from '../utils/cookies'
import { clearSessionStorageAtoms } from '../atoms/utils'
import ConfigurationJDBCDriversIcon from '../assets/icons/ConfigurationJDBCDriversIcon'
import ConfigurationGlobalIcon from '../assets/icons/ConfigurationGlobalIcon'

function MainMenuSidebar() {
  const [minimized, setMinimized] = useAtom(isMainSidebarMinimized)

  const { data: statusData } = useGetStatusAndVersion()
  const navigate = useNavigate()
  const [importPersistState] = useAtom(importPersistStateAtom)
  const [exportPersistState] = useAtom(exportPersistStateAtom)

  const [toggleAirflowActive, setToggleAirflowActive] = useState(false)
  const [toggleConfigurationActive, setToggleConfigurationActive] =
    useState(false)
  const [toggleUsernameMenu, setToggleUsernameMenu] = useState(false)
  const [isAirflowSubmenuActive, setIsAirflowSubmenuActive] = useAtom(
    isAirflowSubmenuActiveAtom
  )
  const [isConfigurationSubmenuActive, setIsConfigurationSubmenuActive] =
    useAtom(isConfigurationSubmenuActiveAtom)
  const [userName] = useAtom(usernameAtom)

  const handleToggleAirflowMenu = () => {
    setToggleAirflowActive((prev) => !prev)
  }

  const handleToggleConfigurationMenu = () => {
    setToggleConfigurationActive((prev) => !prev)
  }

  const handleToggleMinimize = () => {
    setMinimized((prevMinimized) => !prevMinimized)
  }

  const handleToggleUsenameMenu = () => {
    setToggleUsernameMenu((prev) => !prev)
  }

  const handleLogout = () => {
    deleteCookie('DBI_auth_token')
    navigate('/login')
    clearSessionStorageAtoms()
  }
  return (
    <>
      <div className={`mainsidebar-root ${minimized ? 'minimized' : ''}`}>
        <div className="mainsidebar-logo-container">
          <NavLink
            to="/"
            className="mainsidebar-logo-link"
            onClick={() => {
              setIsAirflowSubmenuActive(false)
            }}
          >
            <LogoWithText size="small" noText={minimized ? true : false} />
            {/* {minimized ? (
            <DBImportIconLogo />
          ) : (
            <>
              <DBImportIconTextLogo size="small" />
            </>
          )} */}
          </NavLink>
        </div>
        {/* <p style={{ fontSize: '5.5px', color: ' white', marginLeft: 88 }}> // To compare sharpness to logo svg text
          Powered by
        </p> */}

        <div className="menu-options">
          <ul>
            <li className="mainsidebar-menu-li">
              <NavLink
                to={importPersistState ? `${importPersistState}` : '/import'}
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
                onClick={() => {
                  setIsAirflowSubmenuActive(false)
                  setIsConfigurationSubmenuActive(false)
                }}
              >
                <ImportIcon />
                {!minimized && <h2>Import</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-menu-li">
              <NavLink
                to={exportPersistState ? `${exportPersistState}` : '/export'}
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
                onClick={() => {
                  setIsAirflowSubmenuActive(false)
                  setIsConfigurationSubmenuActive(false)
                }}
              >
                <ExportIcon />
                {!minimized && <h2>Export</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-menu-li">
              <div
                className={`mainsidebar-menu-link ${
                  isAirflowSubmenuActive ? 'active' : ''
                }`}
                onClick={handleToggleAirflowMenu}
              >
                <ApacheAirflowIcon />
                {!minimized && <h2>Airflow</h2>}
              </div>
              {toggleAirflowActive && (
                <ul>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/airflow/import"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowSubmenuActive(true)
                        setIsConfigurationSubmenuActive(false)
                      }}
                    >
                      <AirflowImportIcon />
                      {!minimized && <h3>Import</h3>}
                    </NavLink>
                  </li>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/airflow/export"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowSubmenuActive(true)
                        setIsConfigurationSubmenuActive(false)
                      }}
                    >
                      <AirflowExportIcon />

                      {!minimized && <h3>Export</h3>}
                    </NavLink>
                  </li>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/airflow/custom"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowSubmenuActive(true)
                        setIsConfigurationSubmenuActive(false)
                      }}
                    >
                      <AirflowCustomIcon />

                      {!minimized && <h3>Custom</h3>}
                    </NavLink>
                  </li>
                </ul>
              )}
            </li>
            <li className="mainsidebar-menu-li">
              <NavLink
                to="/connection"
                className={({ isActive }) =>
                  `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                }
                onClick={() => {
                  setIsAirflowSubmenuActive(false)
                  setIsConfigurationSubmenuActive(false)
                }}
              >
                <ConnectionIcon />
                {!minimized && <h2>Connection</h2>}
              </NavLink>
            </li>
            <li className="mainsidebar-menu-li">
              <div
                className={`mainsidebar-menu-link ${
                  isConfigurationSubmenuActive ? 'active' : ''
                }`}
                onClick={handleToggleConfigurationMenu}
              >
                <ConfigurationIcon />
                {!minimized && <h2>Configuration</h2>}
              </div>
              {toggleConfigurationActive && (
                <ul>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/configuration/global"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowSubmenuActive(false)
                        setIsConfigurationSubmenuActive(true)
                      }}
                    >
                      <ConfigurationGlobalIcon />
                      {!minimized && <h3>Global</h3>}
                    </NavLink>
                  </li>
                  <li className="airflow-submenu-option">
                    <NavLink
                      to="/configuration/jdbcdrivers"
                      className={({ isActive }) =>
                        `mainsidebar-menu-link ${isActive ? 'active' : ''}`
                      }
                      onClick={() => {
                        setIsAirflowSubmenuActive(false)
                        setIsConfigurationSubmenuActive(true)
                      }}
                    >
                      <ConfigurationJDBCDriversIcon />

                      {!minimized && <h3>JDBC Drivers</h3>}
                    </NavLink>
                  </li>
                </ul>
              )}
            </li>
            <li className="mainsidebar-usermenu-li">
              {toggleUsernameMenu && (
                <ul>
                  <li
                    className="mainsidebar-usermenu mainsidebar-usermenu-logout"
                    onClick={handleLogout}
                  >
                    <LogoutIcon />
                    {!minimized && <h3>Logout</h3>}
                  </li>
                </ul>
              )}
              <div
                className="mainsidebar-usermenu mainsidebar-usermenu-username"
                onClick={handleToggleUsenameMenu}
              >
                <UserIcon />
                {!minimized && <h2>{userName ? userName : null}</h2>}
              </div>
            </li>
          </ul>
        </div>

        <div
          className="chevron-double"
          style={minimized ? { marginBottom: '26px' } : {}}
        >
          <button onClick={handleToggleMinimize}>
            {minimized ? <ChevronDoubleRight /> : <ChevronDoubleLeft />}
          </button>
        </div>
        {!minimized && statusData && (
          <p className="dbimport-version">V{statusData.version}</p>
        )}
      </div>
    </>
  )
}

export default MainMenuSidebar
