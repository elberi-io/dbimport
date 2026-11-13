import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useMemo } from 'react'
import ChevronRight from '../assets/icons/ChevronRight'
import { useAtom } from 'jotai'
import {
  exportCnListFiltersAtom,
  importTableListFiltersAtom,
  isAirflowSubmenuActiveAtom,
  isDbDropdownReadyAtom,
  exportPersistStateAtom,
  importPersistStateAtom
} from '../atoms/atoms'
import './Breadcrumbs.scss'

const Breadcrumbs = () => {
  const location = useLocation()
  const navigate = useNavigate()
  const [, setImportPersistState] = useAtom(importPersistStateAtom)
  const [, setIsDbDropdownReady] = useAtom(isDbDropdownReadyAtom)
  const [, setSelectedImportFilters] = useAtom(importTableListFiltersAtom)

  const [, setExportPersistState] = useAtom(exportPersistStateAtom)
  const [, setSelectedExportFilters] = useAtom(exportCnListFiltersAtom)

  const [, setIsAirflowSubmenuActive] = useAtom(isAirflowSubmenuActiveAtom)

  const capitalizeFirstLetter = (string: string) => {
    return string.charAt(0).toUpperCase() + string.slice(1)
  }

  const crumbs = useMemo(() => {
    const pathnames = location.pathname
      .split('/')
      .filter((x) => x)
      .map((pathname) => decodeURIComponent(pathname))
    const breadcrumbItems = [{ label: 'Home', path: '/' }]

    pathnames.forEach((pathname, index) => {
      const path = `/${pathnames.slice(0, index + 1).join('/')}`
      const label =
        index === 0
          ? capitalizeFirstLetter(pathname)
          : pathname === 'jdbcdrivers'
          ? 'JDBC Drivers'
          : (pathnames[0] === 'airflow' || pathnames[0] === 'configuration') &&
            index === 1
          ? capitalizeFirstLetter(pathname)
          : pathname
      breadcrumbItems.push({
        label,
        path
      })
    })

    return breadcrumbItems
  }, [location])

  const handleBreadcrumbClick = (path: string) => {
    if (path === '/import') {
      setIsDbDropdownReady(false)
      setImportPersistState(null)
      setSelectedImportFilters({})
    }
    if (path === '/') {
      setIsAirflowSubmenuActive(false)
    }

    if (path === '/export') {
      setIsDbDropdownReady(false)
      setExportPersistState(null)
      setSelectedExportFilters({})
    }
    navigate(path)
  }

  return (
    <nav aria-label="breadcrumb">
      <ol className="breadcrumbs">
        {crumbs.map((crumb, idx) => (
          <li
            key={idx}
            className={`breadcrumb-item ${
              idx === crumbs.length - 1 ? 'active' : ''
            }`}
          >
            {/* Only shows chevron after the first item */}
            {idx > 0 && (
              <div>
                <ChevronRight />
              </div>
            )}
            {/* Current/last item is not a link */}
            {idx === crumbs.length - 1 ||
            idx >= 3 ||
            (crumb.path.startsWith('/import') && idx >= 2) ||
            (crumb.path.startsWith('/export') && idx >= 2) ? (
              <span>{crumb.label}</span>
            ) : crumb.label === 'Airflow' || crumb.label === 'Configuration' ? (
              <span>{crumb.label}</span>
            ) : (
              <Link
                to={crumb.path}
                onClick={(e) => {
                  e.preventDefault()
                  handleBreadcrumbClick(crumb.path)
                }}
              >
                {crumb.label}
              </Link>
            )}
          </li>
        ))}
      </ol>
    </nav>
  )
}

export default Breadcrumbs
