import { useEffect, useMemo, useRef, useState } from 'react'
import { useConnections } from '../../utils/queries'
import { useAtom } from 'jotai'
import { isCnDropdownReadyAtom } from '../../atoms/atoms'
import Dropdown from '../../components/Dropdown'
import ChevronDown from '../../assets/icons/ChevronDown'
import ChevronUp from '../../assets/icons/ChevronUp'
import FilterFunnel from '../../assets/icons/FilterFunnel'
import Button from '../../components/Button'
import {
  // ConnectionSearchFilter,
  EditSettingValueTypes,
  UiConnectionSearchFilter
} from '../../utils/interfaces'
import FavoriteFilterSearch from '../../components/FavoriteFilterSearch'
import { useFocusTrap } from '../../utils/hooks'
import { createTrimOnBlurHandler } from '../../utils/functions'
import '../../components/HeaderActions.scss'

interface ConnectionSearchFilterProps {
  isSearchFilterOpen: boolean
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (filters: UiConnectionSearchFilter) => void
  disabled?: boolean
}

function ConnectionSearchFilterCns({
  isSearchFilterOpen,
  onToggle,
  onShow,
  disabled
}: ConnectionSearchFilterProps) {
  const query = new URLSearchParams(location.search)

  const name = query.get('name') || null
  const connectionString = query.get('connectionString') || null
  const serverType = query.get('serverType') || null

  const [serverTypeFilter, setServerTypeFilter] = useState({
    label: 'Server Type',
    keyLabel: 'serverType',
    value: serverType,
    items: [
      'MySQL',
      'Oracle',
      'MSSQL Server', // motsvarar SQL Server
      'PostgreSQL',
      'Progress', // motsvarar Progress DB
      'DB2 UDB',
      'DB2 AS400',
      'MongoDB',
      'Cache', // motsvarar CacheDB
      'Snowflake',
      'AWS S3',
      'Informix',
      'SQL Anywhere'
    ]
  })

  const [isCnDropdownReady, setIsCnDropdownReady] = useAtom(
    isCnDropdownReadyAtom
  )

  const { data, isLoading } = useConnections()
  const connectionNames = useMemo(() => {
    return Array.isArray(data) ? data.map((connection) => connection.name) : []
  }, [data])

  const containerRef = useRef<HTMLDivElement>(null)
  const favoriteRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [formValues, setFormValues] = useState<UiConnectionSearchFilter>({
    name: name ? name : null,
    connectionString: connectionString ? connectionString : null,
    serverType: serverType ? serverType : null
  })

  useFocusTrap(
    containerRef,
    isSearchFilterOpen,
    openDropdown === 'addFavoriteDropdown'
  )

  useEffect(() => {
    setServerTypeFilter((prevFilter) => ({
      ...prevFilter,
      value: formValues.serverType
    }))
  }, [formValues])

  useEffect(() => {
    if (isLoading || !connectionNames.length) return
    setIsCnDropdownReady(true)
  }, [connectionNames.length, isLoading, setIsCnDropdownReady])

  useEffect(() => {
    if (!isSearchFilterOpen) return

    const containerRefCurrent = containerRef.current

    const handleClickOutside = (event: MouseEvent) => {
      const target = event.target as Node

      if (containerRefCurrent && !containerRefCurrent.contains(target)) {
        onToggle(false)
        setOpenDropdown(null)
        return
      }

      if (
        openDropdown &&
        (openDropdown === 'addFavoriteDropdown' ||
          openDropdown === 'favoritesDropdown') &&
        favoriteRef.current &&
        !favoriteRef.current.contains(target)
      ) {
        setOpenDropdown(null)
      }
    }

    const handleKeyUp = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (openDropdown) {
          setOpenDropdown(null)
        } else {
          onToggle(false)
        }
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keyup', handleKeyUp)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keyup', handleKeyUp)
    }
  }, [isSearchFilterOpen, onToggle, openDropdown])

  // const handleKeyDownOnInput = (
  //   event: React.KeyboardEvent<HTMLInputElement>
  // ) => {
  //   if (event.key === 'Enter') {
  //     buttonRef.current?.classList.add('active')
  //   }
  // }

  // const handleKeyUpOnInput = (event: React.KeyboardEvent<HTMLInputElement>) => {
  //   if (event.key === 'Enter') {
  //     buttonRef.current?.classList.remove('active')
  //   }
  // }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setTimeout(() => {
        setOpenDropdown(null)
      }, 0)
    }
  }

  const handleShow = () => {
    setOpenDropdown(null)
    onShow(formValues)
  }

  const handleInputDropdownChange = (value: string | null) => {
    setFormValues((prev) => ({ ...prev, name: value }))

    if (value && value.length > 0) {
      const matches = connectionNames.some((name) =>
        name.toLowerCase().startsWith(value.toLowerCase())
      )
      if (matches) {
        setOpenDropdown('cnSearch')
      } else {
        setOpenDropdown(null)
      }
    } else {
      setOpenDropdown(null)
    }
  }

  const handleInputDropdownSelect = (item: string | null) => {
    setFormValues((prev) => ({ ...prev, name: item }))
  }

  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string
  ) => {
    if (!keyLabel) return

    if (keyLabel === 'serverType') {
      const value = typeof item === 'string' ? item : null

      setServerTypeFilter((prev) => ({
        ...prev,
        value: value
      }))

      setFormValues((prev) => ({
        ...prev,
        serverType: item as string | null
      }))
    } else {
      setFormValues((prev) => ({
        ...prev,
        [keyLabel]: item
      }))
    }
  }

  const filteredConnectionNames = useMemo(() => {
    if (!formValues.name) return connectionNames
    return connectionNames.filter((name) =>
      name.toLowerCase().includes(formValues.name!.toLowerCase())
    )
  }, [formValues.name, connectionNames])

  const handleTrimOnBlur =
    createTrimOnBlurHandler<UiConnectionSearchFilter>(setFormValues)

  return (
    <div className="search-filter-container" ref={containerRef}>
      <button
        className="search-filter-button"
        onClick={() => onToggle(!isSearchFilterOpen)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') {
            onToggle(!isSearchFilterOpen)
          }
        }}
        tabIndex={0}
        disabled={disabled}
      >
        <div className="search-filter-funnel">
          <FilterFunnel />
        </div>
        Filter
        <div className="search-filter-chevron-container">
          {isSearchFilterOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </button>
      {isSearchFilterOpen && (
        <div className="search-filter-dropdown" style={{ width: '212px' }}>
          <div className="search-filter-dropdown-h-ctn">
            <h3>Search and show connections</h3>
            <FavoriteFilterSearch<UiConnectionSearchFilter>
              ref={favoriteRef}
              type="connection"
              formValues={formValues}
              onSelectFavorite={(favoriteState) => setFormValues(favoriteState)}
              openDropdown={openDropdown}
              handleDropdownToggle={handleDropdownToggle}
            />
          </div>

          <p>{`Use the asterisk (*) as a wildcard character for partial matches.`}</p>
          <form
            onSubmit={(event) => {
              event.preventDefault()
              handleShow()
            }}
          >
            <div className="filter-container">
              <div className="filter-first-container">
                <label htmlFor="searchFilterCnName">
                  Name:
                  <input
                    id="searchFilterCnName"
                    type="text"
                    value={formValues.name || ''}
                    onChange={(event) =>
                      handleInputDropdownChange(event.target.value)
                    }
                    onBlur={handleTrimOnBlur('name')}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"

                    // onKeyDown={handleKeyDownOnInput}
                    // onKeyUp={handleKeyUpOnInput}
                  />
                  {isCnDropdownReady && (
                    <Dropdown
                      id="searchFilterCnName"
                      items={
                        filteredConnectionNames.length > 0
                          ? filteredConnectionNames
                          : ['No Connection yet']
                      }
                      onSelect={handleInputDropdownSelect}
                      isOpen={openDropdown === 'cnSearch'}
                      onToggle={(isOpen: boolean) =>
                        handleDropdownToggle('cnSearch', isOpen)
                      }
                      searchFilter={false}
                      textInputMode={true}
                      backgroundColor="inherit"
                      textColor="black"
                      border="0.5px solid rgb(42, 42, 42)"
                      borderRadius="3px"
                      height="21.5px"
                      lightStyle={true}
                    />
                  )}
                </label>
                <label htmlFor="searchFilterCnString">
                  Connection string:
                  <input
                    id="searchFilterCnString"
                    type="text"
                    value={formValues.connectionString || ''}
                    onChange={(event) =>
                      setFormValues((prev) => ({
                        ...prev,
                        connectionString: event.target.value
                      }))
                    }
                    onBlur={handleTrimOnBlur('connectionString')}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"

                    // onKeyDown={handleKeyDownOnInput}
                    // onKeyUp={handleKeyUpOnInput}
                  />
                </label>
                <label
                  htmlFor={`dropdown-serverType`}
                  key={`dropdown-serverType`}
                >
                  {serverTypeFilter.label}:
                  <Dropdown
                    id={`dropdown-serverType`}
                    keyLabel={serverTypeFilter.keyLabel}
                    items={serverTypeFilter.items}
                    onSelect={handleSelect}
                    isOpen={openDropdown === `dropdown-serverType`}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle(`dropdown-serverType`, isOpen)
                    }
                    searchFilter={false}
                    initialTitle={
                      serverTypeFilter.value
                        ? String(serverTypeFilter.value)
                        : 'Select...'
                    }
                    cross={true}
                    backgroundColor="inherit"
                    textColor="black"
                    fontSize="14px"
                    border="0.5px solid rgb(42, 42, 42)"
                    borderRadius="3px"
                    height="21.5px"
                    width="212px"
                    chevronWidth="11"
                    chevronHeight="7"
                    lightStyle={true}
                  />
                </label>
              </div>
            </div>
            <div
              className="submit-button-container"
              style={{ marginTop: '30px' }}
            >
              <Button type="submit" title="Show" ref={buttonRef} />
            </div>
          </form>
        </div>
      )}
    </div>
  )
}

export default ConnectionSearchFilterCns
