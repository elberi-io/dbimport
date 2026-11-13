import { useEffect, useMemo, useRef, useState } from 'react'
import { useAtom } from 'jotai'
import ChevronDown from '../../assets/icons/ChevronDown'
import ChevronUp from '../../assets/icons/ChevronUp'
import FilterFunnel from '../../assets/icons/FilterFunnel'
import { isDbDropdownReadyAtom, isCnDropdownReadyAtom } from '../../atoms/atoms'
import Button from '../../components/Button'
import Dropdown from '../../components/Dropdown'
import FavoriteFilterSearch from '../../components/FavoriteFilterSearch'
import { initEnumDropdownFilters } from '../../utils/cardRenderFormatting'
import { createTrimOnBlurHandler } from '../../utils/functions'
import { useFocusTrap } from '../../utils/hooks'
import {
  UiImportSearchFilter,
  EditSetting,
  EditSettingValueTypes
} from '../../utils/interfaces'
import { getKeyFromLabel } from '../../utils/nameMappings'
import { useDatabases, useConnections } from '../../utils/queries'
import '../../components/HeaderActions.scss'

interface ImportSearchFilterProps {
  isSearchFilterOpen: boolean
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (filters: UiImportSearchFilter) => void
  disabled?: boolean
}

function ImportSearchFilterTables({
  isSearchFilterOpen,
  onToggle,
  onShow,
  disabled
}: ImportSearchFilterProps) {
  const query = new URLSearchParams(location.search)
  const connection = query.get('connection') || null
  const database = query.get('database') || null
  const table = query.get('table') || null
  const includeInAirflow = query.get('includeInAirflow') || null
  const lastUpdateFromSource = query.get('lastUpdateFromSource') || null
  const importType = query.get('importType') || null
  const importTool = query.get('importTool') || null
  const etlType = query.get('etlType') || null
  const etlEngine = query.get('etlEngine') || null

  const [enumDropdownFilters, setEnumDropdownFilters] = useState<EditSetting[]>(
    initEnumDropdownFilters(importType, importTool, etlType, etlEngine)
  )

  const [includeInAirflowFilter, setIncludeInAirflowFilter] = useState({
    label: 'Include in Airflow',
    keyLabel: 'includeInAirflow',
    value: includeInAirflow,
    items: ['True', 'False']
  })

  const [lastUpdateFromSourceFilter, setLastUpdateFromSourceFilter] = useState({
    label: 'Last update from source',
    keyLabel: 'lastUpdateFromSource',
    value: lastUpdateFromSource,
    items: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  })

  const [isDbDropdownReady, setIsDbDropdownReady] = useAtom(
    isDbDropdownReadyAtom
  )

  const [isCnDropdownReady, setIsCnDropdownReady] = useAtom(
    isCnDropdownReadyAtom
  )

  const { data, isLoading } = useDatabases()
  const databaseNames = useMemo(() => {
    return Array.isArray(data) ? data.map((database) => database.name) : []
  }, [data])

  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData?.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const containerRef = useRef<HTMLDivElement>(null)
  const favoriteRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [formValues, setFormValues] = useState<UiImportSearchFilter>({
    connection: connection ? connection : null,
    database: database ? database : null,
    table: table ? table : null,
    includeInAirflow: includeInAirflow ? includeInAirflow : null,
    lastUpdateFromSource: lastUpdateFromSource ? lastUpdateFromSource : null,
    importPhaseType: importType ? importType : null,
    importTool: importTool ? importTool : null,
    etlPhaseType: etlType ? etlType : null,
    etlEngine: etlEngine ? etlEngine : null
  })

  useFocusTrap(
    containerRef,
    isSearchFilterOpen,
    openDropdown === 'addFavoriteDropdown'
  )

  useEffect(() => {
    setEnumDropdownFilters(
      initEnumDropdownFilters(
        formValues.importPhaseType,
        formValues.importTool,
        formValues.etlPhaseType,
        formValues.etlEngine
      )
    )

    setIncludeInAirflowFilter((prevFilter) => ({
      ...prevFilter,
      value: formValues.includeInAirflow
    }))

    setLastUpdateFromSourceFilter((prevFilter) => ({
      ...prevFilter,
      value: formValues.lastUpdateFromSource
    }))
  }, [formValues])

  useEffect(() => {
    if (isLoading || !databaseNames.length || !connectionNames.length) return
    setIsCnDropdownReady(true)
    setIsDbDropdownReady(true)
  }, [
    connectionNames.length,
    databaseNames.length,
    isLoading,
    setIsCnDropdownReady,
    setIsDbDropdownReady
  ])

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

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (openDropdown) {
          setOpenDropdown(null)
        } else {
          onToggle(false)
        }
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
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

  const handleInputDropdownChange = (
    key: 'database' | 'connection',
    value: string | null
  ) => {
    setFormValues((prev) => ({ ...prev, [key]: value }))

    if (value && value.length > 0) {
      const matches =
        key === 'database'
          ? databaseNames.some((name) =>
              name.toLowerCase().startsWith(value.toLowerCase())
            )
          : connectionNames.some((name) =>
              name.toLowerCase().startsWith(value.toLowerCase())
            )

      if (matches) {
        setOpenDropdown(key === 'database' ? 'dbSearch' : 'cnSearch')
      } else {
        setOpenDropdown(null)
      }
    } else {
      setOpenDropdown(null)
    }
  }

  const handleInputDropdownSelect = (
    item: string | null,
    keyLabel: string | undefined
  ) => {
    if (!keyLabel) return
    setFormValues((prev) => ({ ...prev, [keyLabel]: item }))
  }

  const filteredDatabaseNames = useMemo(() => {
    if (!formValues.database) return databaseNames
    return databaseNames.filter((name) =>
      name.toLowerCase().includes(formValues.database!.toLowerCase())
    )
  }, [formValues.database, databaseNames])

  const filteredConnectionNames = useMemo(() => {
    if (!formValues.connection) return connectionNames
    return connectionNames.filter((name) =>
      name.toLowerCase().includes(formValues.connection!.toLowerCase())
    )
  }, [formValues.connection, connectionNames])

  const handleSelect = (
    item: EditSettingValueTypes | null,
    keyLabel?: string,
    settingType?: string
  ) => {
    if (!keyLabel) return

    if (keyLabel === 'includeInAirflow') {
      const value = typeof item === 'string' ? item : null

      setIncludeInAirflowFilter((prev) => ({
        ...prev,
        value: value
      }))

      setFormValues((prev) => ({
        ...prev,
        includeInAirflow: item as string | null
      }))
    } else if (keyLabel === 'lastUpdateFromSource') {
      const value = typeof item === 'string' ? item : null

      setLastUpdateFromSourceFilter((prev) => ({
        ...prev,
        value: value
      }))

      setFormValues((prev) => ({
        ...prev,
        lastUpdateFromSource: item as string | null
      }))
    } else if (settingType === 'enum') {
      const updatedFilters = enumDropdownFilters.map((filter) =>
        filter.label === keyLabel ? { ...filter, value: item } : filter
      )
      setEnumDropdownFilters(updatedFilters)

      const key = getKeyFromLabel(keyLabel)

      setFormValues((prev) => ({
        ...prev,
        [key as string]: item
      }))
    } else {
      setFormValues((prev) => ({
        ...prev,
        [keyLabel]: item
      }))
    }
  }

  const handleTrimOnBlur =
    createTrimOnBlurHandler<UiImportSearchFilter>(setFormValues)

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
        <div className="search-filter-dropdown">
          <div className="search-filter-dropdown-h-ctn">
            <h3>Search and show tables</h3>
            <FavoriteFilterSearch<UiImportSearchFilter>
              ref={favoriteRef}
              type="import"
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
                <label
                  htmlFor="searchFilterConnection"
                  className="filter-text-dropdown"
                >
                  Connection:
                  <input
                    id="searchFilterConnection"
                    type="text"
                    value={formValues.connection || ''}
                    onChange={(event) =>
                      handleInputDropdownChange(
                        'connection',
                        event.target.value
                      )
                    }
                    onBlur={handleTrimOnBlur('connection')}
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
                      id="searchFilterConnection"
                      keyLabel="connection"
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
                <label
                  htmlFor="searchFilterDatabase"
                  className="filter-text-dropdown"
                >
                  Database:
                  <input
                    id="searchFilterDatabase"
                    type="text"
                    value={formValues.database || ''}
                    onChange={(event) =>
                      handleInputDropdownChange('database', event.target.value)
                    }
                    onBlur={handleTrimOnBlur('database')}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"
                    // onKeyDown={handleKeyDownOnInput}
                    // onKeyUp={handleKeyUpOnInput}
                  />
                  {isDbDropdownReady && (
                    <Dropdown
                      id="searchFilterDatabase"
                      keyLabel="database"
                      items={
                        filteredDatabaseNames.length > 0
                          ? filteredDatabaseNames
                          : ['No DB yet']
                      }
                      onSelect={handleInputDropdownSelect}
                      isOpen={openDropdown === 'dbSearch'}
                      onToggle={(isOpen: boolean) =>
                        handleDropdownToggle('dbSearch', isOpen)
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
                <label htmlFor="searchFilterTable">
                  Table:
                  <input
                    id="searchFilterTable"
                    type="text"
                    value={formValues.table || ''}
                    onChange={(event) =>
                      setFormValues((prev) => ({
                        ...prev,
                        table: event.target.value
                      }))
                    }
                    onBlur={handleTrimOnBlur('table')}
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
                  htmlFor={`dropdown-lastUpdateFromSource`}
                  key={`dropdown-lastUpdateFromSource`}
                >
                  {lastUpdateFromSourceFilter.label}:
                  <Dropdown
                    id={`dropdown-lastUpdateFromSource`}
                    keyLabel={lastUpdateFromSourceFilter.keyLabel}
                    items={lastUpdateFromSourceFilter.items}
                    onSelect={handleSelect}
                    isOpen={openDropdown === `dropdown-lastUpdateFromSource`}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle(
                        `dropdown-lastUpdateFromSource`,
                        isOpen
                      )
                    }
                    searchFilter={false}
                    initialTitle={
                      lastUpdateFromSourceFilter.value
                        ? String(lastUpdateFromSourceFilter.value)
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
                <label
                  htmlFor={`dropdown-includeInAirflow`}
                  key={`dropdown-includeInAirflow`}
                >
                  {includeInAirflowFilter.label}:
                  <Dropdown
                    id={`dropdown-includeInAirflow`}
                    keyLabel={includeInAirflowFilter.keyLabel}
                    items={includeInAirflowFilter.items}
                    onSelect={handleSelect}
                    isOpen={openDropdown === `dropdown-includeInAirflow`}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle(`dropdown-includeInAirflow`, isOpen)
                    }
                    searchFilter={false}
                    initialTitle={
                      includeInAirflowFilter.value
                        ? String(includeInAirflowFilter.value)
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
              <div className="filter-second-container">
                {enumDropdownFilters.map((filter, index) => {
                  const dropdownOptions = filter.enumOptions
                    ? Object.values(filter.enumOptions)
                    : []
                  return (
                    <label
                      htmlFor={`dropdown-${index}`}
                      key={`dropdown-${index}`}
                    >
                      {filter.label}:
                      <Dropdown
                        id={`dropdown-${index}`}
                        keyLabel={filter.label}
                        settingType={filter.type}
                        items={dropdownOptions}
                        onSelect={handleSelect}
                        isOpen={openDropdown === `dropdown-${index}`}
                        onToggle={(isOpen: boolean) =>
                          handleDropdownToggle(`dropdown-${index}`, isOpen)
                        }
                        searchFilter={false}
                        initialTitle={
                          filter.value ? String(filter.value) : 'Select...'
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
                  )
                })}
                <div className="submit-button-container">
                  <Button type="submit" title="Show" ref={buttonRef} />
                </div>
              </div>
            </div>
            {/* <div className="submit-button-container">
              <Button type="submit" title="Show" ref={buttonRef} />
            </div> */}
          </form>
        </div>
      )}
    </div>
  )
}

export default ImportSearchFilterTables
