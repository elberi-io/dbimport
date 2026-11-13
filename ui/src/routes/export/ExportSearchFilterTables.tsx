import { useAtom } from 'jotai'
import { useState, useMemo, useRef, useEffect } from 'react'
import ChevronDown from '../../assets/icons/ChevronDown'
import ChevronUp from '../../assets/icons/ChevronUp'
import FilterFunnel from '../../assets/icons/FilterFunnel'
import { isCnDropdownReadyAtom } from '../../atoms/atoms'
import Button from '../../components/Button'
import Dropdown from '../../components/Dropdown'
import FavoriteFilterSearch from '../../components/FavoriteFilterSearch'
import { initExportEnumDropdownFilters } from '../../utils/cardRenderFormatting'
import { createTrimOnBlurHandler } from '../../utils/functions'
import { useFocusTrap } from '../../utils/hooks'
import {
  UiExportSearchFilter,
  EditSetting,
  EditSettingValueTypes
} from '../../utils/interfaces'
import { getKeyFromExportLabel } from '../../utils/nameMappings'
import { useExportConnections } from '../../utils/queries'
import '../../components/HeaderActions.scss'

interface ExportSearchFilterProps {
  isSearchFilterOpen: boolean
  onToggle: (isSearchFilterOpen: boolean) => void
  onShow: (filters: UiExportSearchFilter) => void
  disabled?: boolean
}

function ExportSearchFilterTables({
  isSearchFilterOpen,
  onToggle,
  onShow,
  disabled
}: ExportSearchFilterProps) {
  const query = new URLSearchParams(location.search)

  const connection = query.get('connection') || null
  const targetTable = query.get('targetTable') || null
  const targetSchema = query.get('targetSchema') || null
  const includeInAirflow = query.get('includeInAirflow') || null
  const lastUpdateFromHive = query.get('lastUpdateFromHive') || null
  const exportType = query.get('exportType') || null
  const exportTool = query.get('exportTool') || null

  const [enumDropdownFilters, setEnumDropdownFilters] = useState<EditSetting[]>(
    initExportEnumDropdownFilters(exportType, exportTool)
  )

  const [includeInAirflowFilter, setIncludeInAirflowFilter] = useState({
    label: 'Include in Airflow',
    keyLabel: 'includeInAirflow',
    value: includeInAirflow,
    items: ['True', 'False']
  })

  const [lastUpdateFromHiveFilter, setLastUpdateFromHiveFilter] = useState({
    label: 'Last update from Hive',
    keyLabel: 'lastUpdateFromHive',
    value: lastUpdateFromHive,
    items: ['Last Day', 'Last Week', 'Last Month', 'Last Year']
  })

  const [isCnDropdownReady, setIsCnDropdownReady] = useAtom(
    isCnDropdownReadyAtom
  )

  const { data, isLoading } = useExportConnections()
  const connectionNames = useMemo(() => {
    return Array.isArray(data) ? data.map((connection) => connection.name) : []
  }, [data])

  const containerRef = useRef<HTMLDivElement>(null)
  const favoriteRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [formValues, setFormValues] = useState<UiExportSearchFilter>({
    connection: connection ? connection : null,
    targetTable: targetTable ? targetTable : null,
    targetSchema: targetSchema ? targetSchema : null,
    includeInAirflow: includeInAirflow ? includeInAirflow : null,
    lastUpdateFromHive: lastUpdateFromHive ? lastUpdateFromHive : null,
    exportType: exportType ? exportType : null,
    exportTool: exportTool ? exportTool : null
  })

  useFocusTrap(
    containerRef,
    isSearchFilterOpen,
    openDropdown === 'addFavoriteDropdown'
  )

  useEffect(() => {
    setEnumDropdownFilters(
      initExportEnumDropdownFilters(
        formValues.exportType,
        formValues.exportTool
      )
    )

    setIncludeInAirflowFilter((prevFilter) => ({
      ...prevFilter,
      value: formValues.includeInAirflow
    }))

    setLastUpdateFromHiveFilter((prevFilter) => ({
      ...prevFilter,
      value: formValues.lastUpdateFromHive
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

  const handleInputDropdownChange = (value: string | null) => {
    setFormValues((prev) => ({ ...prev, connection: value }))

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
    setFormValues((prev) => ({ ...prev, connection: item }))
  }

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
    } else if (keyLabel === 'lastUpdateFromHive') {
      const value = typeof item === 'string' ? item : null

      setLastUpdateFromHiveFilter((prev) => ({
        ...prev,
        value: value
      }))

      setFormValues((prev) => ({
        ...prev,
        lastUpdateFromHive: item as string | null
      }))
    } else if (settingType === 'enum') {
      const updatedFilters = enumDropdownFilters.map((filter) =>
        filter.label === keyLabel ? { ...filter, value: item } : filter
      )
      setEnumDropdownFilters(updatedFilters)

      const key = getKeyFromExportLabel(keyLabel)

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
    createTrimOnBlurHandler<UiExportSearchFilter>(setFormValues)

  return (
    <div className="search-filter-container" ref={containerRef}>
      <button
        className={`search-filter-button `}
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
            <FavoriteFilterSearch<UiExportSearchFilter>
              ref={favoriteRef}
              type="export"
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
                      handleInputDropdownChange(event.target.value)
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
                <label htmlFor="searchFilterTargetTable">
                  Target Table:
                  <input
                    id="searchFilterTargetTable"
                    type="text"
                    value={formValues.targetTable || ''}
                    onChange={(event) =>
                      setFormValues((prev) => ({
                        ...prev,
                        targetTable: event.target.value
                      }))
                    }
                    onBlur={handleTrimOnBlur('targetTable')}
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
                <label htmlFor="searchFilterTargetSchema">
                  Target Schema:
                  <input
                    id="searchFilterTargetSchema"
                    type="text"
                    value={formValues.targetSchema || ''}
                    onChange={(event) =>
                      setFormValues((prev) => ({
                        ...prev,
                        targetSchema: event.target.value
                      }))
                    }
                    onBlur={handleTrimOnBlur('targetSchema')}
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
                  htmlFor={`dropdown-lastUpdateFromHive`}
                  key={`dropdown-lastUpdateFromHive`}
                >
                  {lastUpdateFromHiveFilter.label}:
                  <Dropdown
                    id={`dropdown-lastUpdateFromHive`}
                    keyLabel={lastUpdateFromHiveFilter.keyLabel}
                    items={lastUpdateFromHiveFilter.items}
                    onSelect={handleSelect}
                    isOpen={openDropdown === `dropdown-lastUpdateFromHive`}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle(
                        `dropdown-lastUpdateFromHive`,
                        isOpen
                      )
                    }
                    searchFilter={false}
                    initialTitle={
                      lastUpdateFromHiveFilter.value
                        ? String(lastUpdateFromHiveFilter.value)
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

export default ExportSearchFilterTables
