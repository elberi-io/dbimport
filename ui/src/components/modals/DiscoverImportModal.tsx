import {
  SetStateAction,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react'
import ReactDOM from 'react-dom'
import Button from '../Button'
import ConfirmationModal from './ConfirmationModal'
import './Modals.scss'
import { useFocusTrap } from '../../utils/hooks'
import { createTrimOnBlurHandler } from '../../utils/functions'
import {
  Column,
  ImportDiscoverSearch,
  ImportDiscoverTable,
  UiImportDiscoverSearch
} from '../../utils/interfaces'
import Dropdown from '../Dropdown'
import { useAtom } from 'jotai'
import {
  isDbDropdownReadyAtom,
  isMainSidebarMinimized
} from '../../atoms/atoms'
import {
  useDatabases,
  useConnections,
  useDiscoverImportTables
} from '../../utils/queries'
import TableList from '../TableList'
import RequiredFieldsInfo from '../RequiredFieldsInfo'
import { useAddImportTables } from '../../utils/mutations'
import { useQueryClient } from '@tanstack/react-query'
import infoTexts from '../../infoTexts.json'
import InfoText from '../InfoText'

const columns: Column<ImportDiscoverTable>[] = [
  { header: 'Table', accessor: 'table' },
  { header: 'Connection', accessor: 'connection' },
  { header: 'Database', accessor: 'database' },
  { header: 'Source Schema', accessor: 'sourceSchema' },
  { header: 'Source Table', accessor: 'sourceTable' }
]

interface DiscoverModalProps {
  isDiscoverModalOpen: boolean
  title: string
  onClose: () => void
}

function DiscoverImportModal({
  isDiscoverModalOpen,
  title,
  onClose
}: DiscoverModalProps) {
  const queryClient = useQueryClient()

  const [openDropdown, setOpenDropdown] = useState<string | null>(null)

  const [hasChanges, setHasChanges] = useState(false)
  const [showConfirmation, setShowConfirmation] = useState(false)

  const [modalWidth, setModalWidth] = useState(760)
  const [isResizing, setIsResizing] = useState(false)
  const [initialMouseX, setInitialMouseX] = useState(0)
  const [initialWidth, setInitialWidth] = useState(760)
  const MIN_WIDTH = 584

  const containerRef = useRef<HTMLDivElement>(null)
  useFocusTrap(containerRef, isDiscoverModalOpen, showConfirmation)

  const [rowSelection, setRowSelection] = useState({})

  const [formValues, setFormValues] = useState<UiImportDiscoverSearch>({
    connection: null,
    database: null,
    schemaFilter: null,
    tableFilter: null,
    addSchemaToTable: false,
    addCounterToTable: false,
    counterStart: null,
    addCustomText: null
  })

  const [filters, setFilters] = useState<ImportDiscoverSearch | null>(null)

  const isConnectionEmpty =
    formValues.connection === '' || formValues.connection === null

  const isDatabaseEmpty =
    formValues.database === '' || formValues.database === null

  const isRequiredFieldEmpty = isConnectionEmpty || isDatabaseEmpty

  const isRowSelectionEmpty = Object.keys(rowSelection).length === 0

  const [isDbDropdownReady, setIsDbDropdownReady] = useAtom(
    isDbDropdownReadyAtom
  )

  const [mainSidebarMinimized] = useAtom(isMainSidebarMinimized)

  const { mutate: addImportTables } = useAddImportTables()

  const { data: databasesData, isLoading: isDbLoading } = useDatabases()
  const databaseNames = useMemo(() => {
    return Array.isArray(databasesData)
      ? databasesData.map((database) => database.name)
      : []
  }, [databasesData])

  const { data: connectionsData } = useConnections(true)
  const connectionNames = useMemo(
    () =>
      Array.isArray(connectionsData)
        ? connectionsData?.map((connection) => connection.name)
        : [],
    [connectionsData]
  )

  const { data: discoverTables, isLoading: isDiscoverLoading } =
    useDiscoverImportTables(filters)

  const [uiTables, setUiTables] = useState<ImportDiscoverTable[]>([])

  useEffect(() => {
    if (discoverTables) {
      setUiTables(discoverTables)
    }
  }, [discoverTables])

  const selectedRowsBulkData = useMemo(() => {
    const selectedIndexes = Object.keys(rowSelection).map((id) =>
      parseInt(id, 10)
    )
    return selectedIndexes.map((index) => uiTables[index])
  }, [uiTables, rowSelection])

  useEffect(() => {
    if (isDbLoading) return
    setIsDbDropdownReady(true)
  }, [isDbLoading, setIsDbDropdownReady])

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

  const handleTrimOnBlur =
    createTrimOnBlurHandler<UiImportDiscoverSearch>(setFormValues)

  const handleInputDropdownChange = (
    key: 'database' | 'connection',
    value: string | null
  ) => {
    setFormValues((prev) => ({ ...prev, [key]: value }))
    setHasChanges(true)

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
        setOpenDropdown(key === 'database' ? 'dbDiscover' : 'cnDiscover')
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
    setHasChanges(true)
  }

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setTimeout(() => {
        setOpenDropdown(null)
      }, 0)
    }
  }

  const handleSearchClick = () => {
    const filters = formValues as ImportDiscoverSearch
    setFilters(filters)
  }

  const handleTablesAdd = () => {
    if (selectedRowsBulkData === undefined) return

    addImportTables(selectedRowsBulkData, {
      onSuccess: () => {
        setUiTables((prev) =>
          prev.filter((table) => !selectedRowsBulkData.includes(table))
        )

        queryClient.invalidateQueries({
          queryKey: ['import'], // Matches all related queries that starts the queryKey with 'import'
          exact: false
        })
        console.log('Add tables to Import was successful')
        setRowSelection({})
      },
      onError: (error) => {
        console.log('Error adding tables', error.message)
      }
    })
  }

  const handleCloseClick = () => {
    console.log('hasChanges', hasChanges)

    if (hasChanges) {
      setShowConfirmation(true)
    } else {
      onClose()
    }
  }

  const handleConfirmCancel = () => {
    setShowConfirmation(false)
    onClose()
  }

  const handleCloseConfirmation = () => {
    setShowConfirmation(false)
  }

  const handleMouseDown = (e: { clientX: SetStateAction<number> }) => {
    setIsResizing(true)
    setInitialMouseX(e.clientX)
    setInitialWidth(modalWidth)
    document.body.classList.add('resizing')
  }

  const handleMouseUp = useCallback(() => {
    setIsResizing(false)
    document.body.classList.remove('resizing')
  }, [])

  const handleMouseMove = useCallback(
    (e: { clientX: number }) => {
      if (isResizing) {
        const deltaX = e.clientX - initialMouseX
        setModalWidth(Math.max(initialWidth + deltaX, MIN_WIDTH))
      }
    },
    [isResizing, initialMouseX, initialWidth]
  )

  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    } else {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing, handleMouseMove, handleMouseUp])

  return ReactDOM.createPortal(
    <div className="table-modal-backdrop">
      <div
        className={`table-modal-content ${
          mainSidebarMinimized ? 'sidebar-minimized' : ''
        }`}
        style={{ width: `${modalWidth}px` }}
        ref={containerRef}
      >
        {/* <div
          className="table-modal-resize-handle left"
          onMouseDown={handleMouseDown}
        ></div> */}
        <div
          className="table-modal-resize-handle right"
          onMouseDown={handleMouseDown}
        ></div>
        <div className="discover-modal-header">
          <h2 className="table-modal-h2">{title}</h2>
          <p>
            Discover tables not yet added to DBImport, select and optionally add
            them.
          </p>
          <p>{`Use the asterisk (*) as a wildcard character in the filters for partial matches.`}</p>
        </div>
        <form
          onSubmit={(event) => {
            event.preventDefault()
            const activeElement = document.activeElement as HTMLElement

            // Triggers Add only if Add button with type submit is clicked or focused+Enter
            if (
              activeElement &&
              activeElement.getAttribute('type') === 'submit'
            ) {
              handleTablesAdd()
            }
          }}
        >
          <div className="discover-container">
            <div className="discover-first-container">
              <label htmlFor="discoverConnection">
                <span>
                  Connection:
                  {isConnectionEmpty && <span style={{ color: 'red' }}>*</span>}
                </span>

                <div style={{ display: 'flex' }}>
                  <Dropdown
                    id="discoverConnection"
                    keyLabel="connection"
                    items={
                      filteredConnectionNames.length > 0
                        ? filteredConnectionNames
                        : ['No Connection yet']
                    }
                    onSelect={handleInputDropdownSelect}
                    isOpen={openDropdown === 'cnDiscover'}
                    onToggle={(isOpen: boolean) =>
                      handleDropdownToggle('cnDiscover', isOpen)
                    }
                    searchFilter={true}
                    initialTitle={'Select...'}
                    cross={true}
                    backgroundColor="inherit"
                    textColor="black"
                    border="0.5px solid rgb(42, 42, 42)"
                    borderRadius="3px"
                    height="21.5px"
                    width="100%"
                    lightStyle={true}
                  />
                  <span style={{ marginLeft: 5 }}>
                    <InfoText
                      label={'Connection'}
                      infoText={infoTexts.discover.import.connection}
                      // isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>
              <label htmlFor="discoverSchemaFilter">
                <span>Schema filter:</span>
                <div style={{ display: 'flex' }}>
                  <input
                    id="discoverSchemaFilter"
                    type="text"
                    value={formValues.schemaFilter || ''}
                    onChange={(event) => {
                      setFormValues((prev) => ({
                        ...prev,
                        schemaFilter: event.target.value
                      }))
                      setHasChanges(true)
                    }}
                    onBlur={handleTrimOnBlur('schemaFilter')}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"
                  />
                  <span style={{ marginLeft: 5 }}>
                    <InfoText
                      label={'Schema filter'}
                      infoText={infoTexts.discover.import.schemaFilter}
                      // isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>
              <label htmlFor="discoverTableFilter">
                <span>Table filter:</span>
                <div style={{ display: 'flex' }}>
                  <input
                    id="discoverTableFilter"
                    type="text"
                    value={formValues.tableFilter || ''}
                    onChange={(event) => {
                      setFormValues((prev) => ({
                        ...prev,
                        tableFilter: event.target.value
                      }))
                      setHasChanges(true)
                    }}
                    onBlur={handleTrimOnBlur('tableFilter')}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"
                  />
                  <span style={{ marginLeft: 5 }}>
                    <InfoText
                      label={'Table filter'}
                      infoText={infoTexts.discover.import.tableFilter}
                      // isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>
            </div>
            <div className="discover-second-container">
              <label
                htmlFor="discoverDatabase"
                className="discover-db-dropdown"
              >
                <span>
                  Database:
                  {isDatabaseEmpty && <span style={{ color: 'red' }}>*</span>}
                </span>
                <div style={{ display: 'flex' }}>
                  <div style={{ width: '100%' }}>
                    <input
                      id="discoverDatabase"
                      type="text"
                      value={formValues.database || ''}
                      onChange={(event) =>
                        handleInputDropdownChange(
                          'database',
                          event.target.value
                        )
                      }
                      onBlur={handleTrimOnBlur('database')}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter') {
                          event.preventDefault() // Prevents Enter from triggering form submission
                        }
                      }}
                      autoComplete="off"
                    />
                    {isDbDropdownReady && (
                      <Dropdown
                        id="discoverDatabase"
                        keyLabel="database"
                        items={
                          filteredDatabaseNames.length > 0
                            ? filteredDatabaseNames
                            : ['No DB yet']
                        }
                        onSelect={handleInputDropdownSelect}
                        isOpen={openDropdown === 'dbDiscover'}
                        onToggle={(isOpen: boolean) =>
                          handleDropdownToggle('dbDiscover', isOpen)
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
                  </div>
                  <span style={{ marginLeft: 12 }}>
                    <InfoText
                      label={'Database'}
                      infoText={infoTexts.discover.import.database}
                      // isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>
              <label>
                <span>Add schema to table:</span>
                <div className="discover-radio-edit">
                  <label>
                    <input
                      type="radio"
                      name={`discover-add-schema-true`}
                      value="true"
                      checked={formValues.addSchemaToTable === true}
                      onChange={() => {
                        setFormValues((prev) => ({
                          ...prev,
                          addSchemaToTable: true
                        }))
                        setHasChanges(true)
                      }}
                    />
                    True
                  </label>
                  <label>
                    <input
                      type="radio"
                      name={`discover-add-schema-false`}
                      value="false"
                      checked={formValues.addSchemaToTable === false}
                      onChange={() => {
                        setFormValues((prev) => ({
                          ...prev,
                          addSchemaToTable: false
                        }))
                        setHasChanges(true)
                      }}
                    />
                    False
                  </label>
                  <span style={{ marginLeft: 5 }}>
                    <InfoText
                      label={'Add schema to table'}
                      infoText={infoTexts.discover.import.addSchemaToTable}
                      // isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>
              <label>
                <span>Add counter to table:</span>
                <div className="discover-radio-edit">
                  <label>
                    <input
                      type="radio"
                      name={`discover-add-counter-true`}
                      value="true"
                      checked={formValues.addCounterToTable === true}
                      onChange={() => {
                        setFormValues((prev) => ({
                          ...prev,
                          addCounterToTable: true
                        }))
                        setHasChanges(true)
                      }}
                    />
                    True
                  </label>
                  <label>
                    <input
                      type="radio"
                      name={`discover-add-counter-false`}
                      value="false"
                      checked={formValues.addCounterToTable === false}
                      onChange={() => {
                        setFormValues((prev) => ({
                          ...prev,
                          addCounterToTable: false
                        }))
                        setHasChanges(true)
                      }}
                    />
                    False
                  </label>
                  <label htmlFor="discoverCounterStart">
                    <span
                      style={
                        formValues.addCounterToTable === false
                          ? { color: '#a0a0a0' }
                          : {}
                      }
                    >
                      Start:
                    </span>
                    <input
                      id="discoverCounterStart"
                      // ref={inputRef}
                      className="input-fields-number-input"
                      style={
                        formValues.addCounterToTable === false
                          ? { minHeight: 0, color: '#a0a0a0' }
                          : { minHeight: 0 }
                      }
                      type="number"
                      value={formValues.counterStart || ''}
                      onChange={(event) => {
                        let value: number | null =
                          event.target.value === ''
                            ? null
                            : Number(event.target.value)

                        if (typeof value === 'number' && value < 1) {
                          value = null
                          event.target.value = ''
                        }

                        setFormValues((prev) => ({
                          ...prev,
                          counterStart: isNaN(Number(value)) ? null : value
                        }))
                        setHasChanges(true)
                      }}
                      onBlur={(event) => {
                        const valueString = event.target.value.trim()

                        // If empty or < 1, resets to null
                        if (
                          isNaN(Number(valueString)) ||
                          Number(valueString) < 1
                        ) {
                          setFormValues((prev) => ({
                            ...prev,
                            counterStart: null
                          }))
                          event.target.value = ''
                        } else {
                          // Converts e.g. `+10` to `10` (removes any valid sign characters, including Dead)
                          const numericValue = Number(valueString)
                          setFormValues((prev) => ({
                            ...prev,
                            counterStart: numericValue
                          }))
                          event.target.value = String(numericValue)
                        }
                      }}
                      onKeyDown={(event) => {
                        // Prevent invalid characters from being typed
                        if (
                          ['e', 'E', '+', '-', '.', ',', 'Dead'].includes(
                            event.key
                          ) // Dead is still working, fix so it is not
                        ) {
                          event.preventDefault()
                        }
                      }}
                      step="1"
                      disabled={formValues.addCounterToTable === false}
                    />
                  </label>
                  <span style={{ marginLeft: 5 }}>
                    <InfoText
                      label={'Add counter to table'}
                      infoText={infoTexts.discover.import.addCounterToTable}
                      isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>

              <label htmlFor="discoverCustomText">
                <span>Add custom text:</span>
                <div style={{ display: 'flex' }}>
                  <input
                    id="discoverCustomText"
                    type="text"
                    value={formValues.addCustomText || ''}
                    onChange={(event) => {
                      setFormValues((prev) => ({
                        ...prev,
                        addCustomText: event.target.value
                      }))
                      setHasChanges(true)
                    }}
                    onBlur={handleTrimOnBlur('addCustomText')}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault() // Prevents Enter from triggering form submission
                      }
                    }}
                    autoComplete="off"
                  />
                  <span style={{ marginLeft: 5 }}>
                    <InfoText
                      label={'Add custom text'}
                      infoText={infoTexts.discover.import.addCustomText}
                      isInfoTextPositionRight={true}
                    />
                  </span>
                </div>
              </label>
            </div>
          </div>
          <RequiredFieldsInfo isRequiredFieldEmpty={isRequiredFieldEmpty} />

          <div className="discover-button-container">
            <div />
            <Button
              title="Discover"
              disabled={isRequiredFieldEmpty}
              onClick={handleSearchClick}
            />
          </div>
          <p style={{ marginTop: 0 }}>Results:</p>
          {discoverTables ? (
            <TableList
              columns={columns}
              data={uiTables}
              // onEdit={handleEditClick}
              // onDelete={handleDeleteIconClick}
              isLoading={isDiscoverLoading}
              rowSelection={rowSelection}
              containerRef={containerRef}
              onRowSelectionChange={setRowSelection}
              enableMultiSelection={true}
              noLinkOnTableName={true}
              enableSourceTableOverflow={true}
            />
          ) : isDiscoverLoading ? (
            <div className="text-block discover">
              <p>Loading...</p>
            </div>
          ) : (
            <div className="text-block discover">
              <p>No tables yet.</p>
            </div>
          )}

          <div className="discover-button-container footer">
            <Button
              title="Close"
              onClick={handleCloseClick}
              lightStyle={true}
            />
            <Button type="submit" title="Add" disabled={isRowSelectionEmpty} />
          </div>
        </form>
      </div>
      {showConfirmation && (
        <ConfirmationModal
          title="Close Discover and Add tables"
          message="Modal will be cleared."
          buttonTitleCancel="No, Go Back"
          buttonTitleConfirm="Yes, Close"
          onConfirm={handleConfirmCancel}
          onCancel={handleCloseConfirmation}
          isActive={showConfirmation}
        />
      )}
    </div>,
    document.body
  )
}

export default DiscoverImportModal
