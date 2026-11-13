import { useCallback, useEffect, useRef, useState } from 'react'
import './Dropdown.scss'
import FilterFunnel from '../assets/icons/FilterFunnel'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import ChevronRight from '../assets/icons/ChevronRight'
import { EditSetting } from '../utils/interfaces'
import CrossIcon from '../assets/icons/CloseIcon'

interface DropdownProps<T> {
  items: T[]
  onSelect: (item: T | null, keyLabel?: string, settingType?: string) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
  searchFilter: boolean
  textInputMode?: boolean
  keyLabel?: string
  settingType?: string
  chevron?: boolean
  cross?: boolean
  initialTitle?: string
  placeholder?: string
  leftwards?: boolean
  backgroundColor?: string
  textColor?: string
  fontSize?: string
  border?: string
  borderRadius?: string
  height?: string
  width?: string
  chevronWidth?: string
  chevronHeight?: string
  lightStyle?: boolean
  disabled?: boolean
  id?: string
}

function Dropdown<T>({
  items,
  onSelect,
  isOpen,
  onToggle,
  searchFilter = true,
  textInputMode,
  keyLabel,
  settingType,
  chevron = false,
  cross = false,
  initialTitle,
  placeholder = 'Search...',
  leftwards,
  backgroundColor,
  textColor,
  fontSize,
  border,
  borderRadius,
  height,
  width,
  chevronWidth,
  chevronHeight,
  lightStyle,
  disabled = false,
  id
}: DropdownProps<T>): JSX.Element {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedItem, setSelectedItem] = useState<T | null>(
    (initialTitle as T) || null
  )
  const [savedScrollPosition, setSavedScrollPosition] = useState<number>(0)
  const [highlightedIndex, setHighlightedIndex] = useState<number>(-1)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)
  const searchInputRef = useRef<HTMLInputElement>(null)

  const handleSelect = useCallback(
    (item: T | null) => {
      if (disabled) return

      setSelectedItem(item)

      if (settingType) {
        onSelect(item, keyLabel, settingType)
      } else if (keyLabel) {
        onSelect(item, keyLabel)
      } else {
        onSelect(item)
      }

      setHighlightedIndex(-1)
      onToggle(false)
      setSearchTerm('')
    },
    [disabled, keyLabel, onSelect, onToggle, settingType]
  )

  const getItemLabel = (item: T): string => {
    if (typeof item === 'string') {
      return item
    }
    return (item as EditSetting).label
  }

  const filteredItems = (
    items?.filter((item) =>
      getItemLabel(item).toLowerCase().includes(searchTerm.toLowerCase())
    ) ?? []
  ).filter((item) => item !== selectedItem)

  useEffect(() => {
    if (initialTitle) {
      setSelectedItem(initialTitle as T)
    }
  }, [initialTitle])

  useEffect(() => {
    if (textInputMode) {
      if (items.length > 0) {
        setHighlightedIndex((prevIndex) =>
          prevIndex >= items.length || prevIndex < 0 ? -1 : prevIndex
        )
      } else {
        setHighlightedIndex(-1)
      }
    } else {
      if (filteredItems.length > 0) {
        setHighlightedIndex((prevIndex) =>
          prevIndex >= filteredItems.length || prevIndex < 0 ? -1 : prevIndex
        )
      } else {
        setHighlightedIndex(-1)
      }
    }
  }, [filteredItems, highlightedIndex, items.length, textInputMode])

  useEffect(() => {
    if (disabled) return

    if (!isOpen) {
      setHighlightedIndex(-1)
      return
    }

    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        onToggle(false)
        setHighlightedIndex(-1)
      }
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onToggle(false)
        setHighlightedIndex(-1)
      }

      if (event.key === 'ArrowDown') {
        event.preventDefault()
        setHighlightedIndex((prev) =>
          textInputMode
            ? Math.min(prev + 1, items.length - 1)
            : Math.min(prev + 1, filteredItems.length - 1)
        )
      }

      if (event.key === 'ArrowUp') {
        event.preventDefault()
        setHighlightedIndex((prev) =>
          textInputMode ? Math.max(prev - 1, 0) : Math.max(prev - 1, 0)
        )
      }

      if (event.key === 'Enter') {
        if (!isOpen) {
          onToggle(true)
        } else if (highlightedIndex >= 0) {
          const selected = textInputMode
            ? items[highlightedIndex]
            : filteredItems[highlightedIndex]

          // if (event.key === 'Enter' && highlightedIndex >= 0) {
          //   const selected = textInputMode
          //     ? items[highlightedIndex]
          //     : filteredItems[highlightedIndex]

          handleSelect(selected)
          setHighlightedIndex(-1)
          // onToggle(false)
        }
      }
    }

    document.addEventListener('mouseup', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mouseup', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [
    disabled,
    filteredItems,
    handleSelect,
    highlightedIndex,
    isOpen,
    items,
    items.length,
    onToggle,
    textInputMode
  ])

  useEffect(() => {
    if (isOpen && searchFilter && searchInputRef.current) {
      searchInputRef.current.focus()
    }
  }, [isOpen, searchFilter])

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (disabled) return

    const value = e.target.value

    if (menuRef.current) {
      if (value) {
        setSavedScrollPosition(menuRef.current.scrollTop)
        menuRef.current.scrollTop = 0
      } else {
        menuRef.current.scrollTop = savedScrollPosition
      }
    }

    setSearchTerm(value)
    setHighlightedIndex(value ? 0 : -1)
  }

  const dropdownStyle: React.CSSProperties = {
    backgroundColor,
    color: textColor,
    fontSize,
    border,
    borderRadius,
    height,
    width,
    cursor: disabled ? 'not-allowed' : 'pointer',
    opacity: disabled ? 0.5 : 1
  }

  const itemContentStyle: React.CSSProperties = {
    padding: dropdownStyle.padding,
    height: dropdownStyle.height
      ? `calc(${dropdownStyle.height} + 5px)`
      : undefined
    // backgroundColor: 'blue',
    // border: '1px solid red'
  }

  const heightStyle: React.CSSProperties = {
    height: dropdownStyle.height
      ? `calc(${dropdownStyle.height} + 7px)`
      : undefined
  }

  const dropdownPositionStyle: React.CSSProperties = {
    top: textInputMode ? -3 : undefined
  }

  return (
    <div className="dropdown" ref={dropdownRef} id={id}>
      {!textInputMode && (
        <div
          className="dropdown-selected-item"
          style={dropdownStyle}
          onClick={() => !disabled && onToggle(!isOpen)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              onToggle(!isOpen)
            }

            if (event.key === 'Backspace') {
              event.stopPropagation()
              handleSelect(null)
            }
          }}
          tabIndex={0}
        >
          {selectedItem ? getItemLabel(selectedItem) : initialTitle}

          <div>
            {cross &&
            selectedItem &&
            selectedItem !== 'Select...' &&
            !isOpen ? (
              <div
                className="cross-icon-container"
                onClick={(event) => {
                  event.stopPropagation()
                  handleSelect(null)
                }}
              >
                <CrossIcon />
              </div>
            ) : isOpen ? (
              <div className="chevron-container">
                <ChevronUp
                  width={chevronWidth || ''}
                  height={chevronHeight || ''}
                />
              </div>
            ) : (
              <div className="chevron-container">
                <ChevronDown
                  width={chevronWidth || ''}
                  height={chevronHeight || ''}
                />
              </div>
            )}
          </div>
        </div>
      )}

      {!disabled && isOpen && (
        <div
          ref={menuRef}
          className={
            leftwards
              ? lightStyle
                ? 'light-menu leftwards'
                : 'menu leftwards'
              : lightStyle
              ? 'light-menu'
              : 'menu'
          }
          style={dropdownPositionStyle}
        >
          {searchFilter && (
            <div className="search" style={heightStyle}>
              <span className="icon">
                <FilterFunnel />
              </span>
              <input
                type="text"
                value={searchTerm}
                placeholder={placeholder}
                onChange={handleInputChange}
                ref={searchInputRef}
              />
            </div>
          )}

          {textInputMode ? (
            <ul>
              {Array.isArray(items) &&
                items.map((item, index) => (
                  <li
                    key={index}
                    onClick={() => {
                      setHighlightedIndex(index)
                      handleSelect(item)
                    }}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        setHighlightedIndex(index)
                        handleSelect(item)
                      }
                    }}
                    style={
                      index === highlightedIndex
                        ? { backgroundColor: 'lightgrey' }
                        : {}
                    }
                  >
                    <div className="item-content" style={itemContentStyle}>
                      <span className="item-text">{getItemLabel(item)}</span>
                      {chevron && !leftwards && <ChevronRight />}
                    </div>
                  </li>
                ))}
            </ul>
          ) : (
            <ul>
              {Array.isArray(filteredItems) && filteredItems.length ? (
                filteredItems.map((item, index) => (
                  <li
                    key={index}
                    onClick={(event) => {
                      event.stopPropagation()
                      setHighlightedIndex(index)
                      handleSelect(item)
                    }}
                    style={
                      index === highlightedIndex
                        ? { backgroundColor: 'lightgrey' }
                        : {}
                    }
                  >
                    <div className="item-content" style={itemContentStyle}>
                      <span className="item-text">{getItemLabel(item)}</span>
                      {chevron && !leftwards && <ChevronRight />}
                    </div>
                  </li>
                ))
              ) : (
                <li className="no-results">No results found</li>
              )}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}

export default Dropdown
