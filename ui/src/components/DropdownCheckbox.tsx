import { useEffect, useRef } from 'react'
import './DropdownCheckbox.scss'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'

interface DropdownCheckboxProps {
  items: string[]
  title: string
  selectedItems: string[]
  onSelect: (selectedItems: string[]) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
  lightStyle?: boolean
}

function DropdownCheckbox({
  items,
  title,
  selectedItems,
  onSelect,
  isOpen,
  onToggle,
  lightStyle
}: DropdownCheckboxProps) {
  const dropdownRef = useRef<HTMLDivElement>(null)

  const handleSelect = (item: string) => {
    const newSelectedItems = selectedItems.includes(item)
      ? selectedItems?.filter((i) => i !== item) ?? []
      : [...selectedItems, item]

    onSelect(newSelectedItems)
  }

  useEffect(() => {
    if (!isOpen) return

    const handleClickOutside = (event: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target as Node)
      ) {
        onToggle(false)
      }
    }

    const handleKeyUp = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onToggle(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keyup', handleKeyUp)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keyup', handleKeyUp)
    }
  }, [isOpen, onToggle])

  return (
    <div className="checkbox-dropdown" ref={dropdownRef}>
      <div
        className={
          lightStyle
            ? 'light-checkbox-dropdown-selected-item'
            : 'checkbox-dropdown-selected-item'
        }
        tabIndex={0} // For focusability
        onClick={() => onToggle(!isOpen)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault()
            onToggle(!isOpen)
          }
        }}
      >
        {title}
        {Array.isArray(selectedItems) && selectedItems.length > 0 ? (
          <span
            className="count-badge"
            style={
              lightStyle
                ? { backgroundColor: '#ffffff', border: '1px solid darkGrey' }
                : undefined
            }
          >
            {selectedItems.length}{' '}
          </span>
        ) : (
          <span className="count-badge-placeholder" />
        )}

        <div className="chevron-container">
          {isOpen ? <ChevronUp /> : <ChevronDown />}
        </div>
      </div>

      {isOpen && (
        <div className={lightStyle ? 'light-menu' : 'menu'}>
          <ul>
            {Array.isArray(items) && items.length ? (
              items.map((item, index) => (
                <li key={index} onClick={() => handleSelect(item)}>
                  <div className="item-content">
                    <input
                      type="checkbox"
                      className="checkbox"
                      checked={selectedItems.includes(item)}
                      onChange={() => {}}
                    />
                    <span className="item-text">{item}</span>
                  </div>
                </li>
              ))
            ) : (
              <li className="no-results" no-hover>
                No results found
              </li>
            )}
          </ul>
        </div>
      )}
    </div>
  )
}

export default DropdownCheckbox
