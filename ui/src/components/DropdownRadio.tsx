import { useEffect, useRef } from 'react'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import './DropdownRadio.scss'

interface DropdownRadioProps {
  items: string[]
  title: string
  radioName: string
  selectedItem: string | null
  onSelect: (selectedItems: string[]) => void
  isOpen: boolean
  onToggle: (isOpen: boolean) => void
  badgeContent?: string[]
  lightStyle?: boolean
}

function DropdownRadio({
  items,
  title,
  radioName,
  selectedItem,
  onSelect,
  isOpen,
  onToggle,
  badgeContent,
  lightStyle = false
}: DropdownRadioProps) {
  const dropdownRef = useRef<HTMLDivElement>(null)

  const handleSelect = (item: string) => {
    if (selectedItem === item) {
      onSelect([])
    } else {
      onSelect([item])
    }
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

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onToggle(false)
      }
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isOpen, onToggle])

  return (
    <div className="radio-dropdown" ref={dropdownRef}>
      <div
        className={
          lightStyle
            ? 'light-radio-dropdown-selected-item'
            : 'radio-dropdown-selected-item'
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
        {badgeContent && selectedItem ? (
          <span
            className="count-badge"
            style={
              lightStyle
                ? { backgroundColor: '#ffffff', border: '1px solid darkGrey' }
                : undefined
            }
          >
            {badgeContent[items.indexOf(selectedItem)]}
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
                      name={radioName}
                      className="checkbox"
                      checked={selectedItem === item}
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

export default DropdownRadio
