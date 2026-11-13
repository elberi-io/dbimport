import React, { ReactNode, useState, useEffect, useRef } from 'react'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import './HeaderActions.scss'

interface DropdownActionItem {
  icon: ReactNode
  label: string
  onClick: () => void
}

interface DropdownActionsProps {
  items: DropdownActionItem[]
  isDropdownActionsOpen: boolean
  onToggle: (isDropdownActionsOpen: boolean) => void
  disabled?: boolean
  marginTop?: number
  maxWidth?: number
}

function DropdownActions({
  items,
  isDropdownActionsOpen,
  onToggle,
  disabled = false,
  marginTop = 0,
  maxWidth
}: DropdownActionsProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)

  const handleKeyDown = (event: React.KeyboardEvent<HTMLButtonElement>) => {
    if (!isDropdownActionsOpen) return

    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault()
        setActiveIndex((prev) =>
          prev === null || prev === items.length - 1 ? 0 : prev + 1
        )
        break
      case 'ArrowUp':
        event.preventDefault()
        setActiveIndex((prev) =>
          prev === null || prev === 0 ? items.length - 1 : prev - 1
        )
        break

      case 'Enter':
        event.preventDefault()
        if (activeIndex !== null) {
          // Simulate a click on the highlighted item
          items[activeIndex].onClick()
        }
        break

      case 'Escape':
        onToggle(false)
        break
    }
  }

  useEffect(() => {
    if (!isDropdownActionsOpen) {
      setActiveIndex(null)
    }
  }, [isDropdownActionsOpen])

  return (
    <>
      <div
        className="actions-dropdown-container"
        style={{ marginTop: marginTop }}
      >
        <button
          className="action-dropdown-button"
          onClick={() => onToggle(!isDropdownActionsOpen)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
        >
          Actions
          <div className="action-dropdown-chevron-container">
            {isDropdownActionsOpen ? <ChevronUp /> : <ChevronDown />}
          </div>
        </button>

        {isDropdownActionsOpen && (
          <div className="actions-dropdown-wrapper" ref={dropdownRef}>
            {items.map((item, index) => (
              <div
                key={index}
                className={`actions-dropdown-item ${
                  activeIndex === index ? 'highlighted' : ''
                } ${
                  items.length === 1
                    ? 'first last'
                    : index === 0
                    ? 'first'
                    : index === items.length - 1
                    ? 'last'
                    : ''
                }`}
                onClick={item.onClick}
                tabIndex={0}
              >
                {item.icon}{' '}
                <p
                  style={{
                    margin: 0,
                    maxWidth: maxWidth,
                    wordBreak: 'break-word'
                  }}
                >
                  {item.label}
                </p>
              </div>
            ))}
          </div>
        )}
      </div>
    </>
  )
}

export default DropdownActions
