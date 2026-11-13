import { useEffect, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { EditSetting } from './interfaces'

export function useCustomSelection(
  ref: React.RefObject<HTMLElement>,
  isDropdownOpen: boolean
) {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      // Checks if Cmd + A (Mac) or Ctrl + A (Windows) is pressed when the dropdown is open
      if (
        isDropdownOpen &&
        (event.metaKey || event.ctrlKey) &&
        event.key === 'a'
      ) {
        event.preventDefault()

        if (ref.current) {
          const selection = window.getSelection()
          const range = document.createRange()

          // Selects all the text inside the dropdown
          range.selectNodeContents(ref.current)
          selection?.removeAllRanges()
          selection?.addRange(range)
        }
      }
    }

    document.addEventListener('keydown', handleKeyDown)

    // Cleanup listener on unmount
    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isDropdownOpen, ref])
}

export const useFocusTrap = (
  containerRef: React.RefObject<HTMLElement>,
  isActive: boolean,
  isChildActive: boolean = false,
  firstFocus: boolean = false
) => {
  useEffect(() => {
    if (!isActive || !containerRef.current) return

    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'textarea:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      '[tabindex]:not([tabindex="-1"]):not([disabled])'
    ]

    const getFocusableElements = () =>
      Array.from(
        containerRef.current!.querySelectorAll<HTMLElement>(
          focusableSelectors.join(',')
        )
      )

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Tab' || isChildActive) return

      const focusableElements = getFocusableElements()

      if (focusableElements.length === 0) return

      const firstElement = focusableElements[0]
      const lastElement = focusableElements[focusableElements.length - 1]

      if (event.shiftKey) {
        if (document.activeElement === firstElement) {
          event.preventDefault()
          lastElement.focus()
        }
      } else {
        if (document.activeElement === lastElement) {
          event.preventDefault()
          firstElement.focus()
        }
      }
    }

    const firstFocusable = getFocusableElements()[0]
    if (firstFocus) firstFocusable?.focus()

    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [isActive, containerRef, isChildActive, firstFocus])
}

export const useHandleLinkClick = (
  type: 'import' | 'export' | 'airflowLink',
  item: string
) => {
  const navigate = useNavigate()

  const encodedItem = encodeURIComponent(item)

  if (type === 'import') {
    navigate(`/import?connection=${encodedItem}`)
  } else if (type === 'export') {
    navigate(`/export?connection=${encodedItem}`)
  } else {
    window.open(item, '_blank')
  }
}

export const useIsRequiredFieldsEmpty = (editedSettings: EditSetting[]) => {
  const isRequiredFieldEmpty = useMemo(() => {
    const requiredFields = editedSettings.filter(
      (setting) => setting.isRequired
    )

    return requiredFields.some(
      (setting) => setting.value === null || setting.value === ''
    )
  }, [editedSettings])

  return isRequiredFieldEmpty
}
