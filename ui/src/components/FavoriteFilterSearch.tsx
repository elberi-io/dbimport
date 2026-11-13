import {
  useState,
  useEffect,
  useRef,
  useImperativeHandle,
  forwardRef
} from 'react'
import StarIcon from '../assets/icons/StarIcon'
import StarFillIcon from '../assets/icons/StarFillIcon'
import ChevronDown from '../assets/icons/ChevronDown'
import ChevronUp from '../assets/icons/ChevronUp'
import './FavoriteFilterSearch.scss'
import CrossIcon from '../assets/icons/CloseIcon'
import Button from './Button'
import { useFocusTrap } from '../utils/hooks'

interface FavoriteFilterSearchProps<T> {
  formValues: T
  type: string
  onSelectFavorite: (favorite: T) => void
  openDropdown: string | null
  handleDropdownToggle: (dropdownId: string, isOpen: boolean) => void
}

function FavoriteFilterSearch<T>(
  props: FavoriteFilterSearchProps<T>,
  ref: React.Ref<HTMLDivElement | null>
) {
  const {
    formValues,
    type,
    onSelectFavorite,
    openDropdown,
    handleDropdownToggle
  } = props
  const localRef = useRef<HTMLDivElement>(null)
  const addFavoriteNameDropdown = useRef<HTMLDivElement>(null)

  // Exposes the ref to the parent
  useImperativeHandle(ref, () => localRef.current)

  const [favorites, setFavorites] = useState<{ name: string; state: T }[]>(
    () => {
      const storedFavorites = localStorage.getItem(`favorites-${type}`)
      return storedFavorites ? JSON.parse(storedFavorites) : []
    }
  )

  const [favoriteName, setFavoriteName] = useState<string>('')
  const [isNameTaken, setIsNameTaken] = useState<boolean>(false)
  const [highlightedIndex, setHighlightedIndex] = useState<number>(-1)

  const [matchingFavorite, setMatchingFavorite] = useState<string | null>(null)

  useEffect(() => {
    const matching = favorites.find(
      (fav) => JSON.stringify(fav.state) === JSON.stringify(formValues)
    )
    setMatchingFavorite(matching ? matching.name : null)
  }, [formValues, favorites])

  const handleSaveFavorite = () => {
    if (!favoriteName.trim()) {
      return
    }

    setIsNameTaken(favorites.some((fav) => fav.name === favoriteName.trim()))

    const newFavorite = { name: favoriteName, state: { ...formValues } }
    const updatedFavorites = [...favorites, newFavorite]
    setFavorites(updatedFavorites)
    localStorage.setItem(`favorites-${type}`, JSON.stringify(updatedFavorites))
    setFavoriteName('')
    handleDropdownToggle('addFavoriteDropdown', false)
  }

  const handleDeleteFavorite = (name: string) => {
    const updatedFavorites = favorites.filter((fav) => fav.name !== name)
    setFavorites(updatedFavorites)
    localStorage.setItem(`favorites-${type}`, JSON.stringify(updatedFavorites))
  }

  const handleSelectFavorite = (favoriteState: T) => {
    onSelectFavorite(favoriteState)
    handleDropdownToggle('favoritesDropdown', false)
    setHighlightedIndex(-1)
  }

  const handleToggleStar = () => {
    if (matchingFavorite) {
      handleDeleteFavorite(matchingFavorite)
      setMatchingFavorite(null)
      setHighlightedIndex(-1)
    } else {
      handleDropdownToggle('addFavoriteDropdown', true)
    }
  }

  useFocusTrap(
    addFavoriteNameDropdown,
    openDropdown === 'addFavoriteDropdown',
    false,
    true
  )

  useEffect(() => {
    if (openDropdown !== 'favoritesDropdown') return

    const handleKeyDown = (event: KeyboardEvent) => {
      if (favorites.length === 0) return

      if (event.key === 'ArrowDown') {
        event.preventDefault()
        setHighlightedIndex((prev) =>
          prev === favorites.length - 1 ? 0 : prev + 1
        )
      }

      if (event.key === 'ArrowUp') {
        event.preventDefault()
        setHighlightedIndex((prev) =>
          prev <= 0 ? favorites.length - 1 : prev - 1
        )
      }

      if (event.key === 'Enter' && highlightedIndex >= 0) {
        event.preventDefault()
        const selectedFavorite = favorites[highlightedIndex]
        if (selectedFavorite) {
          handleSelectFavorite(selectedFavorite.state)
          setHighlightedIndex(-1)
        }
      }

      if (event.key === 'Backspace' && highlightedIndex >= 0) {
        event.preventDefault()
        const favoriteToDelete = favorites[highlightedIndex]
        if (favoriteToDelete) {
          handleDeleteFavorite(favoriteToDelete.name)
          setHighlightedIndex((prev) =>
            favorites.length === 1
              ? -1
              : prev >= favorites.length - 1
              ? prev - 1
              : prev
          )
        }
      }
    }

    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('keydown', handleKeyDown)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openDropdown, favorites, highlightedIndex])

  return (
    <div className="favorite-filter-search" ref={localRef}>
      <div className="favorite-filter-icons-ctn">
        <div
          className="favorite-filter-star-icon"
          onClick={handleToggleStar}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              handleToggleStar()
            }
          }}
          tabIndex={0}
        >
          {matchingFavorite ? <StarFillIcon /> : <StarIcon />}
        </div>
        <div className="favorite-filter-chevron-ctn">
          {favorites.length > 0 && (
            <div
              className="favorite-filter-chevron-icon"
              onClick={() =>
                handleDropdownToggle(
                  'favoritesDropdown',
                  openDropdown !== 'favoritesDropdown'
                )
              }
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  handleDropdownToggle(
                    'favoritesDropdown',
                    openDropdown !== 'favoritesDropdown'
                  )
                }
              }}
              tabIndex={0}
            >
              {openDropdown === 'favoritesDropdown' ? (
                <ChevronUp />
              ) : (
                <ChevronDown />
              )}
            </div>
          )}
        </div>
      </div>

      {openDropdown === 'favoritesDropdown' && (
        <ul className="favorites-dropdown">
          {favorites.map((fav, index) => (
            <div
              key={fav.name}
              className={`favorite-item`}
              style={
                index === highlightedIndex
                  ? { backgroundColor: 'lightgrey' }
                  : {}
              }
            >
              <li
                className="favorite-item-name"
                onClick={() => handleSelectFavorite(fav.state)}
              >
                {fav.name}
              </li>
              <div
                className="favorite-cross-icon-container"
                onClick={() => handleDeleteFavorite(fav.name)}
              >
                <CrossIcon />
              </div>
            </div>
          ))}
        </ul>
      )}

      {openDropdown === 'addFavoriteDropdown' && (
        <div
          className="favorite-name-input-dropdown"
          ref={addFavoriteNameDropdown}
        >
          <div style={{ display: 'flex' }}>
            <input
              type="text"
              placeholder="Name this favorite..."
              value={favoriteName}
              onChange={(e) => {
                const newName = e.target.value.trim()
                setFavoriteName(newName)
                setIsNameTaken(favorites.some((fav) => fav.name === newName))
              }}
            />
          </div>
          <p className="favorite-name-validation-info">
            {isNameTaken && <span>name already exist </span>}
          </p>

          <div className="favorite-input-dropdown-buttons">
            <Button
              title="Cancel"
              onClick={() => handleDropdownToggle('addFavoriteDropdown', false)}
              lightStyle={true}
            />
            <Button
              onClick={handleSaveFavorite}
              title="Add"
              disabled={!favoriteName.trim() || isNameTaken}
            />
          </div>
        </div>
      )}
    </div>
  )
}

const WrappedFavoriteFilterSearch = forwardRef(FavoriteFilterSearch) as <T>(
  props: FavoriteFilterSearchProps<T> & { ref?: React.Ref<HTMLDivElement> }
) => JSX.Element

export default WrappedFavoriteFilterSearch
