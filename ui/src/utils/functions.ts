import { EditSetting, ExportPKs, ImportPKs } from './interfaces'

/* eslint-disable no-useless-escape */
export function validateEmails(input: HTMLInputElement) {
  const emails = input.value.split(',').map((email) => email.trim())

  input.setCustomValidity('') // Resets custom message

  const allowedPattern = /^[\w.%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/
  const invalidCharactersPattern = /[^a-zA-Z0-9@._%+\-]/g

  for (const email of emails) {
    if (!email) continue

    if (!/@/.test(email)) {
      input.setCustomValidity("Each email must contain an '@' symbol.")
      break
    }

    if (!/\.[a-zA-Z]{2,}$/.test(email)) {
      input.setCustomValidity(
        "Each email must end with a domain (e.g., '.com')."
      )
      break
    }

    if (!allowedPattern.test(email)) {
      const invalidChars = Array.from(
        new Set(email.match(invalidCharactersPattern))
      )
      input.setCustomValidity(
        `Invalid characters found: ${invalidChars.join(
          ', '
        )}. \n Only letters, numbers, and the following symbols are allowed: \n@ . % + - _`
      )
      break
    }
  }
}

export function isValidOctal(value: string) {
  const octalPattern = /^[0-7]{3,4}$/

  return octalPattern.test(value)
}

export function getUpdatedSettingValue(
  label: string,
  updatedSettings: EditSetting[]
) {
  return updatedSettings
    .find((s) => s.label === label)
    ?.value?.toString()
    .trim()
}

export function createTrimOnBlurHandler<T>(
  setState: React.Dispatch<React.SetStateAction<T>>
) {
  return (key: keyof T) => (event: React.FocusEvent<HTMLInputElement>) => {
    const trimmedValue = event.target.value.trim()
    setState((prev) => ({
      ...prev,
      [key]: trimmedValue === '' ? null : trimmedValue
    }))
  }
}

export const wildcardMatch = (
  inputVal: string | null,
  rowVal: string
): boolean => {
  if (!inputVal) {
    // No filter applied
    return true
  }

  const hasWildcard = inputVal.includes('*')

  if (hasWildcard) {
    // Replaces '*' with '.*' for regex pattern
    const regexPattern = inputVal.replace(/\*/g, '.*')
    // Creates a regex that allows partial matches anywhere
    const regex = new RegExp(`^${regexPattern}$`, 'i')
    return regex.test(rowVal)
  } else {
    // No wildcard: exact match
    return rowVal === inputVal
  }
}

export function isImportPKs(
  primaryKeys: ImportPKs | ExportPKs,
  type: 'import' | 'export'
): primaryKeys is ImportPKs {
  return type === 'import' && !!primaryKeys
}

const ISO_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/
const twoDigits = (value: number) => String(value).padStart(2, '0')

// A time the REST API sends as ISO 8601 with offset, shown as the
// browser's local time YYYY-MM-DD HH:mm:ss; any other value as it is.
export function formatTimestamp<T>(value: T): T | string {
  if (typeof value !== 'string' || !ISO_TIME.test(value)) return value
  const date = new Date(value)
  return (
    `${date.getFullYear()}-${twoDigits(date.getMonth() + 1)}-` +
    `${twoDigits(date.getDate())} ${twoDigits(date.getHours())}:` +
    `${twoDigits(date.getMinutes())}:${twoDigits(date.getSeconds())}`
  )
}
