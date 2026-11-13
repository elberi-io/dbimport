import React, { useMemo, useCallback, useState, useEffect, useRef } from 'react'
import Dropdown from '../components/Dropdown'
import InfoText from '../components/InfoText'
import { BulkField } from './interfaces'
import { SettingType } from './enums'
import './TableInputFields.scss'
import { nameDisplayMappings } from './nameMappings'
import { getAllTimezones } from 'countries-and-timezones'

export interface BulkInputFieldsProps<T> {
  fields: BulkField<T>[]
  selectedRows: T[]
  bulkChanges: Partial<Record<keyof T, string | number | boolean | null>>
  onBulkChange: (
    fieldKey: keyof T,
    newValue: string | number | boolean | null | undefined
  ) => void
  disabled?: boolean
}

function BulkInputFields<T>({
  fields,
  selectedRows,
  bulkChanges,
  onBulkChange,
  disabled = false
}: BulkInputFieldsProps<T>) {
  const [openDropdown, setOpenDropdown] = useState<string | null>(null)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)

  const initialFieldValues = useRef<Record<string, T[keyof T] | undefined>>({})

  useEffect(() => {
    if (Object.keys(initialFieldValues.current).length === 0) {
      const initialValues = fields.reduce((acc, field) => {
        const values = selectedRows.map((row) => row[field.key as keyof T])

        const firstValue = values[0]
        const allSame = values.every((val) => val === firstValue)
        acc[field.key as string] = allSame ? firstValue : undefined
        return acc
      }, {} as Record<string, T[keyof T] | undefined>)

      initialFieldValues.current = initialValues
    }
  }, [fields, selectedRows])

  const handleInputChange = useCallback(
    (fieldKey: keyof T, newValue: string | number | boolean | null) => {
      const initialValue = initialFieldValues.current[fieldKey as string]
      if (newValue === initialValue) {
        onBulkChange(fieldKey, undefined) // Undefined signals removal of key in bulkChanges
      } else {
        onBulkChange(fieldKey, newValue)
      }
    },
    [onBulkChange]
  )

  const fieldValues = useMemo(() => {
    return fields.map((field) => {
      const values = selectedRows.map((row) => {
        if (field.key in bulkChanges) {
          return bulkChanges[field.key as keyof T] as T[keyof T]
        }
        return row[field.key as keyof T]
      })

      const firstValue = values[0]
      const allSame = values.every((val) => val === firstValue)
      return {
        ...field,
        allSame,
        commonValue: allSame ? firstValue : undefined
      }
    })
  }, [fields, selectedRows, bulkChanges])

  const handleDropdownToggle = (dropdownId: string, isOpen: boolean) => {
    if (isOpen) {
      setOpenDropdown(dropdownId)
    } else if (openDropdown === dropdownId) {
      setOpenDropdown(null)
    }
  }

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.maxHeight = `${textareaRef.current.scrollHeight}px`
    }
  }, [])

  return (
    <div className="bulk-input-fields-container">
      {fieldValues.map((field, index) => {
        const {
          key,
          label,
          type,
          enumOptions,
          infoText,
          allSame,
          commonValue,
          isRequired
        } = field

        const keyOfT = key as keyof T

        const currentValue =
          keyOfT in bulkChanges
            ? bulkChanges[keyOfT]
            : allSame
            ? commonValue
            : undefined

        const isChanged = keyOfT in bulkChanges

        switch (type) {
          case SettingType.Enum: {
            const dropdownOptions = enumOptions
              ? Object.values(enumOptions)
              : []

            const dropdownId = `dropdown-${String(field.key)}`

            const currentTitle =
              currentValue != null
                ? nameDisplayMappings[String(key)]?.[String(currentValue)] ??
                  String(currentValue)
                : 'Select...'

            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <Dropdown
                  keyLabel={label}
                  items={dropdownOptions}
                  onSelect={(item) => handleInputChange(keyOfT, item)}
                  isOpen={openDropdown === dropdownId}
                  onToggle={(isOpen: boolean) =>
                    handleDropdownToggle(dropdownId, isOpen)
                  }
                  searchFilter={false}
                  initialTitle={currentTitle}
                  backgroundColor="inherit"
                  textColor="black"
                  fontSize="14px"
                  border="0.5px solid rgb(42,42,42)"
                  borderRadius="3px"
                  height="21.5px"
                  chevronWidth="11"
                  chevronHeight="7"
                  lightStyle={true}
                  disabled={disabled}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )
          }

          case SettingType.Text: {
            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <input
                  type="text"
                  // placeholder={!allSame ? '(multiple values)' : ''}
                  defaultValue={
                    allSame && commonValue != null ? String(commonValue) : ''
                  }
                  onChange={(event) =>
                    handleInputChange(keyOfT, event.target.value)
                  }
                  disabled={disabled}
                  required={isRequired}
                  className="bulk-input-fields-text-input"
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={430}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )
          }

          case SettingType.Textarea: {
            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label
                  className={disabled ? 'input-fields-label-disabled' : ''}
                  htmlFor={`textarea-input-${index}`}
                >
                  {label}:
                </label>
                <textarea
                  id={`textarea-input-${index}`}
                  ref={textareaRef}
                  defaultValue={
                    allSame && commonValue != null ? String(commonValue) : ''
                  }
                  className="bulk-input-fields-textarea"
                  onChange={(event) => {
                    event.target.style.maxHeight = `${event.target.scrollHeight}px`
                    handleInputChange(keyOfT, event.target.value)
                  }}
                  required={isRequired}
                  disabled={disabled}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={430}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )
          }

          case SettingType.Boolean: {
            const boolValue = allSame ? Boolean(commonValue) : null

            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <div className="radio-edit">
                  <label>
                    <input
                      type="radio"
                      name={`boolean-${String(key)}`}
                      value="true"
                      checked={boolValue === true}
                      onChange={() => handleInputChange(keyOfT, true)}
                      disabled={disabled}
                    />
                    True
                  </label>
                  <label>
                    <input
                      type="radio"
                      name={`boolean-${String(keyOfT)}`}
                      value="false"
                      checked={
                        boolValue === false &&
                        boolValue !== null &&
                        commonValue != undefined
                      }
                      onChange={() => handleInputChange(keyOfT, false)}
                      disabled={disabled}
                    />
                    False
                  </label>
                </div>
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )
          }
          case SettingType.Email:
            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                {' '}
                <label
                  className={disabled ? 'input-fields-label-disabled' : ''}
                  htmlFor={`email-input-${String(keyOfT)}`}
                >
                  {label}:
                </label>
                <input
                  className="bulk-input-fields-text-input"
                  id={`email-input-${String(keyOfT)}}`}
                  type="email"
                  pattern="^([\w.%+\-]+@[\-a-zA-Z0-9.\-]+\.[\-a-zA-Z]{2,})(, *[\w.%+\-]+@[\-a-zA-Z0-9.\-]+\.[\-a-zA-Z]{2,})*$"
                  multiple={true}
                  defaultValue={
                    allSame && commonValue != null ? String(commonValue) : ''
                  }
                  onChange={(event) =>
                    handleInputChange(keyOfT, event.target.value)
                  }
                  // onInput={(event) =>
                  //   validateEmails(event.target as HTMLInputElement)
                  // }
                  required={isRequired}
                  disabled={disabled}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )

          case SettingType.IntegerFromOneOrNull:
            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label
                  className={disabled ? 'input-fields-label-disabled' : ''}
                >
                  {label}:
                </label>
                <input
                  className="input-fields-number-input"
                  type="number"
                  defaultValue={
                    allSame && commonValue != null ? String(commonValue) : ''
                  }
                  onChange={(event) => {
                    let value: string | number | null =
                      event.target.value === ''
                        ? ''
                        : Number(event.target.value)

                    if (
                      isNaN(Number(value)) &&
                      typeof value === 'number' &&
                      value < 0
                    ) {
                      value = null
                      event.target.value = ''
                    }

                    handleInputChange(
                      keyOfT,
                      value === '' || isNaN(Number(value)) ? null : value
                    )
                  }}
                  onBlur={(event) => {
                    const value = event.target.value
                    // If input is empty or not a valid number greater than 1, set to null
                    if (
                      value === '' ||
                      isNaN(Number(value)) ||
                      Number(value) < 1
                    ) {
                      handleInputChange(keyOfT, null)
                      event.target.value = ''
                    }
                  }}
                  onKeyDown={(event) => {
                    // Prevent invalid characters from being typed
                    if (
                      ['e', 'E', '+', '-', '.', ',', 'Dead'].includes(event.key) // Dead is still working, fix so it is not
                    ) {
                      event.preventDefault()
                    }
                  }}
                  step="1"
                  disabled={disabled}
                  required={isRequired}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )

          case SettingType.Time:
            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label
                  className={disabled ? 'input-fields-label-disabled' : ''}
                >
                  {label}:
                </label>
                <input
                  className="bulk-input-fields-text-input"
                  type="time"
                  step="1"
                  defaultValue={
                    allSame && commonValue != null ? String(commonValue) : ''
                  }
                  onChange={(event) =>
                    handleInputChange(keyOfT, event.target.value)
                  }
                  disabled={disabled}
                  required={isRequired}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}
                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )

          case SettingType.TimeZone: {
            // const userTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone // Maybe use if field is required
            // console.log("User's current time zone:", userTimeZone)
            const allTimeZones = getAllTimezones({ deprecated: true })

            const timeZoneNames = Object.keys(allTimeZones)
            timeZoneNames.sort((a, b) => a.localeCompare(b))
            const dropdownId = `dropdown-${String(field.key)}`

            const currentTitle =
              currentValue != null ? String(currentValue) : 'Select...'

            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label
                  className={disabled ? 'input-fields-label-disabled' : ''}
                >
                  {label}:
                </label>
                <Dropdown
                  keyLabel={label}
                  items={timeZoneNames}
                  onSelect={(item) => handleInputChange(keyOfT, item)}
                  isOpen={openDropdown === dropdownId}
                  onToggle={(isOpen: boolean) =>
                    handleDropdownToggle(dropdownId, isOpen)
                  }
                  searchFilter={true}
                  initialTitle={currentTitle}
                  cross={true}
                  backgroundColor="inherit"
                  textColor="black"
                  fontSize="14px"
                  border="0.5px solid rgb(42, 42, 42)"
                  borderRadius="3px"
                  height="21.5px"
                  chevronWidth="11"
                  chevronHeight="7"
                  lightStyle={true}
                />
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}

                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )
          }

          case SettingType.Readonly: {
            return (
              <div
                className="bulk-input-row"
                key={`${String(keyOfT)}-${String(index)}`}
              >
                <label className={disabled ? 'label-disabled' : ''}>
                  {label}:
                </label>
                <span>
                  {allSame && commonValue !== null
                    ? String(commonValue)
                    : '(multiple values)'}
                </span>
                {infoText && (
                  <InfoText
                    label={label}
                    infoText={infoText}
                    iconPosition={{ paddingTop: 2 }}
                    infoTextMaxWidth={280}
                    isInfoTextPositionUp={false}
                  />
                )}

                <div className="indication-text-container">
                  {isChanged ? (
                    <p className="changed-text">Changed</p>
                  ) : (
                    <p className="mixed-values-text">
                      {currentValue === undefined ? 'Mixed values' : ''}
                    </p>
                  )}
                </div>
              </div>
            )
          }

          case SettingType.GroupingSpace: {
            return (
              <div
                className="setting-grouping-space"
                key={`${String(keyOfT)}-${String(index)}`}
              />
            )
          }

          default:
            return null
        }
      })}
    </div>
  )
}

export default React.memo(BulkInputFields)
