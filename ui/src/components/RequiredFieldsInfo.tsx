import './RequiredFieldsInfo.scss'

function RequiredFieldsInfo({
  isRequiredFieldEmpty,
  validation,
  isValidationSad,
  validationText
}: {
  isRequiredFieldEmpty: boolean
  validation?: boolean
  isValidationSad?: boolean
  validationText?: string
}) {
  return (
    <>
      {validation && (
        <p
          className={
            isValidationSad ? 'required-info' : 'disabled-required-info'
          }
        >
          {!isRequiredFieldEmpty ? validationText : ''}
        </p>
      )}
      <p
        className={
          isRequiredFieldEmpty ? 'required-info' : 'disabled-required-info'
        }
      >
        *
        <span style={{ marginLeft: 2 }}>
          = required field, needs to be filled before save
        </span>
      </p>
    </>
  )
}

export default RequiredFieldsInfo
