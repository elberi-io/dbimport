export function errorHandling(
  request: string,
  kind: string,
  error: string | Error | unknown
) {
  if (typeof error === 'string') {
    console.error(`Error ${request} ${kind} data:`, error)
  } else if (error instanceof Error) {
    console.error(`Error ${request} ${kind} data:`, error.message)
  } else {
    console.log('Unknown error occured')
  }
}
