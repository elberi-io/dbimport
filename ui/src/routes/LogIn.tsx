import { useEffect, useState } from 'react'
import './Login.scss'
import { errorHandling } from '../utils/errorHandling'
import { useNavigate } from 'react-router-dom'
// import DBImportIconTextLogo from '../assets/icons/DBImportIconTextLogo'
import LogoWithText from '../components/LogoWithText'
import { useAtom } from 'jotai'
import { latestPathAtom, usernameAtom } from '../atoms/atoms'
import { getCookie, setCookie } from '../utils/cookies'

interface LogInResponse {
  access_token?: string
  token_type?: string
  detail?: string
}

function LogIn() {
  const [, setUsername] = useAtom(usernameAtom)
  const [latestPath] = useAtom(latestPathAtom)

  const [loading, setLoading] = useState(false)
  const [formData, setFormData] = useState({
      username: '',
      password: ''
    }),
    [errorMessage, setErrorMessage] = useState('')
  const navigate = useNavigate()

  // For redirecting from login to / if there alredy are a tooken
  const authToken = getCookie('DBI_auth_token')
  useEffect(() => {
    if (authToken) {
      navigate('/')
    }
  }, [authToken, navigate])

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setLoading(true)

    try {
      const success = await loginUser(formData)
      if (success) {
        navigate(latestPath ? latestPath : '/')
      }
    } catch (error) {
      if (error instanceof Error) {
        setErrorMessage(error.message)
      }
    } finally {
      setLoading(false)
    }
  }

  const loginUser = async ({
    username,
    password
  }: {
    username: string
    password: string
  }): Promise<boolean> => {
    const url = '/api/oauth2/access_token'

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
          username: username,
          password: password
        }),
        mode: 'cors'
      })

      const responseData: LogInResponse = await response.json()

      if (!response.ok && responseData.detail) {
        console.error('Login failed:', responseData.detail)
        setErrorMessage(responseData.detail)
        return false
      }

      if (responseData.access_token && responseData.token_type) {
        setCookie('DBI_auth_token', responseData.access_token)
        setUsername(username)
        return true
      }
    } catch (error) {
      errorHandling('POST', 'on Log in', error)
      setErrorMessage('Failed to log in.')
      return false
    }
    return false
  }

  return (
    <div className="login-root">
      <div className="login-container">
        {/* <DBImportIconTextLogo size="big" /> */}
        <LogoWithText size="big" />

        <div className="login-form">
          <h2>Log in</h2>
          <form onSubmit={handleSubmit}>
            <div className="form-fields-container">
              <div className="input-container">
                <label
                  className={loading ? 'login-disabled-label' : ''}
                  htmlFor="username"
                >
                  Username:
                </label>
                <label
                  className={loading ? 'login-disabled-label' : ''}
                  htmlFor="password"
                >
                  Password:
                </label>
              </div>
              <div className="input-container">
                <input
                  id="username"
                  type="text"
                  autoComplete="username"
                  value={formData.username}
                  onChange={(event) => {
                    setFormData({ ...formData, username: event.target.value })
                    setErrorMessage('')
                  }}
                  disabled={loading}
                  required
                />
                <input
                  id="password"
                  type="password"
                  autoComplete="current-password"
                  value={formData.password}
                  onChange={(event) => {
                    setFormData({ ...formData, password: event.target.value })
                    setErrorMessage('')
                  }}
                  disabled={loading}
                  required
                />
              </div>
            </div>

            <button
              className="login-submit-button"
              type="submit"
              disabled={!formData.username || !formData.password || loading}
            >
              Log in
            </button>
            <div className="login-message-placeholder">
              {errorMessage && (
                <div className="login-error-message">{errorMessage}</div>
              )}
              {loading && (
                <div className="logging-in-message">Logging in...</div>
              )}
            </div>
          </form>
        </div>
      </div>
    </div>
  )
}

export default LogIn
