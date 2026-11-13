import { Navigate, useLocation } from 'react-router-dom'
import { useEffect } from 'react'
import { useAtom } from 'jotai'
import { latestPathAtom } from './atoms/atoms'

export const AuthWrapper = ({ children }: { children: React.ReactNode }) => {
  const location = useLocation()
  const [, setLatestPath] = useAtom(latestPathAtom)

  const isAuthenticated = () => {
    return document.cookie.includes('DBI_auth_token')
  }

  const isAuth = isAuthenticated()

  useEffect(() => {
    if (isAuth || location.pathname === '/login') {
      document.body.classList.remove('cloak')
    }
  }, [isAuth, location.pathname])

  if (!isAuth && location.pathname !== '/login') {
    const fullPath = `${location.pathname}${location.search}`
    setLatestPath(fullPath)
    return <Navigate to="/login" replace />
  }

  return <>{children}</>
}
