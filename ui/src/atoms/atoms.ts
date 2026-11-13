import { atom, getDefaultStore } from 'jotai'
import { atomWithSessionStorage } from './utils'

export const isMainSidebarMinimized = atom(false)

export const isDbDropdownReadyAtom = atom(false)

export const isCnDropdownReadyAtom = atom(false)

export const airflowTypeAtom = atom('')

export const clearRowSelectionAtom = atom(0)

export const importPersistStateAtom = atomWithSessionStorage<string | null>(
  'importPersistState',
  null
)

export const exportPersistStateAtom = atomWithSessionStorage<string | null>(
  'exportPersistState',
  null
)

export const connectionPersistStateAtom = atomWithSessionStorage<string | null>(
  'connectionPersistState',
  null
)

export const airflowImportDagsPersistStateAtom = atomWithSessionStorage<
  string | null
>('airflowImportDagsPersistState', null)

export const airflowExportDagsPersistStateAtom = atomWithSessionStorage<
  string | null
>('airflowExportDagsPersistState', null)

export const airflowCustomDagsPersistStateAtom = atomWithSessionStorage<
  string | null
>('airflowCustomDagsPersistState', null)

export const importTableListFiltersAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('importDbListFilters', {})

export const exportCnListFiltersAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('exportCnListFilters', {})

export const connectionFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('connectionFilters', {})

export const airflowImportFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('airflowImportFilter', {})

export const airflowExportFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('airflowExportFilter', {})

export const airflowCustomFilterAtom = atomWithSessionStorage<{
  [key: string]: string[]
}>('airflowCustomFilter', {})

export const isAirflowSubmenuActiveAtom = atomWithSessionStorage<boolean>(
  'isAirflowSubmenuActive',
  false
)
export const isConfigurationSubmenuActiveAtom = atomWithSessionStorage<boolean>(
  'isConfigurationSubmenuActive',
  false
)

export const usernameAtom = atomWithSessionStorage<string | null>(
  'username',
  null
)

export const latestPathAtom = atomWithSessionStorage<string | null>(
  'latestPath',
  null
)

export const setLatestPath = (value: string) => {
  const store = getDefaultStore()
  store.set(latestPathAtom, value)
}
