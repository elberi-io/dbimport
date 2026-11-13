/* eslint-disable @typescript-eslint/no-unused-vars */
import '@tanstack/react-table'

declare module '@tanstack/react-table' {
  interface ColumnMeta<TData extends RowData, TValue> {
    isSticky?: boolean
    stickyType?: 'actions' | 'links'
  }
}
