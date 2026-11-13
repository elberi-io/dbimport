import './ListRowsInfo.scss'

interface ListRowsInfoProps<T> {
  filteredData: T[]
  contentTotalRows: string
  contentMaxReturnedRows?: string
  itemType: 'table' | 'connection' | 'DAG'
}

function ListRowsInfo<T>({
  filteredData,
  contentTotalRows,
  contentMaxReturnedRows,
  itemType
}: ListRowsInfoProps<T>) {
  const totalRows = Number(contentTotalRows)
  const maxRows = contentMaxReturnedRows ? Number(contentMaxReturnedRows) : 0
  const itemTypeString = filteredData.length === 1 ? itemType : `${itemType}s`

  return (
    <p
      className={
        contentMaxReturnedRows && totalRows > maxRows
          ? `list-rows-info list-rows-limited`
          : `list-rows-info`
      }
    >
      {contentMaxReturnedRows
        ? `Showing ${filteredData.length} ${
            totalRows > maxRows ? `(limited to ${contentMaxReturnedRows})` : ''
          } ${itemTypeString} matching the filter`
        : `Showing ${filteredData.length} ${itemTypeString} matching the filter`}
    </p>
  )
}

export default ListRowsInfo
