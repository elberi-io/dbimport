import ReactDOM from 'react-dom'

interface TooltipProps {
  tooltipText: string
  position: { top: number; left: number }
  zIndex?: number
}

function Tooltip({ tooltipText, position, zIndex }: TooltipProps) {
  return ReactDOM.createPortal(
    <div
      style={{
        position: 'absolute',
        top: position.top,
        left: position.left,
        zIndex: zIndex ? zIndex : 99,
        backgroundColor: '#6e6e8c',
        color: '#fcfcfc',
        padding: '5px 10px',
        borderRadius: '5px',
        fontSize: '12px',
        whiteSpace: 'nowrap',
        transform: 'translate(-50%, -100%)' // Centers horizontally and position above
      }}
    >
      {tooltipText}
    </div>,
    document.body // Renders in the body
  )
}

export default Tooltip
