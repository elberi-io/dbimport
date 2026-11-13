import { CSSProperties } from 'react'
import './LogoWithText.scss'
import dbimportLogo from '../assets/icons/DBImportLogo.svg'

import Middlecon from '../assets/icons/Middlecon.svg'

interface LogoWithTextProps {
  // fontSize: string
  // logoSize: string
  // textMarginTop?: string | undefined
  size: 'big' | 'small'
  noText?: boolean
}

function LogoWithText({
  // fontSize,
  // logoSize,
  // textMarginTop,
  size,
  noText = false
}: LogoWithTextProps) {
  const style: CSSProperties & { [key: string]: string } = {
    '--icon-logo-size': size === 'big' ? '100px' : '40px',
    '--title-font-size': size === 'big' ? '75px' : '30px',
    '--title-margin-top': size === 'big' ? '11.25px' : '5.5px',
    '--title-margin-left': size === 'big' ? '' : '3px',
    '--middlecon-container-margin-top': size === 'big' ? '20px' : '8px',
    '--text-font-size': size === 'big' ? '11px' : '4.7px',
    '--text-margin': size === 'big' ? '1px 7px 0 76px' : '0 3px 0 32px',
    '--middlecon-logo-size': size === 'big' ? '12px' : '5.84px'
  }

  return (
    <div className="logo-w-text" style={style}>
      <div className="logo-w-text-container">
        <img
          className="logo-w-text-logo"
          src={dbimportLogo}
          alt="dbimport_logo"
        />
        {!noText && <h1 className="logo-w-text-h1">DBImport</h1>}
      </div>
      {!noText && (
        <div className="logo-w-text-middlecon-container">
          <p>Powered by</p>
          <img
            className="logo-w-text-middlecon"
            src={Middlecon}
            alt="Middlecon_logo"
          />
        </div>
      )}
    </div>
  )
}

export default LogoWithText
