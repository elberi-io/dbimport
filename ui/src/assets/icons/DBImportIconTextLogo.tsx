import DBImportTextLogoBig from './DBImportTextLogoBig.svg'
// import DBImportTextLogoBigBiggerText from './DBImportTextLogoBigBiggerText.svg'
// import DBImportTextLogoBigBiggerText2 from './DBImportTextLogoBigBiggerText2.svg'

import DBImportTextLogoSmall from './DBImportTextLogoSmall.svg'
// import DBImportTextLogoSmallBiggerText from './DBImportTextLogoSmallBiggerText.svg'
// import DBImportTextLogoSmallBiggerText2 from './DBImportTextLogoSmallBiggerText2.svg'

interface DBImportIconTextLogoProps {
  size: 'big' | 'small'
}

export default function DBImportIconTextLogo({
  size
}: DBImportIconTextLogoProps) {
  if (size === 'big') {
    return <img src={DBImportTextLogoBig} alt="Logo" />
    // return <img src={DBImportTextLogoBigBiggerText} alt="Logo" />
    // return <img src={DBImportTextLogoBigBiggerText2} alt="Logo" />
  } else if (size === 'small') {
    return <img src={DBImportTextLogoSmall} alt="Logo" />
    // return <img src={DBImportTextLogoSmallBiggerText} alt="Logo" />
    // return <img src={DBImportTextLogoSmallBiggerText2} alt="Logo" />
  } else {
    return null
  }
}
