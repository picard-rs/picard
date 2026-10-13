import React from 'react'
import Logo from './Picard.png'
import WhiteLogo from './PicardWhite.png'
import { Image } from './styled.js'

const Picard = ({ white }) => {
  return white ? <Image src={WhiteLogo} /> : <Image src={Logo} />
}

export default Picard
