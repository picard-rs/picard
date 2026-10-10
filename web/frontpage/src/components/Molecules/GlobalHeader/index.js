import React from 'react'
import PicardLogo from '../../Atoms/Logo/Picard'
import { Wrapper, PicardWrapper, HeaderBrandText, Nav } from './styled'
import { ExternalLinkWithIcon } from '../../Atoms/Link/ExternalLink'
import GithubLogo from '../../Atoms/Logo/Github'
import GitterLogo from '../../Atoms/Logo/Gitter'


const GlobalHeader = () => {
  return (
    <Wrapper>
      <PicardWrapper>
        <PicardLogo white />
        <HeaderBrandText>Picard</HeaderBrandText>
      </PicardWrapper>
      <Nav>
        <ExternalLinkWithIcon secondary href="https://github.com/picard-rs/picard/" target="_blank" rel="noopener noreferrer">
          <GithubLogo size={18} /> <span>View on GitHub</span>
        </ExternalLinkWithIcon>
        <ExternalLinkWithIcon secondary href="https://gitter.im/picard-rs/Lobby" target="_blank" rel="noopener noreferrer">
          <GitterLogo size={18} /> <span>Chat on Gitter</span>
        </ExternalLinkWithIcon>
      </Nav>
    </Wrapper>
  )
}

export default GlobalHeader