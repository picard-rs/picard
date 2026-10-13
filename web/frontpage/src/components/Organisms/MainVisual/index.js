import React from 'react'
import {withPrefix} from 'gatsby'
import {Wrapper, Container, MainImage, Headline, ButtonWrapper, ListBadges} from './styled'
import Image from './MainVisual.png'
import {ButtonLink} from '../../Atoms/Button'
import Badge from '../../Atoms/Badge'

const badges = [
  {
    badgeUrl: 'https://github.com/picard-rs/picard/actions?query=workflow%3A%22Build+and+run+tests%22',
    badgeSrc: 'https://img.shields.io/github/actions/workflow/status/picard-rs/picard/build-and-test.yml?branch=main&label=build'
  },
  {
    badgeUrl: 'https://github.com/picard-rs/picard/blob/master/LICENSE',
    badgeSrc: 'https://img.shields.io/badge/license-MIT-blue.svg'
  },
  {
    badgeUrl: 'https://crates.io/crates/picard',
    badgeSrc: 'https://img.shields.io/crates/v/picard'
  },
  {
    badgeUrl: 'https://docs.rs/picard',
    badgeSrc: 'https://img.shields.io/docsrs/picard'
  }
]

const MainVisual = () => {
  return (
    <Wrapper>
      <Container>
        <MainImage src={Image} />
        <Headline>A Rust framework for building modern, concurrent and resilient applications</Headline>
        <ListBadges>
          {badges.map((badge, index) => <Badge key={index} url={badge.badgeUrl} src={badge.badgeSrc} />)}
        </ListBadges>
        <ButtonWrapper>
          <ButtonLink href={withPrefix("/docs/actors.html")} primary={"1"}>Get Started</ButtonLink>
          <ButtonLink href={withPrefix("/demo/")}>Live Demo</ButtonLink>
          <ButtonLink href={withPrefix("/docs/faq.html")}>FAQ</ButtonLink>
        </ButtonWrapper>
      </Container>
    </Wrapper>
  )
}

export default MainVisual