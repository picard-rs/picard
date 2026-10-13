import React from 'react'
import {
  Wrapper,
  LayoutHeader,
  Section,
  SectionContainer,
  ContentsGrid,
  Footer,
  Pre,
  LineNo
} from '../styled'
import { withPrefix } from 'gatsby'
import LivingCityImage from '../components/Organisms/MainVisual/LivingCity.png'
import Layout from '../components/Layout'
import GlobalHeader from '../components/Molecules/GlobalHeader'
import MainVisual from '../components/Organisms/MainVisual'
import { ButtonLink } from '../components/Atoms/Button'
import Content from '../components/Molecules/Content'
import SectionHeader from '../components/Molecules/SectionHeader'
import { Copyright } from '../components/styledTypography'
import { Spacer } from '../components/styledUtils'
import Highlight, { defaultProps } from 'prism-react-renderer'
import theme from 'prism-react-renderer/themes/nightOwl'
import IconConccurent from '../components/Atoms/Icons/icon-cuncurrent.png'
import IconEventSoursing from '../components/Atoms/Icons/icon-event-sourcing.png'
import IconGeneral from '../components/Atoms/Icons/icon-general.png'
import IconModern from '../components/Atoms/Icons/icon-modern.png'
import IconModular from '../components/Atoms/Icons/icon-modular.png'
import IconSelfHeal from '../components/Atoms/Icons/icon-self-heal.png'

const exampleCode = `use picard::actors::*;

#[derive(Default)]
struct MyActor;

impl Actor for MyActor {
    type Msg = String;

  fn recv(&mut self, _ctx: &Context<Self::Msg>, msg: Self::Msg, _sender: Sender) {
    println!("received {msg}");
    }
}

fn main() {
  let sys = ActorSystem::new().unwrap();
  let actor = sys.actor_of::<MyActor>("my-actor").unwrap();

  actor.tell("Hello actor!".to_string(), None);
}
`;

const IndexPage = () => (
  <Layout>
    <Wrapper>
      <LayoutHeader>
        <GlobalHeader />
      </LayoutHeader>
      <MainVisual />
      <Section id='demo'>
        <SectionContainer>
          <Spacer size={30} />
          <SectionHeader header='Live Demo: Living City' />
          <Spacer size={16} />
          <p>
            Every car, junction, traffic light and district in this city is a Picard actor.
            Spawn thousands of vehicles, crash an actor and watch supervision restart it, or add a
            whole district at runtime. The simulation runs entirely in your browser: Picard compiled
            to WebAssembly, no server involved.
          </p>
          <Spacer size={16} />
          <a href={withPrefix('/demo/')}>
            <img
              src={LivingCityImage}
              alt='Screenshot of the Living City demo'
              style={{ width: '100%', borderRadius: 8, boxShadow: '0 8px 30px rgba(0,0,0,.25)' }}
            />
          </a>
          <Spacer size={16} />
          <ButtonLink href={withPrefix('/demo/')} primary={'1'}>Launch the demo</ButtonLink>
        </SectionContainer>
        <Spacer size={80} />
      </Section>
      <Section bg={'#F1F3F5'}>
        <SectionContainer>
          <Spacer size={30} />
          <ContentsGrid>
            <Content
              bulletSrc={IconConccurent}
              header='Actor Based Concurrency'
              body='Actors expose a message based API resulting in fast, non-blocking code execution while also eliminating race conditions to make concurrent code a breeze to write.'
            />
            <Content
              bulletSrc={IconSelfHeal}
              header='Self Healing'
              body='Build applications that isolate and recover from failures. Actors provide a Supervision Strategy that form the core of resilient application design in Picard.'
            />
            <Content
              bulletSrc={IconEventSoursing}
              header='CQRS Entity Management'
              body='The separate picard-cqrs crate routes commands by entity ID and manages active entity actors. Your application supplies any persistence and state recovery.'
            />
            <Content
              bulletSrc={IconGeneral}
              header='General Purpose'
              body='In the Cloud, Microservices, IoT, Drones, AI and more. Rust compiles to OS binaries, has no VM or GC overhead, and requires only a few megabytes of memory.'
            />
            <Content
              bulletSrc={IconModular}
              header='Modular Framework'
              body='The runtime includes actor dispatch, logging, message scheduling, and configurable mailboxes. Optional features add diagnostics, serialization, a runtime console, and remote messaging.'
            />
            <Content
              bulletSrc={IconModern}
              header='Modern Rust Design'
              body='Execute Rust Futures with ease and no unsafe code used.'
            />
          </ContentsGrid>
        </SectionContainer>
        <Spacer size={80} />
      </Section>
      <Section >
        <SectionContainer>
          <Spacer size={20} />
          <SectionHeader header='Example Code' />
          <Spacer size={24} />
          <Highlight {...defaultProps} theme={theme} code={exampleCode} language="rust">
            {({ className, style, tokens, getLineProps, getTokenProps }) => (
              <Pre className={className} style={style}>
                {tokens.map((line, i) => (
                  <div {...getLineProps({ line, key: i })}>
                    <LineNo>{i + 1}</LineNo>
                    {line.map((token, key) => <span {...getTokenProps({ token, key })} />)}
                  </div>
                ))}
              </Pre>
            )}
          </Highlight>
        </SectionContainer>
        <Spacer size={160} />
      </Section>
      <Footer>
        <Copyright>&copy; Picard {new Date().getFullYear()}</Copyright>
      </Footer>
    </Wrapper>
  </Layout>
)

export default IndexPage
