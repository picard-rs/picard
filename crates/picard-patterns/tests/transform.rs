use picard::actors::*;
use picard_patterns::transform;
use picard_patterns::transformation::Receive;
use picard_testkit::p_assert_eq;
use picard_testkit::probe::channel::{ChannelProbe, probe};
use picard_testkit::probe::{Probe, ProbeReceive};
use serde::{Deserialize, Serialize};

// NOTE:
// Transform! will be updated in the near future,
// most likely trait-based.
// This is fairly old currently.

#[derive(Clone, Debug)]
struct TestProbe(ChannelProbe<(), ProbeMsg>);

#[derive(Clone, Debug, PartialEq)]
enum ProbeMsg {
    Ok,
    Err,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum MyMsg {
    SetPassword(String),
    // password
    Authenticate(String),
    // password
}

#[allow(dead_code)]
struct UserActor {
    username: String,
    password: Option<String>,
    rec: Receive<Self, MyMsg>,

    // we need a probe for this test
    probe: TestProbe,
}

impl ActorFactoryArgs<(String, TestProbe)> for UserActor {
    fn create_args((username, probe): (String, TestProbe)) -> Self {
        Self {
            username,
            password: None,
            rec: Self::created,
            probe,
        }
    }
}

impl UserActor {
    /// Receive method for this actor when it is in a created state
    /// i.e. password has not yet been set.
    fn created(&mut self, _ctx: &Context<MyMsg>, msg: MyMsg, _sender: Sender) {
        match msg {
            MyMsg::SetPassword(passwd) => {
                self.password = Some(passwd);

                // send back a result to sender
                // e.g. `sender.try_tell(Ok, None);`

                // transform behavior to match a state where the *password is set*
                transform!(self, Self::active);
            }
            MyMsg::Authenticate(_passwd) => {
                // `MyMsg::Authenticate` is invalid since no user password
                // has been set.
                // Signal that this is an error for the current state
                self.probe.0.event(ProbeMsg::Err);
            }
        }
    }

    /// Receive method for this actor when a password has been set
    /// and the user account is now active.
    fn active(&mut self, _ctx: &Context<MyMsg>, msg: MyMsg, _sender: Sender) {
        match msg {
            MyMsg::Authenticate(_passwd) => {
                // send back an authentication result to sender
                // e.g. `sender.try_tell(Ok, None);`

                // signal that this is correct
                self.probe.0.event(ProbeMsg::Ok);
            }
            MyMsg::SetPassword(passwd) => {
                // set a new password
                self.password = Some(passwd);
            }
        }
    }
}

impl Actor for UserActor {
    type Msg = MyMsg;

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        // just call the currently set function
        (self.rec)(self, ctx, msg, sender);
    }
}

#[test]
fn transform() {
    let sys = ActorSystem::new().unwrap();

    // set up probe
    let (probe, listen) = probe();
    let actor = sys
        .actor_of_args::<UserActor, _>("trans", ("user123".into(), TestProbe(probe)))
        .unwrap();

    actor.tell(MyMsg::SetPassword("password123".into()), None);
    actor.tell(MyMsg::Authenticate("password123".into()), None);

    p_assert_eq!(listen, ProbeMsg::Ok);
}

#[test]
fn transform_incorrect() {
    let sys = ActorSystem::new().unwrap();

    // set up probe
    let (probe, listen) = probe();
    let actor = sys
        .actor_of_args::<UserActor, _>("trans", ("user123".into(), TestProbe(probe)))
        .unwrap();

    // in this version we'll try to authenticate the user
    // before a password as been set
    actor.tell(MyMsg::Authenticate("password123".into()), None);

    // we should receive Err.
    p_assert_eq!(listen, ProbeMsg::Err);
}
