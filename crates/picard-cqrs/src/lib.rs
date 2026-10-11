use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use log::{error, trace};
use picard::actors::{
    Actor, ActorRef, ActorRefFactory, ActorSystem, BoxActorProd, Context, CreateError, Message,
    PicardConfig, Props, Sender, Tell, Timer,
};

/// Messages understood by the entity manager actor.
#[derive(Clone, Debug)]
pub enum EntityMsg<Msg: Message> {
    /// Sends a command to the entity instance with the given id,
    /// starting the instance if it is asleep.
    Cmd(String, Msg),
    Tick,
}

/// Shorthand for the message type of the actor created by an `EntityActorProps`.
pub type EntityActorMsg<Pro> = <<Pro as EntityActorProps>::Actor as Actor>::Msg;

pub trait EntityActorProps: Clone + Send + Sync {
    type Actor: Actor;

    fn props(&self, id: String) -> BoxActorProd<Self::Actor>;
}

impl<T> EntityActorProps for Arc<Mutex<T>>
where
    T: EntityActorProps,
{
    type Actor = T::Actor;

    fn props(&self, id: String) -> BoxActorProd<Self::Actor> {
        self.lock().unwrap().props(id)
    }
}

pub struct Entity;

impl Entity {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<Pro>(
        sys: &ActorSystem,
        instance_fact: Pro,
        name: &str,
        conf: Option<EntityActorConfig>,
    ) -> Result<ActorRef<EntityMsg<EntityActorMsg<Pro>>>, CreateError>
    where
        Pro: EntityActorProps + 'static,
    {
        let conf = conf.unwrap_or_else(|| EntityActorConfig::from(sys.config()));
        let props = EntityActor::props(name, instance_fact, conf);
        let actor = sys.actor_of_props(&format!("entity-{name}"), props)?;

        Ok(actor)
    }
}

struct EntityActor<Pro: EntityActorProps> {
    name: String,
    props: Pro,
    instances: HashMap<String, EntityInstance<EntityActorMsg<Pro>>>,
    sleep_after: Duration,
}

impl<Pro> EntityActor<Pro>
where
    Pro: EntityActorProps + 'static,
{
    fn props(name: &str, instance_fact: Pro, conf: EntityActorConfig) -> BoxActorProd<Self> {
        Props::new_from_args(Self::actor, (name.to_string(), instance_fact, conf))
    }

    fn actor((name, instance_fact, conf): (String, Pro, EntityActorConfig)) -> Self {
        Self {
            name,
            props: instance_fact,
            instances: HashMap::new(),
            sleep_after: Duration::from_secs(conf.sleep_after_secs),
        }
    }

    fn handle_cmd(
        &mut self,
        ctx: &Context<EntityMsg<EntityActorMsg<Pro>>>,
        id: String,
        cmd: EntityActorMsg<Pro>,
        sender: Sender,
    ) {
        if self.instances.contains_key(&id) {
            trace!(
                "CQRS: Entity: {}, ID: {}, CMD: {:?}, State: running",
                self.name, id, cmd
            );
            let entity = self.instances.get_mut(&id).unwrap();
            entity.actor.tell(cmd, sender);
            entity.last_used = SystemTime::now();
        } else {
            trace!(
                "CQRS: Entity: {}, ID: {}, CMD: {:?}, State: asleep",
                self.name, id, cmd
            );
            let actor_name = entity_actor_name(&id);
            let entity = match ctx.actor_of_props(&actor_name, self.props.props(id.clone())) {
                Ok(entity) => entity,
                Err(err) => {
                    error!(
                        "CQRS: Failed to start entity {} ({:?}): {:?}",
                        self.name, id, err
                    );
                    return;
                }
            };
            entity.tell(cmd, sender);

            let entity = EntityInstance {
                actor: entity,
                last_used: SystemTime::now(),
            };
            self.instances.insert(id, entity);
        }
    }

    fn schedule_tick(ctx: &Context<EntityMsg<EntityActorMsg<Pro>>>) {
        ctx.schedule_once(Duration::from_secs(60), ctx.myself(), None, EntityMsg::Tick);
    }

    fn sleep_instances(&mut self, ctx: &Context<EntityMsg<EntityActorMsg<Pro>>>) {
        let count = self.instances.len();
        let threshhold = SystemTime::now() - self.sleep_after;

        let (stop, keep): (Vec<_>, Vec<_>) = self
            .instances
            .drain()
            .partition(|(_, instance)| threshhold > instance.last_used);

        // stop instances
        for instance in stop {
            ctx.stop(&instance.1.actor);
        }

        // keep instances that are not due to sleep
        for instance in keep {
            self.instances.insert(instance.0, instance.1);
        }

        trace!(
            "CQRS: Number of instances put to sleep: {}",
            count - self.instances.len()
        );
    }
}

impl<Pro> Actor for EntityActor<Pro>
where
    Pro: EntityActorProps + 'static,
{
    type Msg = EntityMsg<EntityActorMsg<Pro>>;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        Self::schedule_tick(ctx);
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        match msg {
            EntityMsg::Cmd(id, cmd) => self.handle_cmd(ctx, id, cmd, sender),
            EntityMsg::Tick => {
                self.sleep_instances(ctx);
                Self::schedule_tick(ctx);
            }
        }
    }
}

fn entity_actor_name(id: &str) -> String {
    let mut name = String::from("entity-");
    for byte in id.as_bytes() {
        write!(name, "{byte:02x}").expect("writing to a String cannot fail");
    }
    name
}

struct EntityInstance<Msg: Message> {
    last_used: SystemTime,
    actor: ActorRef<Msg>,
}

#[derive(Clone, Debug)]
pub struct EntityActorConfig {
    sleep_after_secs: u64,
}

impl From<&PicardConfig> for EntityActorConfig {
    fn from(config: &PicardConfig) -> Self {
        Self {
            sleep_after_secs: config.cqrs.sleep_after_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use picard::actors::*;
    use std::{thread, time};

    use crate::{Entity, EntityActorProps, EntityMsg};

    #[derive(Clone, Debug)]
    pub enum TestMsg {
        CreateAccountCmd(String),
        AddAmountCmd(i32),
        Notify(std::sync::mpsc::Sender<()>),
    }

    #[derive(Clone, Debug)]
    pub struct BankAccount {
        id: String,
        name: String,
        balance: i32,
    }

    pub struct BankAccountActor {
        id: String,
        state: Option<BankAccount>,
    }

    impl BankAccountActor {
        pub fn new(id: String) -> Self {
            Self { id, state: None }
        }
    }

    impl Actor for BankAccountActor {
        type Msg = TestMsg;

        fn recv(&mut self, _: &Context<TestMsg>, msg: TestMsg, _: Sender) {
            match (&mut self.state, msg) {
                (_, TestMsg::Notify(tx)) => {
                    let _ = tx.send(());
                }
                (None, TestMsg::CreateAccountCmd(name)) => {
                    self.state = Some(BankAccount {
                        id: self.id.clone(),
                        name,
                        balance: 0,
                    });
                }
                (None, _) => println!("Can't update a non-existing account"),
                (Some(account), TestMsg::AddAmountCmd(amount)) => {
                    account.balance += amount;
                    println!(
                        "Account {} ({}) balance {}",
                        account.id, account.name, account.balance
                    );
                }
                (Some(_), _) => {}
            }
        }
    }

    #[derive(Clone)]
    pub struct BankAccountActorFact;

    impl EntityActorProps for BankAccountActorFact {
        type Actor = BankAccountActor;

        fn props(&self, id: String) -> BoxActorProd<Self::Actor> {
            Props::new_from_args(BankAccountActor::new, id)
        }
    }

    #[test]
    fn cqrs() {
        let system = ActorSystem::new().unwrap();

        let em = Entity::new(&system, BankAccountActorFact, "BankAccont", None).unwrap();

        let ids = ["12345678", "87654321", "65555555"];
        for id in &ids {
            em.tell(
                EntityMsg::Cmd(id.to_string(), TestMsg::CreateAccountCmd("Name".into())),
                None,
            );
        }
        for id in &ids {
            for _ in 0..3 {
                em.tell(
                    EntityMsg::Cmd(id.to_string(), TestMsg::AddAmountCmd(100)),
                    None,
                );
            }
        }

        thread::sleep(time::Duration::from_secs(1));
    }

    #[test]
    fn entity_ids_with_path_characters_are_routed_safely() {
        let system = ActorSystem::new().unwrap();
        let manager = Entity::new(&system, BankAccountActorFact, "EntityName", None).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();

        manager.tell(
            EntityMsg::Cmd("account/with/slashes".into(), TestMsg::Notify(tx)),
            None,
        );

        rx.recv_timeout(time::Duration::from_secs(2)).unwrap();
    }
}
