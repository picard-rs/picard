# picard-cqrs

`picard-cqrs` routes commands by entity ID and creates one actor for each
active entity. Idle entity actors are stopped after a configurable interval;
the next command starts a fresh actor instance. Persistence and state recovery
are provided by the entity actor/application, not by this crate.

## Example

Add the crates to your `Cargo.toml`:

```toml
[dependencies]
picard = "0.4.2"
picard-cqrs = "0.2.3"
```

Implement the entity actor and a factory that supplies its ID:

```rust
use picard::actors::*;
use picard_cqrs::{Entity, EntityActorProps, EntityMsg};

struct AccountActor {
    id: String,
}

impl ActorFactoryArgs<String> for AccountActor {
    fn create_args(id: String) -> Self {
        Self { id }
    }
}

impl Actor for AccountActor {
    type Msg = String;

    fn recv(&mut self, _: &Context<Self::Msg>, command: String, _: Sender) {
        println!("account {} received command: {command}", self.id);
    }
}

#[derive(Clone)]
struct AccountProps;

impl EntityActorProps for AccountProps {
    type Actor = AccountActor;

    fn props(&self, id: String) -> BoxActorProd<Self::Actor> {
        Props::new_args::<AccountActor, _>(id)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::new()?;
    let accounts = Entity::new(&system, AccountProps, "accounts", None)?;

    accounts.tell(
        EntityMsg::Cmd("account-123".to_string(), "open".to_string()),
        None,
    );

    Ok(())
}
```

The entity manager creates `AccountActor` on the first command for an ID and
routes later commands for that ID to the same active actor. See the
[persistence guide](../../web/docs/persistence.md) for event-sourcing
considerations and the `sleep_after_secs` configuration.
