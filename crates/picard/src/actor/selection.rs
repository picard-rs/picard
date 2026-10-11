use crate::{
    Message,
    actor::{ActorReference, BasicActorRef},
    validate::{InvalidPath, validate_path},
};
use std::collections::HashSet;

/// A selection represents part of the actor heirarchy, allowing
/// messages to be sent to all actors in the selection.
///
/// There are several use cases where you would interact with actors
/// via a selection instead of actor references:
///
/// - You know the path of an actor but you don't have its `ActorRef`
/// - You want to broadcast a message to all actors within a path
///
/// `ActorRef` is almost always the better choice for actor interaction,
/// since messages are directly sent to the actor's mailbox without
/// any preprocessing or cloning.
///
/// `ActorSelection` provides flexibility for the cases where at runtime
/// the `ActorRef`s can't be known. This comes at the cost of traversing
/// part of the actor heirarchy and cloning messages.
///
/// A selection is anchored to an `ActorRef` and the path is relative
/// to that actor's path.
///
/// Sending to a selection is untyped and requires cloneable messages for
/// fan-out. Each actor is sent the message at most once, even if traversal
/// reaches it through multiple branches. [`ActorSelection::try_tell`] reports
/// how many recipient mailboxes accepted the message and returns an error
/// when there are no recipients or delivery failed for one or more recipients.
#[derive(Debug)]
pub struct ActorSelection {
    anchor: BasicActorRef,
    // dl: BasicActorRef,
    path_vec: Vec<Selection>,
}

impl ActorSelection {
    pub fn new(
        anchor: BasicActorRef,
        // dl: &BasicActorRef,
        path: String,
    ) -> Result<Self, InvalidPath> {
        validate_path(&path)?;

        let path_vec: Vec<Selection> = path
            .split_terminator('/')
            .map({
                |seg| match seg {
                    ".." => Selection::Parent,
                    "*" => Selection::AllChildren,
                    name => Selection::ChildName(name.to_string()),
                }
            })
            .collect();

        Ok(Self {
            anchor,
            // dl: dl.clone(),
            path_vec,
        })
    }

    pub fn try_tell<Msg>(
        &self,
        msg: Msg,
        sender: impl Into<Option<BasicActorRef>>,
    ) -> Result<usize, SelectionSendError>
    where
        Msg: Message + Clone,
    {
        fn walk(
            anchor: &BasicActorRef,
            path: &[Selection],
            targets: &mut Vec<BasicActorRef>,
            seen: &mut HashSet<crate::actor::ActorPath>,
        ) {
            let Some((segment, remaining)) = path.split_first() else {
                if seen.insert(anchor.path().clone()) {
                    targets.push(anchor.clone());
                }
                return;
            };

            match segment {
                Selection::Parent => walk(&anchor.parent(), remaining, targets, seen),
                Selection::AllChildren => {
                    for child in anchor.children() {
                        walk(&child, remaining, targets, seen);
                    }
                }
                Selection::ChildName(name) => {
                    if let Some(child) = anchor.children().find(|child| child.name() == name) {
                        walk(&child, remaining, targets, seen);
                    }
                }
            }
        }

        let mut targets = Vec::new();
        walk(
            &self.anchor,
            &self.path_vec,
            &mut targets,
            &mut HashSet::new(),
        );
        if targets.is_empty() {
            return Err(SelectionSendError::NoRecipients);
        }

        let sender = sender.into();
        let mut delivered = 0;
        let mut failed = 0;
        for target in targets {
            if target.try_tell(msg.clone(), sender.clone()).is_ok() {
                delivered += 1;
            } else {
                failed += 1;
            }
        }
        if failed == 0 {
            Ok(delivered)
        } else {
            Err(SelectionSendError::DeliveryFailed { delivered, failed })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionSendError {
    /// The path did not resolve to any actors.
    NoRecipients,
    /// One or more recipients rejected delivery; counts include all outcomes.
    DeliveryFailed { delivered: usize, failed: usize },
}

impl std::fmt::Display for SelectionSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoRecipients => f.write_str("actor selection matched no recipients"),
            Self::DeliveryFailed { delivered, failed } => write!(
                f,
                "actor selection delivered to {delivered} recipients and failed for {failed}"
            ),
        }
    }
}

impl std::error::Error for SelectionSendError {}

#[derive(Debug)]
enum Selection {
    Parent,
    ChildName(String),
    AllChildren,
}

pub trait ActorSelectionFactory {
    fn select(&self, path: &str) -> Result<ActorSelection, InvalidPath>;
}
