use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::{Envelope, LocalMessage};

pub fn queue<Msg: LocalMessage>() -> (QueueWriter<Msg>, QueueReader<Msg>) {
    let (tx, rx) = unbounded::<Envelope<Msg>>();

    (QueueWriter { tx }, QueueReader { rx })
}

pub struct QueueWriter<Msg: LocalMessage> {
    tx: Sender<Envelope<Msg>>,
}

impl<Msg: LocalMessage> Clone for QueueWriter<Msg> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<Msg: LocalMessage> QueueWriter<Msg> {
    pub fn try_enqueue(&self, msg: Envelope<Msg>) -> EnqueueResult<Msg> {
        self.tx.send(msg).map_err(|e| EnqueueError {
            msg: e.into_inner(),
        })
    }
}

pub struct QueueReader<Msg: LocalMessage> {
    rx: Receiver<Envelope<Msg>>,
}

impl<Msg: LocalMessage> QueueReader<Msg> {
    pub fn try_dequeue(&self) -> DequeueResult<Envelope<Msg>> {
        self.rx.try_recv().map_err(|_| QueueEmpty)
    }

    pub fn has_msgs(&self) -> bool {
        !self.rx.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct EnqueueError<T> {
    pub msg: T,
}

pub type EnqueueResult<Msg> = Result<(), EnqueueError<Envelope<Msg>>>;

pub struct QueueEmpty;
pub type DequeueResult<Msg> = Result<Msg, QueueEmpty>;
