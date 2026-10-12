use crossbeam_channel::{Receiver, Sender, unbounded};

pub fn queue<T>() -> (QueueWriter<T>, QueueReader<T>) {
    let (tx, rx) = unbounded::<T>();

    (QueueWriter { tx }, QueueReader { rx })
}

pub struct QueueWriter<T> {
    tx: Sender<T>,
}

impl<T> Clone for QueueWriter<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T> QueueWriter<T> {
    pub fn try_enqueue(&self, msg: T) -> EnqueueResult<T> {
        self.tx.send(msg).map_err(|e| EnqueueError {
            msg: e.into_inner(),
        })
    }
}

pub struct QueueReader<T> {
    rx: Receiver<T>,
}

impl<T> QueueReader<T> {
    pub fn try_dequeue(&self) -> DequeueResult<T> {
        self.rx.try_recv().map_err(|_| QueueEmpty)
    }

    pub fn has_msgs(&self) -> bool {
        !self.rx.is_empty()
    }

    #[cfg(all(feature = "console", not(target_arch = "wasm32")))]
    pub(crate) fn len(&self) -> usize {
        self.rx.len()
    }
}

#[derive(Clone, Debug)]
pub struct EnqueueError<T> {
    pub msg: T,
}

pub type EnqueueResult<T> = Result<(), EnqueueError<T>>;

pub struct QueueEmpty;
pub type DequeueResult<Msg> = Result<Msg, QueueEmpty>;
