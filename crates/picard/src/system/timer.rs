use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use std::{
    sync::mpsc::{self, RecvTimeoutError},
    thread,
};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    AnyMessage, LocalMessage,
    actor::{ActorRef, BasicActorRef, Sender},
    platform::Instant,
};

#[cfg(not(target_arch = "wasm32"))]
pub type TimerRef = mpsc::Sender<Job>;
#[cfg(target_arch = "wasm32")]
pub type TimerRef = WasmTimerRef;

pub type ScheduleId = Uuid;

pub trait Timer {
    fn schedule<T, M>(
        &self,
        initial_delay: Duration,
        interval: Duration,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: LocalMessage + Into<M>,
        M: LocalMessage + Clone;

    fn schedule_once<T, M>(
        &self,
        delay: Duration,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: LocalMessage + Into<M>,
        M: LocalMessage;

    fn schedule_at_time<T, M>(
        &self,
        time: DateTime<Utc>,
        receiver: ActorRef<M>,
        sender: Sender,
        msg: T,
    ) -> ScheduleId
    where
        T: LocalMessage + Into<M>,
        M: LocalMessage;

    fn cancel_schedule(&self, id: Uuid);
}

pub enum Job {
    Once(OnceJob),
    Repeat(RepeatJob),
    Cancel(Uuid),
}

pub struct OnceJob {
    pub id: Uuid,
    pub send_at: Instant,
    pub receiver: BasicActorRef,
    pub sender: Sender,
    pub msg: AnyMessage,
}

impl OnceJob {
    pub fn send(mut self) {
        let _ = self.receiver.try_tell_any(&mut self.msg, self.sender);
    }
}

pub struct RepeatJob {
    pub id: Uuid,
    pub send_at: Instant,
    pub interval: Duration,
    pub receiver: BasicActorRef,
    pub sender: Sender,
    pub msg: AnyMessage,
}

impl RepeatJob {
    pub fn send(&mut self) {
        let _ = self
            .receiver
            .try_tell_any(&mut self.msg, self.sender.clone());
    }
}

// Default timer implementation

enum Scheduled {
    Once(OnceJob),
    Repeat(RepeatJob),
}

impl Scheduled {
    fn send_at(&self) -> Instant {
        match self {
            Self::Once(job) => job.send_at,
            Self::Repeat(job) => job.send_at,
        }
    }
}

/// Timer that sleeps until the next deadline instead of polling.
///
/// The worker thread exits once every `TimerRef` has been dropped.
#[derive(Default)]
pub struct BasicTimer {
    jobs: HashMap<Uuid, Scheduled>,
    // Min-heap of deadlines. Cancelled or rescheduled jobs leave stale entries
    // behind, which are skipped when their deadline is popped.
    deadlines: BinaryHeap<Reverse<(Instant, Uuid)>>,
}

impl BasicTimer {
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn start() -> TimerRef {
        let mut process = Self::default();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            loop {
                process.run_due();

                let job = match process.next_deadline() {
                    Some(deadline) => {
                        match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                            Ok(job) => job,
                            Err(RecvTimeoutError::Timeout) => continue,
                            Err(RecvTimeoutError::Disconnected) => break,
                        }
                    }
                    None => match rx.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    },
                };

                process.handle(job);
                while let Ok(job) = rx.try_recv() {
                    process.handle(job);
                }
            }
        });

        tx
    }

    #[cfg(target_arch = "wasm32")]
    #[must_use]
    pub fn start() -> TimerRef {
        WasmTimerRef::default()
    }

    fn handle(&mut self, job: Job) {
        match job {
            Job::Cancel(id) => self.cancel(&id),
            Job::Once(job) => self.schedule_once(job),
            Job::Repeat(job) => self.schedule_repeat(job),
        }
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        while let Some(Reverse((at, id))) = self.deadlines.peek().copied() {
            if self.jobs.get(&id).is_some_and(|job| job.send_at() == at) {
                return Some(at);
            }
            self.deadlines.pop();
        }
        None
    }

    /// Sends every job whose deadline has been reached.
    pub fn run_due(&mut self) {
        let now = Instant::now();
        while let Some(at) = self.next_deadline() {
            if at > now {
                break;
            }
            let Some(Reverse((_, id))) = self.deadlines.pop() else {
                break;
            };
            match self.jobs.remove(&id) {
                Some(Scheduled::Once(job)) => job.send(),
                Some(Scheduled::Repeat(mut job)) => {
                    job.send();
                    job.send_at = now + job.interval;
                    self.insert(Scheduled::Repeat(job));
                }
                None => {}
            }
        }
    }

    fn insert(&mut self, job: Scheduled) {
        let (id, at) = match &job {
            Scheduled::Once(j) => (j.id, j.send_at),
            Scheduled::Repeat(j) => (j.id, j.send_at),
        };
        self.deadlines.push(Reverse((at, id)));
        self.jobs.insert(id, job);
    }

    pub fn cancel(&mut self, id: &Uuid) {
        self.jobs.remove(id);
    }

    pub fn schedule_once(&mut self, job: OnceJob) {
        self.insert(Scheduled::Once(job));
    }

    pub fn schedule_repeat(&mut self, job: RepeatJob) {
        self.insert(Scheduled::Repeat(job));
    }
}

/// Timer for targets without threads: a host timeout is armed for the earliest deadline.
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Default)]
pub struct WasmTimerRef {
    state: std::sync::Arc<std::sync::Mutex<WasmTimerState>>,
}

#[cfg(target_arch = "wasm32")]
#[derive(Default)]
struct WasmTimerState {
    timer: BasicTimer,
    armed: Option<Instant>,
}

#[cfg(target_arch = "wasm32")]
impl WasmTimerRef {
    #[allow(clippy::unnecessary_wraps)]
    pub fn send(&self, job: Job) -> Result<(), std::convert::Infallible> {
        let mut state = self.state.lock().unwrap();
        state.timer.handle(job);
        Self::arm(&self.state, &mut state);
        Ok(())
    }

    fn arm(shared: &std::sync::Arc<std::sync::Mutex<WasmTimerState>>, state: &mut WasmTimerState) {
        let Some(deadline) = state.timer.next_deadline() else {
            return;
        };
        if state.armed.is_some_and(|armed| armed <= deadline) {
            return;
        }
        state.armed = Some(deadline);

        let shared = shared.clone();
        let wait = deadline.saturating_duration_since(Instant::now());
        wasm_bindgen_futures::spawn_local(async move {
            gloo_timers::future::sleep(wait).await;
            let mut state = shared.lock().unwrap();
            state.armed = None;
            state.timer.run_due();
            Self::arm(&shared, &mut state);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{BasicTimer, OnceJob, RepeatJob};
    use crate::AnyMessage;
    use crate::actors::*;
    use std::{
        sync::mpsc::{self, Sender as ChannelSender},
        time::{Duration, Instant},
    };

    struct NotifyActor(ChannelSender<()>);

    impl ActorFactoryArgs<ChannelSender<()>> for NotifyActor {
        fn create_args(sender: ChannelSender<()>) -> Self {
            Self(sender)
        }
    }

    impl Actor for NotifyActor {
        type Msg = ();

        fn recv(&mut self, _: &Context<Self::Msg>, (): Self::Msg, _: Sender) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn overdue_repeat_job_waits_for_interval_after_first_send() {
        let system = ActorSystem::new().unwrap();
        let (tx, rx) = mpsc::channel();
        let actor = system
            .actor_of_args::<NotifyActor, _>("timer-repeat-test", tx)
            .unwrap();
        let mut timer = BasicTimer::default();

        timer.schedule_repeat(RepeatJob {
            id: uuid::Uuid::new_v4(),
            send_at: Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
            interval: Duration::from_secs(30),
            receiver: actor.into(),
            sender: None,
            msg: AnyMessage::new_repeatable(()),
        });

        timer.run_due();
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        timer.run_due();
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn once_jobs_fire_when_due_and_can_be_cancelled() {
        let system = ActorSystem::new().unwrap();
        let (tx, rx) = mpsc::channel();
        let actor = system
            .actor_of_args::<NotifyActor, _>("timer-once-test", tx)
            .unwrap();
        let mut timer = BasicTimer::default();
        let job = |id, delay| OnceJob {
            id,
            send_at: Instant::now() + delay,
            receiver: actor.clone().into(),
            sender: None,
            msg: AnyMessage::new(()),
        };

        let cancelled = uuid::Uuid::new_v4();
        timer.schedule_once(job(cancelled, Duration::ZERO));
        timer.schedule_once(job(uuid::Uuid::new_v4(), Duration::ZERO));
        timer.schedule_once(job(uuid::Uuid::new_v4(), Duration::from_secs(60)));
        timer.cancel(&cancelled);

        timer.run_due();
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert!(timer.next_deadline().is_some());
    }
}
