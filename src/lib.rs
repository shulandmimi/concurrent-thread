use std::{
    cell::Cell,
    collections::VecDeque,
    sync::{Arc, Mutex},
};
mod job;
mod latch;
mod thread_data;
mod util;

use job::{Job, JobRef};
use latch::Latch;
use thread_data::Root;
use tracing::{instrument, trace};

struct ThreadData {
    queue: Mutex<VecDeque<JobRef>>,
    index: usize,
    crated: Latch,
}

impl ThreadData {
    fn new(index: usize) -> Self {
        ThreadData {
            queue: Mutex::new(VecDeque::new()),
            index,
            crated: Latch::new(),
        }
    }

    fn wait(&self) {
        self.crated.wait();
    }
}

thread_local! {
    static CURRENT_THREAD_WORKER: Cell<*const ThreadWoker> = const { Cell::new(std::ptr::null::<ThreadWoker>()) };
}

struct ThreadWoker {
    root: Arc<Root>,
    index: usize,
}

impl ThreadWoker {
    fn current() -> *const ThreadWoker {
        CURRENT_THREAD_WORKER.with(|worker| worker.get())
    }

    fn current_index() -> usize {
        unsafe { (*Self::current()).index }
    }

    fn set_current(&self) {
        trace!("set current: {}", self.index);
        CURRENT_THREAD_WORKER.with(|worker| worker.set(self));
    }

    fn push(&self, job: JobRef) {
        self.root.threads[self.index]
            .queue
            .lock()
            .unwrap()
            .push_back(job);
    }

    fn pop(&self) -> Option<JobRef> {
        self.root.threads[self.index]
            .queue
            .lock()
            .unwrap()
            .pop_front()
    }
}

#[instrument(skip_all)]
pub fn join<F1, F2>(a: F1, b: F2)
where
    F1: FnOnce() + Sync,
    F2: FnOnce() + Sync,
{
    let worker = ThreadWoker::current();

    if worker.is_null() {
        trace!("worker is null");
        return inject_job(a, b);
    }

    unsafe {
        trace!("join on worker: {}", (*worker).index);

        let latch = Arc::new(Latch::new());
        let job = Job::new(a, latch.clone());
        let job_ref = JobRef::new(&job);

        let latch_b = Arc::new(Latch::new());
        let job_b = Job::new(b, latch_b.clone());
        let job_b_ref = JobRef::new(&job_b);

        (*worker).push(job_b_ref);

        // (&worker);
        job_ref.execute();

        if let Some(job) = (*worker).pop() {
            job.execute();
        }

        latch_b.wait();
    }
}

#[instrument(skip_all)]
fn inject_job<F1, F2>(a: F1, b: F2)
where
    F1: FnOnce() + Sync,
    F2: FnOnce() + Sync,
{
    unsafe {
        let root = Root::current();

        let latch = Arc::new(Latch::new());
        let job = Job::new(a, latch.clone());
        let job_a_ref = JobRef::new(&job);

        let latch_b = Arc::new(Latch::new());
        let job_b = Job::new(b, latch_b.clone());
        let job_b_ref = JobRef::new(&job_b);

        root.state
            .pending_tasks_job
            .lock()
            .unwrap()
            .extend([job_a_ref, job_b_ref]);

        latch.wait();
        latch_b.wait();
    };
}