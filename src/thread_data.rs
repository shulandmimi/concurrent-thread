use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Once},
    thread,
};

use tracing::{debug, trace};

use crate::{util::leak, JobRef, ThreadData, ThreadWoker};

static mut ROOT: Option<&'static Root> = None;
static ROOT_SET: Once = Once::new();

fn initialize() -> &'static Root {
    ROOT_SET.call_once(|| unsafe {
        let root = Root::new();
        let root = leak(root);
        ROOT = Some(root);
    });

    let root = unsafe { ROOT.unwrap() };

    root.wait_thread_created();

    root
}

#[derive(Default)]
pub struct RootState {
    pub pending_tasks_job: Mutex<VecDeque<JobRef>>,
}

pub struct Root {
    pub threads: Vec<ThreadData>,
    pub state: RootState,
}

pub const CORE_NUMS: usize = 4;

impl Root {
    fn new() -> Arc<Root> {
        let root = Arc::new(Root {
            threads: (0..CORE_NUMS).map(ThreadData::new).collect(),
            state: RootState::default(),
        });

        for index in 0..CORE_NUMS {
            let thread_root = root.clone();
            thread::spawn(move || {
                thread_loop(index, thread_root);
            });
        }

        root
    }

    pub fn current() -> &'static Root {
        initialize()
    }

    fn wait_thread_created(&self) {
        for thread in &self.threads {
            thread.wait();
            trace!("thread created: {}", thread.index);
        }
    }

    fn wait_task(&self) -> Option<JobRef> {
        loop {
            let mut pending_tasks = self.state.pending_tasks_job.lock().unwrap();

            trace!("worker {} wait a job {}", ThreadWoker::current_index(), pending_tasks.len());
            if pending_tasks.is_empty() {
                break;
            }

            if let Some(job) = pending_tasks.pop_front() {
                trace!("worker {} take a job", ThreadWoker::current_index());
                return Some(job);
            };

            thread::yield_now();
        }

        None
    }

    fn steal(&self, index: usize) -> Option<JobRef> {
        self.threads
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != index)
            .find_map(|(thread_index, thread)| {
                if let Ok(mut v) = thread.queue.try_lock() {
                    if v.is_empty() {
                        return None;
                    }

                    debug!("worker {} steal a job from worker {}", index, thread_index);

                    return v.pop_front();
                }

                None
            })
    }
}

fn thread_loop(index: usize, root: Arc<Root>) {
    let worker = ThreadWoker {
        root: root.clone(),
        index,
    };

    worker.set_current();

    root.threads[index].crated.set();

    loop {
        trace!("worker {} loop", index);
        // root.threads
        if let Some(job) = root.wait_task() {
            unsafe { job.execute() };
        } else if let Some(job) = root.steal(index) {
            unsafe { job.execute() };
        }

        thread::yield_now();
    }
}
