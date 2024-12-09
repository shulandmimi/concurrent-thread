use std::{
    cell::UnsafeCell,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

use tracing::error;

use crate::latch::Latch;

trait Execute {
    unsafe fn execute(this: *const ());
}

pub struct Job<F> {
    latch: Arc<Latch>,
    task: UnsafeCell<Option<F>>,
}

impl<F> Execute for Job<F>
where
    F: FnOnce() + Sync,
{
    unsafe fn execute(this: *const ()) {
        let this = this as *const Job<F>;
        let this = &*this;

        let func = (*this.task.get()).take().unwrap();

        match catch_unwind(AssertUnwindSafe(func)) {
            Ok(_) => {}
            Err(err) => {
                error!("panic in job: {:#?}", err);
            }
        };

        this.latch.set();
    }
}

impl<F> Job<F> {
    pub unsafe fn new(code: F, latch: Arc<Latch>) -> Job<F> {
        Job {
            task: UnsafeCell::new(Some(code)),
            latch,
        }
    }
}

pub struct JobRef {
    f: unsafe fn(*const ()),
    data: *const (),
}

impl JobRef {
    #[allow(private_bounds)]
    pub unsafe fn new<T: Execute>(data: &T) -> JobRef {
        JobRef {
            data: data as *const T as *const (),
            f: <T as Execute>::execute,
        }
    }

    pub unsafe fn execute(&self) {
        (self.f)(self.data);
    }
}

unsafe impl Send for JobRef {}
unsafe impl Sync for JobRef {}
