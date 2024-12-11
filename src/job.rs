use std::{
    cell::UnsafeCell,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

use crate::latch::Latch;

trait Execute {
    unsafe fn execute(this: *const ());
}

pub struct Job<F, R> {
    latch: Arc<Latch>,
    task: UnsafeCell<Option<F>>,
    result: *mut Option<R>,
}

impl<F, R> Execute for Job<F, R>
where
    F: (FnOnce() -> R) + Send,
    R: Send,
{
    unsafe fn execute(this: *const ()) {
        let this = this as *const Job<F, R>;
        let this = &*this;

        let func = (*this.task.get()).take().unwrap();

        let r = match catch_unwind(AssertUnwindSafe(func)) {
            Ok(r) => r,
            Err(err) => {
                panic!("panic in job: {:#?}", err)
            }
        };

        *this.result = Some(r);
        this.latch.set();
    }
}

impl<F, R> Job<F, R> {
    pub unsafe fn new(code: F, latch: Arc<Latch>, result: *mut Option<R>) -> Job<F, R> {
        Job {
            task: UnsafeCell::new(Some(code)),
            latch,
            result,
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
