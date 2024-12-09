use std::sync::atomic::AtomicBool;

pub struct Latch {
    latch: AtomicBool,
}

unsafe impl Sync for Latch {}
unsafe impl Send for Latch {}

impl Latch {
    pub fn new() -> Self {
        Latch {
            latch: AtomicBool::new(false),
        }
    }

    pub fn wait(&self) {
        while !self.latch.load(std::sync::atomic::Ordering::Acquire) {
            std::hint::spin_loop()
        }
    }

    pub fn set(&self) {
        self.latch.store(true, std::sync::atomic::Ordering::Release);
    }
}
