use std::sync::Arc;

pub fn leak<T>(v: Arc<T>) -> &'static T {
    unsafe {
        let p: *const T = &*v;

        #[allow(forgetting_copy_types)]
        std::mem::forget(p);

        &*p
    }
}
