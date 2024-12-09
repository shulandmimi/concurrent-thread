#![allow(unused)]

use std::ops::{Range, RangeBounds};

// data structure impl
struct DrainProducer<'data, T> {
    vec: &'data mut [T],
}

impl<'data, T> DrainProducer<'data, T> {
    fn new(vec: &'data mut [T]) -> Self {
        DrainProducer { vec }
    }

    // abstract impl
    fn split_at(self, mid: usize) -> (Self, Self) {
        let (left, right) = self.vec.split_at_mut(mid);

        (DrainProducer { vec: left }, DrainProducer { vec: right })
    }
}

// data structure impl
struct Drain<'data, T> {
    vec: &'data mut Vec<T>,
}

impl<'data, T> Drain<'data, T> {
    // abstract impl
    fn with_producer(self) -> DrainProducer<'data, T> {
        unsafe {
            self.vec.set_len(0);
            let producer = DrainProducer::new(self.vec);

            producer
        }
    }
}

trait ParallelDrain<'data, T> {
    fn par_drain(self) -> DrainProducer<'data, T>;
}

impl<'data, T> ParallelDrain<'data, T> for &'data mut Vec<T> {
    fn par_drain(self) -> DrainProducer<'data, T> {
        DrainProducer { vec: self }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use crate::{
        iter::{DrainProducer, ParallelDrain},
        join, log_init,
        thread_data::CORE_NUMS,
    };

    #[test]
    fn tests() {
        log_init();
        let mut items: Vec<_> = (0..100)
            .map(|_| rand::random::<u16>())
            .enumerate()
            .collect();

        let len = items.len();

        let producer = items.par_drain();

        fn split(len: &mut usize) -> bool {
            if *len / 2 != 0 {
                *len /= 2;
                true
            } else {
                false
            }
        }

        fn helper<T: Sync + Debug>(len: usize, mut split_len: usize, producer: DrainProducer<T>) {
            if split(&mut split_len) {
                let mid = len / 2;
                let (left, right) = producer.split_at(mid);
                join(
                    || {
                        helper(mid, split_len, left);
                    },
                    || {
                        helper(len - mid, split_len, right);
                    },
                );
            } else {
                // TODO: abstract, compatible

                // vec.iter => IntoIterator
                // for_each => structure.into_par_iter().for_each
                // into_par_iter: IntoIterator => IntoIter

                // for_each(iter, op);

                // iter: Splitable, Interator, Reduce
                producer.vec.iter().for_each(|item| {
                    println!("{:?}", item);
                });
            }
        }

        helper(len, CORE_NUMS, producer);
    }
}
