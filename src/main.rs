use concurrent_threads::log_init;
use tracing::info;

fn main() {
    log_init();
    info!("hello world");
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fmt::Debug,
        thread::{self, sleep},
        time::Duration,
    };

    use concurrent_threads::{
        iter::vec::{IntoParallelIterator, ParallelIterator},
        join,
    };

    #[test]
    fn join_result() {
        let (r1, r2) = join(
            || 1,
            || {
                thread::sleep(Duration::from_secs(1));
                2
            },
        );

        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
    }

    #[test]
    fn nested_join() {
        join(
            || {
                println!("a");
            },
            || {
                println!("b");

                join(
                    || {
                        println!("c");
                        sleep(Duration::from_secs(1));
                    },
                    || {
                        println!("d");
                    },
                );
                join(
                    || {
                        sleep(Duration::from_secs(1));
                        println!("c");
                    },
                    || {
                        sleep(Duration::from_secs(1));
                        println!("d");
                    },
                );
                join(
                    || {
                        println!("c");
                    },
                    || {
                        println!("d");
                    },
                );
            },
        );
    }

    #[test]
    fn sort() {
        fn quick_sort<T: PartialOrd + Send + Sync + Debug>(v: &mut [T]) {
            if v.len() <= 1 {
                return;
            }

            let mid = partition(v);
            let (lo, hi) = v.split_at_mut(mid);
            join(|| quick_sort(lo), || quick_sort(hi));
        }

        fn partition<T: PartialOrd + Send + Sync>(v: &mut [T]) -> usize {
            let pivot = v.len() - 1;
            let mut i = 0;
            for j in 0..pivot {
                if v[j] <= v[pivot] {
                    v.swap(i, j);
                    i += 1;
                }
            }
            v.swap(i, pivot);
            i
        }

        let mut data: Vec<_> = (0..10 * 1024)
            .map(|_| rand::random::<u16>())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        quick_sort(&mut data);
        let mut sorted_data = data.clone();
        sorted_data.sort();

        assert_eq!(data, sorted_data);
    }

    #[test]
    fn for_each() {
        let items: Vec<_> = (0..100)
            .map(|_| rand::random::<u16>())
            .enumerate()
            .collect();

        items.into_par_iter().for_each(|item| {
            println!("{:?}", item);
        });
    }

    #[test]
    fn map() {
        let v = vec![1, 2, 3]
            .into_par_iter()
            .map(|i| i * 2)
            .map(|i| i * 2)
            .collect::<Vec<_>>();

        assert!(v == vec![4, 8, 12]);
    }
}
