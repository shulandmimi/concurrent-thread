use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

fn log_init() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

fn main() {
    log_init();
    info!("hello world");
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use threads::join;

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
        fn quick_sort<T: PartialOrd + Send + Sync>(v: &mut [T]) {
            if v.len() <= 1 {
                return;
            }

            let mid = partition(v);
            let (lo, hi) = v.split_at_mut(mid);
            join(
                || {
                    quick_sort(lo);
                },
                || {
                    quick_sort(hi);
                },
            );
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

        let mut data: Vec<_> = (0..6).map(|_| rand::random::<u16>()).collect();

        quick_sort(&mut data);

        let mut sorted_data = data.clone();
        sorted_data.sort();

        assert_eq!(data, sorted_data);
    }
}
