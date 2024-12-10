#![allow(clippy::only_used_in_recursion)]

// 1. 适合数据结构触发的 triat (入口函数)
// 2. 入口函数实现工具函数（for_each, map）
// 3. 将数据结构转换可分割的结构（vec -> len split, string -> mid split), 适合分割的数据结构需要从数据结构本身获取

use core::slice;
use std::ptr;

use tracing::{instrument, trace};

use crate::{join, thread_data::CORE_NUMS};

impl<T: Send> IntoParallelIterator<T> for Vec<T> {
    type Item = T;

    #[instrument(skip_all)]
    fn into_par_iter(self) -> IntoIter<T> {
        IntoIter { vec: self }
    }
}

pub struct IntoIter<T> {
    vec: Vec<T>,
}

pub trait IntoParallelIterator<T>
where
    T: Send,
{
    type Item: Send;

    fn into_par_iter(self) -> IntoIter<T>;
}

impl<T: Send> ParallelIterator for IntoIter<T> {
    type Item = T;

    fn execute<OP>(mut self, op: OP)
    where
        OP: Consumer<Self::Item>,
    {
        trace!("vec parallel iterator execute");
        // 1. self => splitable data strcture
        // 2. splitable data structure => producer

        let len = self.vec.len();
        let splitable = VecSplitable {
            vec: &mut self.vec,
            len,
        };

        run(splitable, op);

        // splitable.split_at(mid)
    }
}

trait Splitable: Sized + Send {
    type Item: Send;
    type IntoIter: Iterator<Item = Self::Item>;

    fn split_at(self, mid: usize) -> (Self, Self);

    fn len(&self) -> usize;

    fn into_iter(self) -> Self::IntoIter;
}

struct SliceSplitable<'data, T> {
    slice: slice::IterMut<'data, T>,
}

impl<'data, T> Iterator for SliceSplitable<'data, T>
where
    T: 'data + Send,
{
    fn next(&mut self) -> Option<Self::Item> {
        let ptr: *const T = self.slice.next()?;

        Some(unsafe { ptr::read(ptr) })
    }

    type Item = T;
}

struct VecSplitable<'data, T>
where
    T: Send,
{
    vec: &'data mut [T],
    len: usize,
}

impl<'data, T> Splitable for VecSplitable<'data, T>
where
    T: 'data + Send,
{
    type Item = T;

    type IntoIter = SliceSplitable<'data, T>;

    fn split_at(self, mid: usize) -> (Self, Self) {
        let (left, right) = self.vec.split_at_mut(mid);

        (
            VecSplitable {
                vec: left,
                len: mid,
            },
            VecSplitable {
                vec: right,
                len: self.len - mid,
            },
        )
    }

    fn len(&self) -> usize {
        self.vec.len()
    }

    fn into_iter(mut self) -> Self::IntoIter {
        let slice = std::mem::take(&mut self.vec);

        SliceSplitable {
            slice: slice.iter_mut(),
        }
    }
}

// tools function trait
pub trait ParallelIterator: Sized {
    type Item: Send;

    #[instrument(skip_all)]
    fn for_each<OP>(self, op: OP)
    where
        OP: Fn(Self::Item) + Send + Sync,
    {
        trace!("for_each");
        for_each(self, op);
    }

    fn execute<OP>(self, op: OP)
    where
        OP: Consumer<Self::Item>;
}

#[instrument(skip_all)]
fn for_each<I, F, T>(i: I, op: F)
where
    I: ParallelIterator<Item = T>,
    F: Fn(T) + Sync,
    T: Send,
{
    let consumer = ForEachConsumer::new(&op);

    trace!("create consumer");
    i.execute(consumer);
}

pub trait Consumer<T>: Send + Sized
where
    T: Send,
{
    fn consume(self, item: T);

    fn consume_iter<I>(self, iter: I)
    where
        I: IntoIterator<Item = T>;

    fn split_at(self, index: usize) -> (Self, Self);
}

struct ForEachConsumer<'f, F> {
    op: &'f F,
}

impl<'f, T, F> Consumer<T> for ForEachConsumer<'f, F>
where
    T: Send,
    F: Fn(T) + Sync,
{
    fn consume(self, item: T) {
        (self.op)(item);
    }

    fn consume_iter<I>(self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        iter.into_iter().for_each(self.op);
    }

    fn split_at(self, _index: usize) -> (Self, Self) {
        (Self::new(self.op), self)
    }
}

impl<'f, F> ForEachConsumer<'f, F> {
    fn new(op: &'f F) -> Self {
        ForEachConsumer { op }
    }
}

#[derive(Debug, Clone, Copy)]
struct Spliter {
    len: usize,
}

impl Spliter {
    fn new() -> Self {
        Self { len: CORE_NUMS }
    }

    fn try_split(&mut self) -> bool {
        if self.len / 2 != 0 {
            self.len /= 2;
            true
        } else {
            false
        }
    }
}

fn run<T, P, C>(producer: P, consumer: C)
where
    T: Send,
    P: Splitable<Item = T>,
    C: Consumer<T>,
{
    let spliter = Spliter::new();
    let len = producer.len();

    fn helper<T: Send, P: Splitable<Item = T>, C: Consumer<T>>(
        mut spliter: Spliter,
        len: usize,
        producer: P,
        consumer: C,
    ) {
        if spliter.try_split() {
            trace!("spliter split");
            let mid = producer.len() / 2;
            let (left, right) = producer.split_at(mid);
            let (left_consumer, right_consumer) = consumer.split_at(mid);

            join(
                || {
                    helper(spliter, mid, left, left_consumer);
                },
                || {
                    helper(spliter, len - mid, right, right_consumer);
                },
            );
        } else {
            trace!("consumer len: {}", producer.len());
            consumer.consume_iter(producer.into_iter());
        }
    }

    helper(spliter, len, producer, consumer);
}
