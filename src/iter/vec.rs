#![allow(clippy::only_used_in_recursion)]

// 1. 适合数据结构触发的 triat (入口函数)
// 2. 入口函数实现工具函数（for_each, map）
// 3. 将数据结构转换可分割的结构（vec -> len split, string -> mid split), 适合分割的数据结构需要从数据结构本身获取

use core::slice;
use std::ptr;

use tracing::{instrument, trace};

use crate::{join, thread_data::CORE_NUMS};

impl<T: Send> IntoParallelIterator for Vec<T> {
    type Item = T;
    type Iter = IntoIter<T>;

    #[instrument(skip_all)]
    fn into_par_iter(self) -> IntoIter<T> {
        IntoIter { vec: self }
    }
}

pub struct IntoIter<T> {
    vec: Vec<T>,
}

pub trait IntoParallelIterator {
    type Item: Send;
    type Iter: ParallelIterator<Item = Self::Item>;

    fn into_par_iter(self) -> Self::Iter;
}

impl<T: Send> ParallelIterator for IntoIter<T> {
    type Item = T;

    fn execute<OP>(mut self, op: OP) -> OP::Output
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

        run(splitable, op)
    }

    fn len(&self) -> usize {
        self.vec.len()
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
#[allow(clippy::len_without_is_empty)]
pub trait ParallelIterator: Sized {
    type Item: Send;

    #[instrument(skip_all)]
    fn for_each<OP>(self, op: OP)
    where
        OP: Fn(Self::Item) + Send + Sync,
    {
        for_each(self, op)
    }

    #[instrument(skip_all)]
    fn map<OP>(self, op: OP) -> Map<Self, OP>
    where
        OP: Fn(Self::Item) -> Self::Item + Send + Sync,
    {
        Map { iter: self, op }
    }

    fn execute<OP>(self, op: OP) -> OP::Output
    where
        OP: Consumer<Self::Item>;

    fn collect<C>(self) -> C
    where
        C: FromParallelIterator<Self::Item>,
    {
        C::from_par_iter(self)
    }

    fn len(&self) -> usize;
}

impl<T: ParallelIterator> IntoParallelIterator for T {
    type Item = T::Item;

    type Iter = T;

    fn into_par_iter(self) -> Self::Iter {
        self
    }
}

impl<T: Send> FromParallelIterator<T> for Vec<T> {
    #[instrument(skip_all)]
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = T>,
    {
        collect(par_iter)
    }
}

#[derive(Debug, Clone, Copy)]
struct SendPtr<T>(*mut T);

unsafe impl<T> Send for SendPtr<T> {}

impl<T> SendPtr<T> {
    fn new(ptr: *mut T) -> Self {
        Self(ptr)
    }

    fn add(&self, index: usize) -> Self {
        Self((self.0 as usize + index * std::mem::size_of::<T>()) as *mut T)
    }
}

struct CollectConsumer<T>
where
    T: Send,
{
    start: SendPtr<T>,
    len: usize,
    index: usize,
}

impl<T> CollectConsumer<T>
where
    T: Send,
{
    fn new(vec: &mut Vec<T>, len: usize) -> Self {
        let start = vec.len();
        CollectConsumer {
            start: SendPtr::new(unsafe { vec.as_mut_ptr().add(start) }),
            len,
            index: 0,
        }
    }
}

struct CollectResult<T> {
    start: SendPtr<T>,
    len: usize,
}

struct CollectReducer {}

pub trait Reducer<T> {
    fn reduce(self, left: T, right: T) -> T;
}

impl CollectReducer {}

impl<T> Reducer<CollectResult<T>> for CollectReducer {
    fn reduce(self, left: CollectResult<T>, right: CollectResult<T>) -> CollectResult<T> {
        CollectResult {
            start: left.start,
            len: left.len + right.len,
        }
    }
}

impl<T> Consumer<T> for CollectConsumer<T>
where
    T: Send,
{
    type Output = CollectResult<T>;
    type Reducer = CollectReducer;

    fn consume(mut self, item: T) -> Self {
        unsafe {
            self.start.0.add(self.index).write(item);
            self.index += 1;
        }

        self
    }

    fn consume_iter<I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        for item in iter {
            unsafe {
                self.start.0.add(self.index).write(item);
                self.index += 1;
            }
        }

        self
    }

    fn complete(self) -> Self::Output {
        CollectResult {
            start: self.start,
            len: self.len,
        }
    }

    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer) {
        let len = self.len;

        let right = CollectConsumer {
            start: self.start.add(index),
            len: len - index,
            index: 0,
        };

        let left = CollectConsumer {
            start: self.start,
            len: index,
            index: 0,
        };

        (left, right, CollectReducer {})
    }
}

fn extend<I>(i: I, vec: &mut Vec<I::Item>)
where
    I: IntoParallelIterator,
{
    let iter = i.into_par_iter();

    let len = iter.len();

    vec.reserve(len);

    iter.execute(CollectConsumer::new(vec, len));

    let new_len = len + vec.len();

    unsafe {
        vec.set_len(new_len);
    };
}

trait ParallelExtend<I: IntoParallelIterator> {
    fn parallel_extend(&mut self, i: I);
}

impl<I> ParallelExtend<I> for Vec<I::Item>
where
    I: IntoParallelIterator,
{
    fn parallel_extend(&mut self, i: I) {
        extend(i, self);
    }
}

fn collect<C, I>(i: I) -> C
where
    I: IntoParallelIterator,
    C: ParallelExtend<I> + Default,
{
    let mut v = C::default();

    v.parallel_extend(i);

    v
}

pub trait FromParallelIterator<T>: Sized
where
    T: Send,
{
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = T>;
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
    type Output: Send;
    type Reducer: Reducer<Self::Output>;
    fn consume(self, item: T) -> Self;

    fn consume_iter<I>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = T>;

    fn complete(self) -> Self::Output;

    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer);
}

// map 准确来说并不是一个 consumer，它会消耗之前的数据并产生一个新的数据，并且后续由其它 consumer 消费 (for_each, collect)

// 支持链式调用，所以 map 需要返回一个 ParallelIterator 实现
// 懒执行，在被消费时才调用 execute，这时可以接受到其它 consumer 实现，MapConsumer 可以保存它并执行 execute, 在执行 map fn 时调用保存的 consumer，以此实现 map
pub struct Map<I, F> {
    iter: I,
    op: F,
}

// struct MapConsumer<'f, I, F> {
//     iter: I,
//     op: &'f F,
// }

// impl <T>Consumer<T> for  Map<> {}

impl<I, F> ParallelIterator for Map<I, F>
where
    I: ParallelIterator,
    F: Fn(I::Item) -> I::Item + Sync,
{
    type Item = I::Item;

    fn execute<OP>(self, op: OP) -> OP::Output
    where
        OP: Consumer<Self::Item>,
    {
        let consumer = MapConsumer {
            op: &self.op,
            base: op,
        };

        self.iter.execute(consumer)
    }

    fn len(&self) -> usize {
        self.iter.len()
    }
}

struct MapConsumer<'f, B, F> {
    op: &'f F,
    base: B,
}

impl<'f, C, F, T, R> Consumer<T> for MapConsumer<'f, C, F>
where
    C: Consumer<F::Output>,
    F: Fn(T) -> R + Sync,
    T: Send,
    R: Send,
{
    type Output = C::Output;

    type Reducer = C::Reducer;

    fn consume(self, item: T) -> Self {
        let v = (*self.op)(item);

        MapConsumer {
            op: self.op,
            base: self.base.consume(v),
        }
    }

    fn consume_iter<I>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        self.base = self.base.consume_iter(iter.into_iter().map(self.op));

        self
    }

    fn complete(self) -> Self::Output {
        self.base.complete()
    }

    fn split_at(self, index: usize) -> (Self, Self, Self::Reducer) {
        let (left, right, reducer) = self.base.split_at(index);

        (
            MapConsumer {
                op: self.op,
                base: left,
            },
            MapConsumer {
                op: self.op,
                base: right,
            },
            reducer,
        )
    }
}

struct ForEachConsumer<'f, F> {
    op: &'f F,
}

struct NoopReducer;

impl Reducer<()> for NoopReducer {
    fn reduce(self, _left: (), _right: ()) {}
}

impl<'f, T, F> Consumer<T> for ForEachConsumer<'f, F>
where
    T: Send,
    F: Fn(T) + Sync,
{
    type Output = ();
    type Reducer = NoopReducer;
    fn consume(self, item: T) -> Self {
        (self.op)(item);
        self
    }

    fn consume_iter<I>(self, iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        iter.into_iter().for_each(self.op);

        self
    }

    fn split_at(self, _index: usize) -> (Self, Self, Self::Reducer) {
        (Self::new(self.op), self, NoopReducer {})
    }

    fn complete(self) {}
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

fn run<T, P, C>(producer: P, consumer: C) -> C::Output
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
    ) -> C::Output {
        if spliter.try_split() {
            trace!("spliter split");
            let mid = producer.len() / 2;
            let (left, right) = producer.split_at(mid);
            let (left_consumer, right_consumer, reducer) = consumer.split_at(mid);

            let (l, r) = join(
                || helper(spliter, mid, left, left_consumer),
                || helper(spliter, len - mid, right, right_consumer),
            );

            reducer.reduce(l, r)
        } else {
            trace!("consumer len: {}", producer.len());
            consumer.consume_iter(producer.into_iter()).complete()
        }
    }

    helper(spliter, len, producer, consumer)
}
