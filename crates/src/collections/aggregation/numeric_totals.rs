/// A running total that values can be added to and removed from. `remove` returns whether
/// the total must be recomputed from the remaining values (it lost too much precision).
trait NumericTotal<N> {
    fn add(&mut self, value: N);
    fn remove(&mut self, value: N) -> bool;
    fn reset(&mut self);
}

/// Magnitude from which a removal recomputes a float total from the remaining values: from
/// 2^53 on, `f64` no longer represents every integer, so a compensated subtraction may lose
/// the small values that were added alongside.
const RECOMPUTE_MAGNITUDE: f64 = 9_007_199_254_740_992.0;

/// A compensated (Neumaier) sum of the finite values; infinities and NaN are counted apart,
/// so removing them restores the finite sum exactly.
#[derive(Clone, Copy, Debug, Default)]
struct FloatSum {
    sum: f64,
    compensation: f64,
    nan: usize,
    positive_infinite: usize,
    negative_infinite: usize,
}

impl FloatSum {
    fn accumulate(&mut self, value: f64) {
        let total = self.sum + value;
        // An overflowed sum keeps no compensation (it would turn infinity into NaN).
        if total.is_finite() {
            if self.sum.abs() >= value.abs() {
                self.compensation += (self.sum - total) + value;
            } else {
                self.compensation += (value - total) + self.sum;
            }
        }
        self.sum = total;
    }

    fn value(&self) -> f64 {
        if self.nan > 0 || (self.positive_infinite > 0 && self.negative_infinite > 0) {
            f64::NAN
        } else if self.positive_infinite > 0 {
            f64::INFINITY
        } else if self.negative_infinite > 0 {
            f64::NEG_INFINITY
        } else if !self.sum.is_finite() {
            // The finite values overflowed.
            self.sum
        } else {
            self.sum + self.compensation
        }
    }

    fn count_non_finite(&mut self, value: f64, delta: isize) -> bool {
        let counter = if value.is_nan() {
            &mut self.nan
        } else if value == f64::INFINITY {
            &mut self.positive_infinite
        } else if value == f64::NEG_INFINITY {
            &mut self.negative_infinite
        } else {
            return false;
        };
        *counter = counter.saturating_add_signed(delta);
        true
    }
}

impl NumericTotal<f64> for FloatSum {
    fn add(&mut self, value: f64) {
        if !self.count_non_finite(value, 1) {
            self.accumulate(value);
        }
    }

    fn remove(&mut self, value: f64) -> bool {
        if self.count_non_finite(value, -1) {
            return false;
        }
        // A large value, or any value leaving an overflowed sum: recompute from the rest.
        if value.abs() >= RECOMPUTE_MAGNITUDE || !self.sum.is_finite() {
            return true;
        }
        self.accumulate(-value);
        false
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// The two sums behind a standard deviation: of the values and of their squares.
#[derive(Clone, Copy, Debug, Default)]
struct StdDevTotals {
    sum: FloatSum,
    squares: FloatSum,
}

impl NumericTotal<f64> for StdDevTotals {
    fn add(&mut self, value: f64) {
        self.sum.add(value);
        self.squares.add(value * value);
    }

    fn remove(&mut self, value: f64) -> bool {
        let sum = self.sum.remove(value);
        let squares = self.squares.remove(value * value);
        sum || squares
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// The total of `sum_by`: a compensated [`FloatSum`] when `N` is `f64` or `f32`, plain
/// addition otherwise.
struct RunningTotal<N> {
    total: N,
    float: Option<FloatSum>,
}

impl<N> Default for RunningTotal<N>
where
    N: Default + 'static,
{
    fn default() -> Self {
        let float = std::any::TypeId::of::<N>();
        let is_float =
            float == std::any::TypeId::of::<f64>() || float == std::any::TypeId::of::<f32>();
        Self {
            total: N::default(),
            float: is_float.then(FloatSum::default),
        }
    }
}

impl<N> RunningTotal<N>
where
    N: Copy + Default + Add<Output = N> + Sub<Output = N> + 'static,
{
    fn as_f64(value: N) -> f64 {
        let value = &value as &dyn std::any::Any;
        value
            .downcast_ref::<f64>()
            .copied()
            .or_else(|| value.downcast_ref::<f32>().map(|value| f64::from(*value)))
            .unwrap_or_default()
    }

    fn value(&self) -> N {
        let Some(float) = &self.float else {
            return self.total;
        };
        let value = float.value();
        let boxed: Box<dyn std::any::Any> =
            if std::any::TypeId::of::<N>() == std::any::TypeId::of::<f32>() {
                Box::new(value as f32)
            } else {
                Box::new(value)
            };
        boxed.downcast::<N>().map_or(self.total, |value| *value)
    }
}

impl<N> NumericTotal<N> for RunningTotal<N>
where
    N: Copy + Default + Add<Output = N> + Sub<Output = N> + 'static,
{
    fn add(&mut self, value: N) {
        match &mut self.float {
            Some(float) => float.add(Self::as_f64(value)),
            None => self.total = self.total + value,
        }
    }

    fn remove(&mut self, value: N) -> bool {
        match &mut self.float {
            Some(float) => float.remove(Self::as_f64(value)),
            None => {
                self.total = self.total - value;
                false
            }
        }
    }

    fn reset(&mut self) {
        self.total = N::default();
        if let Some(float) = &mut self.float {
            float.reset();
        }
    }
}

/// Recomputes `total` from `values` when a removal asked for it.
fn recompute_total<N: Copy>(
    total: &mut impl NumericTotal<N>,
    recompute: bool,
    values: impl Iterator<Item = N>,
) {
    if recompute {
        total.reset();
        for value in values {
            total.add(value);
        }
    }
}

/// A total order over `PartialOrd` values: an incomparable value (NaN) sorts after every
/// other value and equals itself, so NaN handling does not depend on arrival order.
fn total_cmp<S: PartialOrd>(left: &S, right: &S) -> Ordering {
    let left_nan = left.partial_cmp(left).is_none();
    let right_nan = right.partial_cmp(right).is_none();
    match (left_nan, right_nan) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(right).unwrap_or(Ordering::Equal),
    }
}

/// Equality under [`total_cmp`]: NaN equals NaN.
fn total_eq<S: PartialOrd>(left: &S, right: &S) -> bool {
    total_cmp(left, right) == Ordering::Equal
}

/// `is_empty`/`is_not_empty`: the first count result, or `of(0)` when the source emits no
/// snapshot while subscribing (an empty source), then only distinct values.
fn emptiness(counts: ValueStream<usize>, of: fn(usize) -> bool) -> ValueStream<bool> {
    ValueStream::new(move |subscriber| {
        let (emitter, delivery) = OrderedEmitter::new(move |value: &bool| subscriber(value))?;
        let last = Arc::new(Mutex::new(None::<bool>));
        let count_last = Arc::clone(&last);
        let count_emitter = emitter.clone();
        let subscription = counts.subscribe(move |count| {
            let value = of(*count);
            count_emitter.update(&count_last, |last| {
                (*last != Some(value)).then(|| {
                    *last = Some(value);
                    value
                })
            });
        })?;
        emitter.update(&last, |last| {
            last.is_none().then(|| {
                let value = of(0);
                *last = Some(value);
                value
            })
        });
        Ok(Subscription::many(vec![subscription, delivery]))
    })
}
