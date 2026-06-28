use futures::{StreamExt, channel::mpsc};
use gpux::{Context, Task};
use rayx_reactive_primitives::{Result, Subscription, ValueStream};
use std::sync::{Arc, Mutex};

pub struct GpuiBinding {
    _subscription: Subscription,
    _task: Task<()>,
}

pub trait ReactiveContextExt<T>
where
    T: 'static,
{
    fn bind_stream_value<V>(
        &mut self,
        stream: &ValueStream<V>,
        on_value: impl Fn(&mut T, &V, &mut Context<T>) + 'static,
    ) -> Result<GpuiBinding>
    where
        V: Clone + Send + Sync + 'static;

    fn notify_on<V>(&mut self, stream: &ValueStream<V>) -> Result<GpuiBinding>
    where
        V: Clone + Send + Sync + 'static;
}

impl<T> ReactiveContextExt<T> for Context<'_, T>
where
    T: 'static,
{
    fn bind_stream_value<V>(
        &mut self,
        stream: &ValueStream<V>,
        on_value: impl Fn(&mut T, &V, &mut Context<T>) + 'static,
    ) -> Result<GpuiBinding>
    where
        V: Clone + Send + Sync + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded::<V>();
        let tx = Arc::new(Mutex::new(tx));
        let tx_for_stream = Arc::clone(&tx);
        let subscription = stream.subscribe(move |value| {
            if let Ok(tx) = tx_for_stream.lock() {
                let _ = tx.unbounded_send(value.clone());
            }
        })?;

        let task = self.spawn(async move |this, cx| {
            while let Some(value) = rx.next().await {
                if this
                    .update(cx, |entity, cx| on_value(entity, &value, cx))
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(GpuiBinding {
            _subscription: subscription,
            _task: task,
        })
    }

    fn notify_on<V>(&mut self, stream: &ValueStream<V>) -> Result<GpuiBinding>
    where
        V: Clone + Send + Sync + 'static,
    {
        self.bind_stream_value(stream, |_, _, cx| cx.notify())
    }
}
