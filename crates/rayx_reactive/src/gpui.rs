use futures::{StreamExt, channel::mpsc};
use gpux::{Context, Task};
use rayx_reactive_primitives::{Result, Subscription, ValueStream};

pub struct GpuiBinding {
    _subscription: Subscription,
    _task: Task<()>,
}

fn subscribe_notifications<V>(
    stream: &ValueStream<V>,
    sender: mpsc::UnboundedSender<()>,
) -> Result<Subscription>
where
    V: Send + Sync + 'static,
{
    stream.subscribe(move |_| {
        let _ = sender.unbounded_send(());
    })
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
        V: Send + Sync + 'static;
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
        let subscription = stream.subscribe(move |value| {
            let _ = tx.unbounded_send(value.clone());
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
        V: Send + Sync + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded::<()>();
        let subscription = subscribe_notifications(stream, tx)?;

        let task = self.spawn(async move |this, cx| {
            while rx.next().await.is_some() {
                if this.update(cx, |_, cx| cx.notify()).is_err() {
                    break;
                }
            }
        });

        Ok(GpuiBinding {
            _subscription: subscription,
            _task: task,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use futures::executor::block_on;

    use super::*;
    use crate::EventBus;

    struct CloneCountingValue {
        clones: Arc<AtomicUsize>,
    }

    impl Clone for CloneCountingValue {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                clones: Arc::clone(&self.clones),
            }
        }
    }

    #[test]
    fn gpui_notification_subscription_does_not_clone_payloads() -> Result<()> {
        let events = EventBus::new();
        let clones = Arc::new(AtomicUsize::new(0));
        let (sender, mut receiver) = mpsc::unbounded();
        let _subscription = subscribe_notifications(&events.stream().values(), sender)?;

        events.emit(CloneCountingValue {
            clones: Arc::clone(&clones),
        })?;

        assert_eq!(clones.load(Ordering::SeqCst), 0);
        assert_eq!(block_on(receiver.next()), Some(()));
        Ok(())
    }
}
