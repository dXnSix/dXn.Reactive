use futures::executor::block_on;
use rayx_reactive::prelude::*;

struct TestViewModel {
    activator: ViewModelActivator,
}

impl ActivatableViewModel for TestViewModel {
    fn activator(&self) -> &ViewModelActivator {
        &self.activator
    }
}

fn requires_reactive_context_ext<T>()
where
    for<'a> gpux::Context<'a, T>: ReactiveContextExt<T>,
    T: 'static,
{
}

fn accepts_gpui_binding(_: Option<GpuiBinding>) {}

#[test]
fn direct_public_api_remains_available() -> Result<()> {
    requires_reactive_context_ext::<u32>();
    accepts_gpui_binding(None);

    let signal = Signal::new(1_u32);
    let read: ReadSignal<u32> = signal.read_only();
    let _: SignalChange<u32> = signal.replace(2)?;
    let _ = signal.set(3)?;
    let _ = signal.update(|value| *value += 1)?;
    let _ = signal.version()?;
    let _ = signal.read(|value| *value)?;
    let _ = signal.get()?;
    let _: ReadSignal<u32> = signal.clone().into();

    let values = signal.values();
    let changes = signal.changes();
    let _values_subscription = values.subscribe(|_| {})?;
    let _changes_subscription = changes.subscribe(|_| {})?;
    let _ = read
        .values()
        .filter(|value| *value > 0)
        .filter_map(|value| Some(value + 1));
    let _ = read.values().distinct_until_changed().skip(1);
    let _ = rayx_reactive::stream_ext::empty_subscription();

    let derived = Computed::derive(read.clone(), |value| value + 1)?;
    let combined = Computed::combine2(read.clone(), read, |left, right| left + right)?;
    let _ = derived.get()?;
    let _ = derived.read_signal();
    let _ = derived.values();
    let _ = derived.changes();
    let _ = combined.get()?;

    let events = EventBus::new();
    let event_stream: EventStream<u32> = events.stream();
    let _event_subscription = event_stream.subscribe(|_| {})?;
    let _ = event_stream.values();
    let _ = event_stream.map(|value| value + 1);
    events.emit(1)?;

    let _effect = Effect::watch(&values, |_| {})?;
    let bag = DisposeBag::new();
    let disposable = Disposable::new(|| {});
    disposable.dispose();
    Subscription::empty().dispose_with(&bag);
    assert!(!bag.is_disposed());
    bag.dispose();
    assert!(bag.is_disposed());

    let view_model = TestViewModel {
        activator: ViewModelActivator::default(),
    };
    view_model.activator().when_activated(|_| Ok(()))?;
    let _token: ActivationToken = view_model.activator().activate()?;

    let _: CommandResult = Ok(());
    let _: CommandError = CommandError::Disabled;
    let _: CommandState = CommandState::Idle;
    let command = Command::new(|| Ok(()));
    let _ = command.can_execute();
    let _ = command.state();
    command.execute().map_err(|_| ReactiveError::Poisoned)?;
    let async_command =
        AsyncCommand::with_can_execute(Signal::new(true).read_only(), || async { Ok(()) });
    let _ = async_command.can_execute();
    let _ = async_command.state();
    block_on(async_command.execute()).map_err(|_| ReactiveError::Poisoned)?;

    let _queue = DeliveryQueue::<u32>::new();
    let _subscription = Subscription::empty();
    Ok(())
}
