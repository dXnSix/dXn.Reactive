//! INT-1: error-generic streams and collection-to-scalar errors.

use std::sync::{Arc, Mutex};

use crate::collections::error::CollectionError;
use crate::collections::{ListAggregationExt, SourceList};
use crate::primitives::{ReactiveError, ValueStream};
use crate::{
    Command, CommandError, Computed, DisposeWith, Effect, ReactiveValueStreamExt, Signal,
    ViewModelActivator,
};

#[derive(Debug, PartialEq)]
struct DomainError(&'static str);

impl From<ReactiveError> for DomainError {
    fn from(_: ReactiveError) -> Self {
        Self("reactive")
    }
}

#[test]
fn map_err_and_err_into_convert_subscription_errors() {
    let failing =
        ValueStream::<u32, ReactiveError>::new(|_subscriber| Err(ReactiveError::Poisoned));

    let mapped = failing.map_err(|_| DomainError("mapped"));
    assert_eq!(mapped.subscribe(|_| {}).err(), Some(DomainError("mapped")));

    let converted: ValueStream<u32, DomainError> = failing.err_into();
    assert_eq!(
        converted.subscribe(|_| {}).err(),
        Some(DomainError("reactive"))
    );

    let signal = Signal::new(3_u32);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let converted: ValueStream<u32, CollectionError> = signal.values().err_into();
    let _subscription = converted
        .subscribe(move |value| sink.lock().unwrap().push(*value))
        .unwrap();
    signal.set(4).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![3, 4]);
}

#[test]
fn scalar_extensions_work_on_collection_streams() {
    let list = SourceList::new(vec![1, 2, 3]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);

    let counts: ValueStream<usize, CollectionError> = list.connect().count();
    let _subscription = counts
        .filter(|count| count % 2 == 0)
        .distinct_until_changed()
        .subscribe(move |count| sink.lock().unwrap().push(*count))
        .unwrap();
    list.push(4).unwrap();
    list.push(5).unwrap();
    list.push(6).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![4, 6]);
}

#[test]
fn collection_errors_convert_into_reactive_errors_and_back() {
    assert_eq!(
        ReactiveError::from(CollectionError::Poisoned),
        ReactiveError::Poisoned
    );
    let nested = ReactiveError::from(CollectionError::NestedUpdate);
    match &nested {
        ReactiveError::External(error) => {
            assert_eq!(
                error.downcast_ref::<CollectionError>(),
                Some(&CollectionError::NestedUpdate)
            );
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        nested.to_string(),
        CollectionError::NestedUpdate.to_string()
    );
    assert_eq!(
        CollectionError::from(nested),
        CollectionError::NestedUpdate,
        "the original collection error survives a round trip"
    );
    assert_eq!(
        CollectionError::from(ReactiveError::external(std::fmt::Error)),
        CollectionError::Poisoned
    );
}

#[test]
fn question_mark_carries_collection_errors_out_of_activation_callbacks() {
    let rows = SourceList::new(vec![1, 2]);
    let received = Arc::new(Mutex::new(0_usize));
    let activator = ViewModelActivator::default();
    {
        let rows = rows.clone();
        let received = Arc::clone(&received);
        activator
            .when_activated(move |bag| {
                let received = Arc::clone(&received);
                rows.connect()
                    .subscribe(move |batch| *received.lock().unwrap() += batch.changes.len())?
                    .dispose_with(bag);
                Ok(())
            })
            .unwrap();
    }
    let _token = activator.activate().unwrap();
    rows.push(3).unwrap();
    assert_eq!(*received.lock().unwrap(), 2);

    // A failing collection subscription surfaces as `External` holding the original error.
    let failing = ViewModelActivator::default();
    let result = failing.when_activated(|_bag| {
        let failed: Result<(), CollectionError> = Err(CollectionError::NestedUpdate);
        failed?;
        Ok(())
    });
    assert!(result.is_ok(), "nothing runs until activation");
    let error = failing.activate().err().unwrap();
    assert_eq!(CollectionError::from(error), CollectionError::NestedUpdate);
}

#[test]
fn effects_watch_collection_streams_directly() {
    let list = SourceList::new(vec![1]);
    let counts = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&counts);

    let effect: Result<Effect, CollectionError> =
        Effect::watch(&list.connect().count(), move |count| {
            sink.lock().unwrap().push(*count)
        });
    let effect = effect.unwrap();
    list.push(2).unwrap();
    drop(effect);
    list.push(3).unwrap();

    assert_eq!(*counts.lock().unwrap(), vec![1, 2]);
}

#[test]
fn a_collection_count_enables_a_command_through_a_computed() {
    let list = SourceList::new(Vec::<u32>::new());
    let has_items =
        Computed::from_stream(&list.connect().count().map(|count| *count > 0), false).unwrap();
    let cleared = Arc::new(Mutex::new(0));
    let command = {
        let list = list.clone();
        let cleared = Arc::clone(&cleared);
        Command::with_can_execute(has_items.read_signal(), move || {
            list.clear()
                .map_err(|error| CommandError::Failed(error.to_string()))?;
            *cleared.lock().unwrap() += 1;
            Ok(())
        })
    };

    assert!(!command.can_execute().get().unwrap());
    assert_eq!(command.execute(), Err(CommandError::Disabled));

    list.push(7).unwrap();
    assert!(command.can_execute().get().unwrap());
    command.execute().unwrap();
    assert_eq!(*cleared.lock().unwrap(), 1);
    assert!(!command.can_execute().get().unwrap());

    // Dropping the Computed stops following the stream.
    let stale = has_items.read_signal();
    drop(has_items);
    list.push(8).unwrap();
    assert!(!stale.get().unwrap());
}

#[test]
fn option_extensions_work_on_scalar_streams() {
    use crate::collections::option::OptionValueStreamExt;

    let selection = Signal::new(None::<u32>);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let values: ValueStream<u32, ReactiveError> = selection.values().value_or(|| 0);
    let _subscription = values
        .subscribe(move |value| sink.lock().unwrap().push(*value))
        .unwrap();
    selection.set(Some(5)).unwrap();
    selection.set(None).unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![0, 5, 0]);
}
