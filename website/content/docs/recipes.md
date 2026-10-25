---
title: Recipes
description: Complete patterns for common screens - searchable grids, debounced search, master-detail, live totals, polling snapshots, cancellable loads and form validation.
group: Guides
order: 180
---

Each recipe is a runnable unit: the code compiles against `v1.1.0` and its assertions state the behavior. They use `?`, so imagine an enclosing function returning `Result<(), Box<dyn std::error::Error>>`.

## Searchable, sortable, paged grid

Every input is a signal, every stage is incremental, and the view binds one `ObservableList`.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
struct Employee {
    id: u32,
    name: String,
    salary: u32,
}

#[derive(Clone, Copy, PartialEq)]
enum Column {
    Name,
    Salary,
}

struct EmployeeGridViewModel {
    search: Signal<String>,
    sort: Signal<Column>,
    page: Signal<PageRequest>,
    rows: ObservableList<Employee>,
}

impl EmployeeGridViewModel {
    fn new(employees: &SourceCache<u32, Employee>) -> Result<Self> {
        let search = Signal::new(String::new());
        let sort = Signal::new(Column::Name);
        let page = Signal::new(PageRequest::new(0, 2));

        let comparers = sort.values().map(|column| -> Comparer<Employee> {
            match column {
                Column::Name => Arc::new(|a, b| a.name.cmp(&b.name)),
                Column::Salary => Arc::new(|a, b| b.salary.cmp(&a.salary)),
            }
        });

        let rows = employees
            .connect()
            .filter_with_state(search.values(), |search: &String, employee: &Employee| {
                employee.name.to_lowercase().contains(&search.to_lowercase())
            })
            .sort_keyed_dynamic(comparers)
            .page_keyed_dynamic(page.values())
            .as_observable_list()?;

        Ok(Self { search, sort, page, rows })
    }

    fn names(&self) -> Vec<String> {
        self.rows.items().unwrap_or_default().into_iter().map(|employee| employee.name).collect()
    }
}

let employees = SourceCache::new(|employee: &Employee| employee.id);
employees.edit(|cache| {
    for (id, name, salary) in [(1, "Mia", 70), (2, "Leo", 95), (3, "Ava", 80), (4, "Max", 60)] {
        cache.add_or_update(Employee { id, name: name.into(), salary });
    }
})?;

let grid = EmployeeGridViewModel::new(&employees)?;
assert_eq!(grid.names(), ["Ava", "Leo"]);

grid.sort.set(Column::Salary)?;
assert_eq!(grid.names(), ["Leo", "Ava"]);

grid.page.set(PageRequest::new(1, 2))?;
assert_eq!(grid.names(), ["Mia", "Max"]);

grid.search.set("m".into())?; // Mia, Max; page 1 is now empty
grid.page.set(PageRequest::new(0, 2))?;
assert_eq!(grid.names(), ["Mia", "Max"]);
```

When a search narrows the result, reset the page, as the last two lines do. Doing it in the search setter of a real ViewModel is a good habit.

## Debounced search box

Typing should not re-filter on every keystroke. Debounce the raw text, then drive the filter from the debounced stream. The debounced stream is a scalar `ValueStream`, which the filter accepts directly. Here a `ManualScheduler` is the clock; in production call `debounce(Duration::from_millis(300))`.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::*;
use dxn_reactive::primitives::{ManualScheduler, Scheduler};
use std::sync::Arc;
use std::time::Duration;

let names = SourceList::new(vec!["Ada", "Alan", "Grace", "Barbara"]);
let typed = Signal::new(String::new());

let clock = ManualScheduler::new();
let scheduler: Arc<dyn Scheduler> = Arc::new(clock.clone());
let settled = typed
    .values()
    .debounce_with_scheduler(Duration::from_millis(300), scheduler)
    .start_with(String::new()); // the filter needs a first state to let anything through

let results = names
    .connect()
    .filter_with_state(settled, |query: &String, name: &&str| {
        name.to_lowercase().contains(&query.to_lowercase())
    })
    .sort_by_key(|name| name.to_string())
    .as_observable_list()?;
assert_eq!(results.items()?, ["Ada", "Alan", "Barbara", "Grace"]);

for text in ["g", "gr", "gra"] {
    typed.set(text.to_string())?;
    clock.advance_by(Duration::from_millis(100)); // still typing: no re-filter
}
assert_eq!(results.items()?.len(), 4);

clock.advance_by(Duration::from_millis(300));
assert_eq!(results.items()?, ["Grace"]);
```

## Master-detail

Keep the selection as a key in a signal and derive the detail from the master collection, so the detail updates when either the selection or the selected item changes.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::Signal;

#[derive(Clone, Debug, PartialEq)]
struct Note {
    id: u32,
    title: &'static str,
    body: &'static str,
}

let notes = SourceCache::new(|note: &Note| note.id);
notes.add_or_update_range([
    Note { id: 1, title: "Groceries", body: "milk" },
    Note { id: 2, title: "Ideas", body: "reactive docs" },
])?;

let selected = Signal::new(None::<u32>);
let detail = notes
    .connect()
    .filter_with_state(selected.values(), |selected: &Option<u32>, note: &Note| *selected == Some(note.id))
    .as_observable_cache(|note| note.id)?;

assert_eq!(detail.count()?, 0); // nothing selected

selected.set(Some(2))?;
assert_eq!(detail.items()?[0].body, "reactive docs");

notes.add_or_update(Note { id: 2, title: "Ideas", body: "reactive docs, recipes" })?;
assert_eq!(detail.items()?[0].body, "reactive docs, recipes");
```

For a detail that follows a fixed key, `SourceCache::watch_optional(key)` is simpler; the filter approach also reacts to the selection changing. When the selection can change, `selected.values().switch_map(move |key| notes.watch_optional(...))` follows the key too.

## Live totals

Incremental aggregates update from each change in O(1), and `Computed::from_stream` turns them into signals a command or a label can read.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{Command, Computed};

#[derive(Clone, PartialEq)]
struct Line {
    sku: &'static str,
    quantity: u32,
    unit_cents: u64,
}

let cart = SourceCache::new(|line: &Line| line.sku);

// Seed with 0: an always-empty source never emits a count or a sum.
let items = Computed::from_stream(&cart.connect().sum_by(|line| line.quantity), 0)?;
let total = Computed::from_stream(
    &cart.connect().sum_by(|line| u64::from(line.quantity) * line.unit_cents),
    0,
)?;
let has_lines = Computed::from_stream(&cart.connect().is_not_empty(), false)?;
let checkout = Command::with_can_execute(has_lines.read_signal(), || Ok(()));

assert!(!checkout.can_execute().get()?);

cart.add_or_update(Line { sku: "apple", quantity: 3, unit_cents: 50 })?;
cart.add_or_update(Line { sku: "pear", quantity: 2, unit_cents: 80 })?;
cart.add_or_update(Line { sku: "apple", quantity: 5, unit_cents: 50 })?; // update replaces the old line

assert_eq!((items.get()?, total.get()?), (7, 410));
assert!(checkout.can_execute().get()?);

cart.clear()?;
assert_eq!(total.get()?, 0);
assert!(!checkout.can_execute().get()?);
```

## Polling snapshot with `edit_diff`

A polled endpoint returns the full list every time. Writing it with `edit_diff` turns each poll into the smallest set of changes, so unchanged rows keep identity, selection and scroll position in every bound view. Compare what subscribers see against `load`.

```rust
use dxn_reactive::collections::prelude::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
struct Job {
    id: u32,
    status: &'static str,
}

let jobs = SourceCache::new(|job: &Job| job.id);
let seen = Arc::new(Mutex::new(Vec::new()));
let sink = Arc::clone(&seen);
let _subscription = jobs
    .changes()
    .subscribe(move |batch| {
        sink.lock().unwrap().push(batch.changes.iter().map(|c| c.reason()).collect::<Vec<_>>());
    })?;

// Poll 1: everything is new.
jobs.edit_diff([Job { id: 1, status: "queued" }, Job { id: 2, status: "running" }])?;
// Poll 2: job 1 is unchanged, job 2 finished, job 3 appeared.
jobs.edit_diff([
    Job { id: 1, status: "queued" },
    Job { id: 2, status: "done" },
    Job { id: 3, status: "queued" },
])?;
// Poll 3: nothing changed - nothing is published.
let quiet = jobs.edit_diff([
    Job { id: 1, status: "queued" },
    Job { id: 2, status: "done" },
    Job { id: 3, status: "queued" },
])?;
assert!(quiet.is_empty());

assert_eq!(
    *seen.lock().unwrap(),
    [
        vec![ChangeReason::Add, ChangeReason::Add],
        vec![ChangeReason::Update, ChangeReason::Add], // job 1 untouched
    ]
);
```

Use `edit_diff_by(items, eq)` to compare only a revision number. If the snapshot arrives as a stream (a signal holding the latest poll result), the same diff is available as the `edit_diff(key)` stream adapter, shown on the [materialization page](./materialization.md#snapshot-diff-bridges).

## Async load with cancellation

A load command should show progress, replace a stale request when the user asks again, and never let a cancelled request write its result. The pattern: derive "loading" from the command (`is_executing()`), apply results with `edit_diff` only after the `await`, and cancel by **dropping the `execute()` future**, which publishes `CommandState::Cancelled` and leaves the action's remaining code unrun.

The "network" below is a oneshot channel per request so the example is deterministic; replace `reply.await` with your HTTP call.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{AsyncCommand, CommandError, CommandState};
use futures::FutureExt;
use futures::channel::oneshot;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
struct Repo {
    id: u32,
    stars: u32,
}

let repos = SourceCache::new(|repo: &Repo| repo.id);
let pending: Arc<Mutex<VecDeque<oneshot::Receiver<Vec<Repo>>>>> = Arc::default();

let load = {
    let repos = repos.clone();
    let pending = Arc::clone(&pending);
    AsyncCommand::new(move || {
        let repos = repos.clone();
        let reply = pending.lock().unwrap().pop_front();
        async move {
            let reply = reply.ok_or_else(|| CommandError::Failed("no request".into()))?;
            let snapshot = reply.await.map_err(|_| CommandError::Failed("request dropped".into()))?;
            repos.edit_diff(snapshot).map_err(|error| CommandError::Failed(error.to_string()))?;
            Ok(())
        }
    })
};

// Request 1 starts and is still waiting for its reply.
let (slow_reply, slow_receiver) = oneshot::channel();
pending.lock().unwrap().push_back(slow_receiver);
let mut first = Box::pin(load.execute());
assert!((&mut first).now_or_never().is_none());
assert!(load.is_executing().get()?); // bind a spinner to this

// The user asks again: dropping the first execution cancels it.
drop(first);
assert_eq!(load.state().get()?, CommandState::Cancelled);
assert!(!load.is_executing().get()?);
let _ = slow_reply.send(vec![Repo { id: 99, stars: 1 }]); // too late: nobody is listening

// Request 2 completes.
let (reply, receiver) = oneshot::channel();
pending.lock().unwrap().push_back(receiver);
reply.send(vec![Repo { id: 1, stars: 10 }, Repo { id: 2, stars: 7 }]).unwrap();
futures::executor::block_on(load.execute())?;

assert_eq!(repos.keys()?, [1, 2]); // the cancelled request never wrote
assert_eq!(load.state().get()?, CommandState::Succeeded);
```

To cancel on a timeout or when a signal changes, race `execute()` against a timer or `signal.changes()` future with `select!`; the loser is dropped and the command ends in `Cancelled`. Use `ExecutionPolicy::IgnoreWhileRunning` instead when a second request should be refused rather than replace the first.

## Form validation

Each field owns a `valid` signal. The form is submittable when all fields are valid.

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{Command, Computed, Signal, Subscription};
use std::sync::Arc;

struct Field {
    name: &'static str,
    text: Signal<String>,
    valid: Computed<bool>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Field {
    fn new(name: &'static str, rule: fn(&str) -> bool) -> Result<Arc<Self>> {
        let text = Signal::new(String::new());
        let valid = Computed::derive(text.read_only(), move |text| rule(text))?;
        Ok(Arc::new(Self { name, text, valid }))
    }
}

let fields = SourceCache::new(|field: &Arc<Field>| field.name);
let email = Field::new("email", |text| text.contains('@'))?;
let name = Field::new("name", |text| !text.is_empty())?;
fields.add_or_update_range([Arc::clone(&email), Arc::clone(&name)])?;

let all_valid = Signal::new(false);
let _mirror: Subscription = {
    let all_valid = all_valid.clone();
    fields
        .true_for_all_value(|field, _| field.valid.values(), |valid| *valid)
        .subscribe(move |value| {
            let _ = all_valid.set(*value);
        })?
};
let submit = Command::with_can_execute(all_valid.read_only(), || Ok(()));

assert!(!submit.can_execute().get()?);
email.text.set("ada@example.com".into())?;
name.text.set("Ada".into())?;
assert!(submit.can_execute().get()?);
```

`Computed` is not `Clone`, so the field is shared through `Arc`, which also makes the item cheap to clone into change sets.

## Bounded live feed

Keep the last three log lines, newest first:

```rust
use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::EventBus;

let log_lines = EventBus::<String>::new();

let feed = log_lines.stream().values()
    .to_list_change_set_with_options(ChangeSetAdapterOptions::limit_size_to(3))
    .reverse()
    .as_observable_list()?;

for n in 1..=5 {
    log_lines.emit(format!("line {n}"))?;
}
assert_eq!(feed.items()?, ["line 5", "line 4", "line 3"]);
```

Next: [Testing reactive code](./testing.md).
