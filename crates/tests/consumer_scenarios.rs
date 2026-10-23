//! SCN-1.1 to SCN-1.4: consumer scenarios exercised through the public API only.
//!
//! Each scenario mimics how a UI application consumes the crate (a keyed virtualized list fed by
//! a worker, a tree view mirror, foreground observers under worker writes, and small ViewModel
//! recipes) without depending on any UI toolkit. Marshalling to the "UI thread" uses
//! `std::sync::mpsc`, and every random sequence comes from a seeded in-test PRNG.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{self, AtomicBool, AtomicUsize};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Mutex, Weak};
use std::thread;

use dxn_reactive::collections::prelude::*;
use dxn_reactive::prelude::{Command, CommandError, Computed, EventBus, Signal};

/// A small deterministic xorshift64 generator, so scenarios need no external crates.
struct Prng(u64);

impl Prng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound.max(1) as u64) as usize
    }
}

// ---------------------------------------------------------------------------------------------
// SCN-1.1: keyed virtualized list
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Row {
    id: u32,
    rank: u32,
    revision: usize,
}

/// Total order of the list: by rank, ties broken by id.
fn row_order(left: &Row, right: &Row) -> Ordering {
    (left.rank, left.id).cmp(&(right.rank, right.id))
}

/// The UI-side widget model of a keyed virtualized list: rows keyed by the stream key, plus the
/// scrollbar metadata from the response.
#[derive(Default)]
struct KeyedRowsWidget {
    rows: Vec<(u32, Row)>,
    total: usize,
    start: usize,
    batches: usize,
}

impl KeyedRowsWidget {
    fn key_at(&self, index: usize) -> Option<u32> {
        self.rows.get(index).map(|(key, _)| *key)
    }

    fn apply(&mut self, update: &KeyedVirtualChangeSet<u32, Row>) {
        for change in &update.changes.changes {
            match change {
                CacheChange::Add {
                    key,
                    current,
                    current_index,
                } => {
                    let index = current_index.expect("a keyed window add carries its index");
                    assert!(
                        index <= self.rows.len(),
                        "add of {key} at {index} beyond {} rows",
                        self.rows.len()
                    );
                    self.rows.insert(index, (*key, current.clone()));
                }
                CacheChange::Update {
                    key,
                    current,
                    current_index,
                    previous_index,
                    ..
                } => {
                    let from = previous_index.expect("a keyed window update carries its index");
                    let to = current_index.expect("a keyed window update carries its index");
                    assert_eq!(self.key_at(from), Some(*key), "update previous_index");
                    self.rows.remove(from);
                    assert!(to <= self.rows.len(), "update of {key} moves to {to}");
                    self.rows.insert(to, (*key, current.clone()));
                }
                CacheChange::Remove {
                    key,
                    previous_index,
                    ..
                } => {
                    let index = previous_index.expect("a keyed window removal carries its index");
                    assert_eq!(self.key_at(index), Some(*key), "remove previous_index");
                    self.rows.remove(index);
                }
                CacheChange::Refresh {
                    key,
                    current,
                    current_index,
                } => {
                    let index = current_index.expect("a keyed window refresh carries its index");
                    assert_eq!(self.key_at(index), Some(*key), "refresh current_index");
                    self.rows[index].1 = current.clone();
                }
                CacheChange::Move {
                    key,
                    current,
                    current_index,
                    previous_index,
                } => {
                    assert_eq!(
                        self.key_at(*previous_index),
                        Some(*key),
                        "move previous_index"
                    );
                    self.rows.remove(*previous_index);
                    self.rows.insert(*current_index, (*key, current.clone()));
                }
                CacheChange::Clear { .. } => self.rows.clear(),
            }
        }
        self.total = update.response.total_count;
        self.start = update.response.start;
        self.batches += 1;
        assert_eq!(
            self.rows.len(),
            update.response.visible_count,
            "the widget row count follows the response"
        );
    }

    fn assert_consistent(&self, window: usize) {
        assert!(self.rows.len() <= window, "never more rows than the window");
        let keys: HashSet<u32> = self.rows.iter().map(|(key, _)| *key).collect();
        assert_eq!(keys.len(), self.rows.len(), "row keys are unique");
        for (key, row) in &self.rows {
            assert_eq!(*key, row.id, "each row is stored under its own key");
        }
        for pair in self.rows.windows(2) {
            assert_eq!(
                row_order(&pair[0].1, &pair[1].1),
                Ordering::Less,
                "the visible rows stay sorted"
            );
        }
    }
}

fn expected_window(source: &SourceCache<u32, Row>, request: VirtualRequest) -> Vec<(u32, Row)> {
    let mut rows = source.items().unwrap();
    rows.sort_by(row_order);
    rows.into_iter()
        .skip(request.start)
        .take(request.size)
        .map(|row| (row.id, row))
        .collect()
}

#[test]
fn scn_1_1_keyed_virtualized_list_follows_worker_edits_and_random_scrolls() -> Result<()> {
    const KEY_SPACE: usize = 160;
    const WINDOW: usize = 12;
    const WORKER_OPERATIONS: usize = 1_500;
    const SCROLLS: usize = 400;

    let source = SourceCache::new(|row: &Row| row.id);
    let mut seed = Prng::new(0x5EED_0001);
    let initial: Vec<Row> = (0..100)
        .map(|id| Row {
            id,
            rank: seed.below(50) as u32,
            revision: 0,
        })
        .collect();
    source.add_or_update_range(initial)?;

    // ViewModel: the scroll window is a signal, the binding sends each change set to the UI.
    let request = Signal::new(VirtualRequest::new(0, WINDOW));
    let (to_ui, ui_inbox) = mpsc::channel::<KeyedVirtualChangeSet<u32, Row>>();
    let binding = source
        .connect()
        .sort_keyed_by(row_order)
        .virtualize_keyed_dynamic_with_response(request.values())
        .subscribe(move |update| {
            let _ = to_ui.send(update.clone());
        })?;

    let mut widget = KeyedRowsWidget::default();
    for update in ui_inbox.try_iter() {
        widget.apply(&update);
    }
    assert_eq!(widget.rows, expected_window(&source, request.get()?));
    assert_eq!(widget.total, 100);

    let gate = Arc::new(Barrier::new(2));
    let worker = {
        let source = source.clone();
        let gate = Arc::clone(&gate);
        thread::spawn(move || -> Result<()> {
            let mut rng = Prng::new(0xC0FF_EE01);
            gate.wait();
            for step in 0..WORKER_OPERATIONS {
                let id = rng.below(KEY_SPACE) as u32;
                match rng.below(10) {
                    0..=3 => {
                        source.add_or_update(Row {
                            id,
                            rank: rng.below(50) as u32,
                            revision: step,
                        })?;
                    }
                    4 | 5 => {
                        source.remove_key(&id)?;
                    }
                    6 => {
                        let count = source.count()?;
                        source.move_key(&id, rng.below(count))?;
                    }
                    7 => {
                        source.refresh_key(&id)?;
                    }
                    _ => {
                        let edits: Vec<(u32, Option<u32>)> = (0..4)
                            .map(|_| {
                                let id = rng.below(KEY_SPACE) as u32;
                                let rank = (rng.below(3) > 0).then(|| rng.below(50) as u32);
                                (id, rank)
                            })
                            .collect();
                        source.edit(|cache| {
                            for (id, rank) in edits {
                                match rank {
                                    Some(rank) => cache.add_or_update(Row {
                                        id,
                                        rank,
                                        revision: step,
                                    }),
                                    None => {
                                        let _ = cache.remove_key(&id);
                                    }
                                }
                            }
                        })?;
                    }
                }
                if step % 64 == 0 {
                    thread::yield_now();
                }
            }
            Ok(())
        })
    };

    gate.wait();
    let mut scrolls = Prng::new(0x5C20_1101);
    for _ in 0..SCROLLS {
        // Sometimes past the end: requests are not clamped.
        let range = widget.total.max(WINDOW) + WINDOW / 2;
        request.set(VirtualRequest::new(scrolls.below(range), WINDOW))?;
        for update in ui_inbox.try_iter() {
            widget.apply(&update);
        }
        widget.assert_consistent(WINDOW);
    }

    worker.join().expect("the worker thread panicked")?;
    for update in ui_inbox.try_iter() {
        widget.apply(&update);
    }
    let last = request.get()?;
    widget.assert_consistent(WINDOW);
    assert_eq!(widget.start, last.start);
    assert_eq!(widget.rows, expected_window(&source, last));
    assert_eq!(
        widget.total,
        source.count()?,
        "the scrollbar total is the source count"
    );

    // Quiescent scrolls after the worker stopped land exactly.
    let count = source.count()?;
    for start in [0, count / 2, count.saturating_sub(WINDOW), count + 3] {
        let next = VirtualRequest::new(start, WINDOW);
        request.set(next)?;
        for update in ui_inbox.try_iter() {
            widget.apply(&update);
        }
        assert_eq!(
            widget.rows,
            expected_window(&source, next),
            "window at {start}"
        );
        assert_eq!(widget.total, count);
    }
    assert!(
        widget.batches > SCROLLS / 2,
        "the widget received live batches"
    );

    drop(binding);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// SCN-1.2: tree view mirror over transform_to_tree
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Folder {
    id: u32,
    parent: u32, // 0 = no parent
    name: &'static str,
}

fn folder(id: u32, parent: u32, name: &'static str) -> Folder {
    Folder { id, parent, name }
}

type FolderNode = TreeNode<u32, Folder>;
/// `None` is the root level, `Some(key)` the children of that node.
type Level = Option<u32>;

/// A tree widget mirror: one ordered row list per level, per-row expansion state keyed by the
/// node key, and one children binding per node. Like a real tree view, a row's state belongs to
/// the row: a row removed and re-added within one frame (a re-parenting) keeps it, a row that
/// is gone at the end of a frame loses it.
#[derive(Default)]
struct TreeWidget {
    levels: HashMap<Level, Vec<u32>>,
    names: HashMap<u32, &'static str>,
    row_level: HashMap<u32, Level>,
    expanded: HashMap<u32, bool>,
    detached: HashMap<u32, bool>,
    nodes: HashMap<u32, FolderNode>,
    child_bindings: HashMap<u32, Subscription>,
}

impl TreeWidget {
    fn insert_row(&mut self, level: Level, key: u32, index: Option<usize>, node: &FolderNode) {
        let rows = self.levels.entry(level).or_default();
        let index = index.expect("tree changes carry indexes");
        assert!(index <= rows.len(), "add of {key} at {index} on {level:?}");
        rows.insert(index, key);
        self.names.insert(key, node.item().name);
        self.nodes.insert(key, node.clone());
        if self.row_level.insert(key, level).is_none() {
            let state = self.detached.remove(&key).unwrap_or(false);
            self.expanded.insert(key, state);
        }
    }

    fn remove_row(&mut self, level: Level, key: u32, index: Option<usize>) {
        let rows = self.levels.entry(level).or_default();
        let index = index.expect("tree changes carry indexes");
        assert_eq!(rows.get(index), Some(&key), "remove index on {level:?}");
        rows.remove(index);
        if self.row_level.get(&key) == Some(&level) {
            self.row_level.remove(&key);
            if let Some(state) = self.expanded.remove(&key) {
                self.detached.insert(key, state);
            }
        }
    }

    fn move_row(&mut self, level: Level, key: u32, from: usize, to: usize) {
        let rows = self.levels.entry(level).or_default();
        assert_eq!(rows.get(from), Some(&key), "move index on {level:?}");
        rows.remove(from);
        rows.insert(to, key);
    }

    fn apply(
        &mut self,
        level: Level,
        change: &CacheChange<u32, FolderNode>,
        to_bind: &mut Vec<FolderNode>,
    ) {
        match change {
            CacheChange::Add {
                key,
                current,
                current_index,
            } => {
                self.insert_row(level, *key, *current_index, current);
                to_bind.push(current.clone());
            }
            CacheChange::Update {
                key,
                current,
                current_index,
                previous_index,
                ..
            } => {
                let from = previous_index.expect("tree updates carry indexes");
                let to = current_index.expect("tree updates carry indexes");
                self.move_row(level, *key, from, to);
                self.names.insert(*key, current.item().name);
                self.nodes.insert(*key, current.clone());
                // The update may carry a replacement node: follow its children from now on.
                to_bind.push(current.clone());
            }
            CacheChange::Remove {
                key,
                previous_index,
                ..
            } => self.remove_row(level, *key, *previous_index),
            CacheChange::Refresh { key, current, .. } => {
                self.names.insert(*key, current.item().name);
            }
            CacheChange::Move {
                key,
                current_index,
                previous_index,
                ..
            } => self.move_row(level, *key, *previous_index, *current_index),
            CacheChange::Clear { .. } => {
                for key in self.levels.remove(&level).unwrap_or_default() {
                    if self.row_level.get(&key) == Some(&level) {
                        self.row_level.remove(&key);
                        if let Some(state) = self.expanded.remove(&key) {
                            self.detached.insert(key, state);
                        }
                    }
                }
            }
        }
    }

    /// Ends a UI frame: rows that are gone lose their state and their children bindings.
    fn end_frame(&mut self) -> Vec<Subscription> {
        let present: HashSet<u32> = self.visible().into_iter().map(|(_, key, _)| key).collect();
        self.detached.clear();
        self.expanded.retain(|key, _| present.contains(key));
        self.row_level.retain(|key, _| present.contains(key));
        self.nodes.retain(|key, _| present.contains(key));
        let gone: Vec<u32> = self
            .child_bindings
            .keys()
            .filter(|key| !present.contains(key))
            .copied()
            .collect();
        gone.into_iter()
            .filter_map(|key| self.child_bindings.remove(&key))
            .collect()
    }

    /// The rendered tree in pre-order: (depth, key, name).
    fn visible(&self) -> Vec<(usize, u32, &'static str)> {
        fn visit(
            widget: &TreeWidget,
            level: Level,
            depth: usize,
            out: &mut Vec<(usize, u32, &'static str)>,
        ) {
            for key in widget.levels.get(&level).into_iter().flatten() {
                out.push((depth, *key, widget.names[key]));
                visit(widget, Some(*key), depth + 1, out);
            }
        }
        let mut out = Vec::new();
        visit(self, None, 0, &mut out);
        out
    }

    fn set_expanded(&mut self, key: u32, expanded: bool) {
        assert!(self.row_level.contains_key(&key), "row {key} is shown");
        self.expanded.insert(key, expanded);
    }

    fn is_expanded(&self, key: u32) -> bool {
        self.expanded.get(&key).copied().unwrap_or(false)
    }
}

fn bind_tree_level(
    widget: &Arc<Mutex<TreeWidget>>,
    level: Level,
    stream: &ChangeStream<CacheChange<u32, FolderNode>>,
) -> Result<Subscription> {
    let weak: Weak<Mutex<TreeWidget>> = Arc::downgrade(widget);
    stream.subscribe(move |batch| {
        let Some(widget) = weak.upgrade() else {
            return;
        };
        let mut to_bind = Vec::new();
        {
            let mut state = widget.lock().unwrap();
            for change in &batch.changes {
                state.apply(level, change, &mut to_bind);
            }
        }
        // Never subscribe while holding the widget lock: a children snapshot arrives at once.
        for node in to_bind {
            rebind_children(&widget, &node);
        }
    })
}

fn rebind_children(widget: &Arc<Mutex<TreeWidget>>, node: &FolderNode) {
    let key = node.key();
    let previous = {
        let mut state = widget.lock().unwrap();
        state.levels.remove(&Some(key));
        state.child_bindings.remove(&key)
    };
    drop(previous);
    let binding = bind_tree_level(widget, Some(key), &node.children().connect())
        .expect("binding a node's children succeeds");
    let replaced = widget.lock().unwrap().child_bindings.insert(key, binding);
    drop(replaced);
}

/// The tree the source describes: roots are folders whose parent is absent; children keep the
/// source (key) order.
fn expected_tree(source: &SourceCache<u32, Folder>) -> Vec<(usize, u32, &'static str)> {
    fn visit(
        items: &[Folder],
        present: &HashSet<u32>,
        parent: Level,
        depth: usize,
        out: &mut Vec<(usize, u32, &'static str)>,
    ) {
        for item in items {
            let belongs = match parent {
                None => !present.contains(&item.parent) || item.parent == item.id,
                Some(parent) => item.parent == parent && item.id != parent,
            };
            if belongs {
                out.push((depth, item.id, item.name));
                visit(items, present, Some(item.id), depth + 1, out);
            }
        }
    }
    let items = source.items().unwrap();
    let present: HashSet<u32> = items.iter().map(|item| item.id).collect();
    let mut out = Vec::new();
    visit(&items, &present, None, 0, &mut out);
    out
}

fn end_frame(widget: &Arc<Mutex<TreeWidget>>) {
    let released = widget.lock().unwrap().end_frame();
    drop(released);
}

#[test]
fn scn_1_2_tree_view_mirror_keeps_expansion_state_across_tree_changes() -> Result<()> {
    let folders = SourceCache::new(|folder: &Folder| folder.id);
    folders.add_or_update_range([
        folder(1, 0, "home"),
        folder(2, 1, "docs"),
        folder(3, 2, "drafts"),
        folder(4, 1, "music"),
        folder(5, 0, "tmp"),
        folder(6, 5, "cache"),
    ])?;

    let widget = Arc::new(Mutex::new(TreeWidget::default()));
    let roots = bind_tree_level(
        &widget,
        None,
        &folders.connect().transform_to_tree(|folder| folder.parent),
    )?;
    let check = |step: &str| {
        end_frame(&widget);
        let state = widget.lock().unwrap();
        assert_eq!(
            state.visible(),
            expected_tree(&folders),
            "tree after {step}"
        );
    };
    check("initial load");
    {
        let mut state = widget.lock().unwrap();
        for key in [1, 2, 3, 4, 6] {
            state.set_expanded(key, true);
        }
    }
    let (handle_3, handle_6) = {
        let state = widget.lock().unwrap();
        (state.nodes[&3].clone(), state.nodes[&6].clone())
    };
    assert_eq!(handle_3.depth(), 2);
    assert_eq!(handle_6.parent_key(), Some(5));

    // Item update in place: the row keeps its state; the mirror follows the (possibly
    // replaced) node's children, so a child added afterwards still appears.
    folders.add_or_update(folder(2, 1, "documents"))?;
    check("renaming docs");
    folders.add_or_update(folder(10, 2, "notes"))?;
    check("adding a child to the renamed node");
    {
        let state = widget.lock().unwrap();
        assert!(state.is_expanded(2) && state.is_expanded(3));
        assert!(!state.is_expanded(10));
    }

    // Re-parenting (2 and its subtree move under 5) in one edit with an unrelated rename.
    folders.edit(|cache| {
        cache.add_or_update(folder(2, 5, "documents"));
        cache.add_or_update(folder(4, 1, "audio"));
    })?;
    check("re-parenting docs under tmp");
    {
        let state = widget.lock().unwrap();
        assert!(
            state.is_expanded(2),
            "a re-parented row keeps its expansion"
        );
        assert!(state.is_expanded(3), "its subtree keeps its expansion");
        assert!(state.is_expanded(4), "an updated row keeps its expansion");
    }
    assert_eq!(handle_3.depth(), 2, "drafts: tmp > documents > drafts");

    // Parent removal: 2 and 6 are promoted to roots with their state; nodes keep identity.
    folders.remove_key(&5)?;
    check("removing tmp");
    {
        let state = widget.lock().unwrap();
        assert!(state.is_expanded(2) && state.is_expanded(6));
        assert_eq!(
            state.levels[&None],
            [1, 2, 6],
            "promoted roots keep source order"
        );
    }
    assert!(handle_6.is_root(), "the promoted node is the same node");
    assert_eq!(handle_6.depth(), 0);
    assert_eq!(handle_3.depth(), 1, "drafts follows its promoted parent");

    // Late-arriving parent: 7 names 8 before 8 exists, so it starts as a root.
    folders.add_or_update(folder(7, 8, "inbox"))?;
    check("adding an orphan");
    widget.lock().unwrap().set_expanded(7, true);
    let handle_7 = widget.lock().unwrap().nodes[&7].clone();
    assert!(handle_7.is_root());
    folders.add_or_update(folder(8, 0, "mail"))?;
    check("adding the late parent");
    assert!(
        widget.lock().unwrap().is_expanded(7),
        "attached orphan keeps its state"
    );
    assert_eq!(
        handle_7.parent_key(),
        Some(8),
        "the orphan node itself was attached"
    );
    assert_eq!(handle_7.depth(), 1);
    folders.add_or_update(folder(9, 7, "receipts"))?;
    check("adding a child to the attached orphan");
    assert_eq!(handle_7.children().keys()?, [9], "same node, same children");

    // Refresh and sibling moves keep node identity and state.
    folders.refresh_key(&7)?;
    folders.move_key(&4, 0)?;
    check("refreshing and moving");
    assert!(widget.lock().unwrap().is_expanded(7));
    assert!(widget.lock().unwrap().is_expanded(4));

    // A row removed for good loses its state: re-adding it in a later frame starts collapsed.
    folders.remove_key(&4)?;
    check("removing audio");
    folders.add_or_update(folder(4, 1, "audio"))?;
    check("re-adding audio");
    assert!(!widget.lock().unwrap().is_expanded(4));

    // Removing a parent with an expanded subtree and re-parenting in the same edit.
    folders.edit(|cache| {
        let _ = cache.remove_key(&1);
        cache.add_or_update(folder(4, 2, "audio"));
    })?;
    check("removing home and moving audio under documents");
    {
        let state = widget.lock().unwrap();
        assert!(!state.row_level.contains_key(&1));
        assert!(state.is_expanded(2) && state.is_expanded(3) && state.is_expanded(7));
    }

    drop(roots);
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// SCN-1.3: foreground observers under worker writes
// ---------------------------------------------------------------------------------------------

type Entry = (u32, u64);

fn apply_entry_changes(
    mirror: &mut BTreeMap<u32, u64>,
    batch: &ChangeSet<CacheChange<u32, Entry>>,
) {
    for change in &batch.changes {
        match change {
            CacheChange::Add { key, current, .. }
            | CacheChange::Update { key, current, .. }
            | CacheChange::Refresh { key, current, .. }
            | CacheChange::Move { key, current, .. } => {
                mirror.insert(*key, current.1);
            }
            CacheChange::Remove { key, .. } => {
                mirror.remove(key);
            }
            CacheChange::Clear { .. } => mirror.clear(),
        }
    }
}

#[test]
fn scn_1_3_foreground_connect_lookup_and_watch_converge_under_worker_writes() -> Result<()> {
    const OPERATIONS: usize = 2_000;
    const KEY_SPACE: usize = 64;
    const STABLE_KEYS: usize = 8; // updated but never removed
    const MAX_ROUNDS: usize = 50_000;

    let source = SourceCache::new(|entry: &Entry| entry.0);
    source.add_or_update_range((0..KEY_SPACE as u32).map(|key| (key, 0)))?;

    let done = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Barrier::new(2));
    let worker = {
        let source = source.clone();
        let done = Arc::clone(&done);
        let gate = Arc::clone(&gate);
        thread::spawn(move || -> Result<()> {
            let mut rng = Prng::new(0x00C0_FFEE);
            gate.wait();
            for operation in 0..OPERATIONS {
                let value = operation as u64 + 1;
                let volatile = (STABLE_KEYS + rng.below(KEY_SPACE - STABLE_KEYS)) as u32;
                match rng.below(8) {
                    0..=2 => {
                        source.add_or_update((rng.below(STABLE_KEYS) as u32, value))?;
                    }
                    3 | 4 => {
                        source.add_or_update((volatile, value))?;
                    }
                    5 => {
                        source.remove_key(&volatile)?;
                    }
                    6 => {
                        let stable = rng.below(STABLE_KEYS) as u32;
                        source.edit(|cache| {
                            cache.add_or_update((stable, value));
                            let _ = cache.remove_key(&volatile);
                            cache.add_or_update((volatile ^ 1, value));
                        })?;
                    }
                    _ => {
                        let count = source.count()?;
                        source.move_key(&volatile, rng.below(count))?;
                    }
                }
                if operation % 32 == 0 {
                    thread::yield_now();
                }
            }
            done.store(true, atomic::Ordering::SeqCst);
            Ok(())
        })
    };

    let mut failures = Vec::<String>::new();
    let mut materialized = Vec::<ObservableCache<u32, Entry>>::new();
    let mut manual = Vec::<(Arc<Mutex<BTreeMap<u32, u64>>>, Subscription)>::new();
    let mut watchers = Vec::<(u32, Arc<Mutex<Option<Entry>>>, Subscription)>::new();
    let transient_snapshots = Arc::new(AtomicUsize::new(0));

    gate.wait();
    let mut round = 0;
    while round < MAX_ROUNDS && (round < 200 || !done.load(atomic::Ordering::SeqCst)) {
        let key = (round % KEY_SPACE) as u32;
        if let Err(error) = source.lookup(&key) {
            failures.push(format!("lookup: {error:?}"));
        }
        match round % 6 {
            0 if materialized.len() < 24 => {
                match source.connect().as_observable_cache(|entry| entry.0) {
                    Ok(view) => materialized.push(view),
                    Err(error) => failures.push(format!("as_observable_cache: {error:?}")),
                }
            }
            1 if manual.len() < 24 => {
                let mirror = Arc::new(Mutex::new(BTreeMap::new()));
                let sink = Arc::clone(&mirror);
                match source
                    .connect()
                    .subscribe(move |batch| apply_entry_changes(&mut sink.lock().unwrap(), batch))
                {
                    Ok(subscription) => manual.push((mirror, subscription)),
                    Err(error) => failures.push(format!("connect: {error:?}")),
                }
            }
            2 if watchers.len() < 32 => {
                let watched = key % STABLE_KEYS as u32;
                let last = Arc::new(Mutex::new(None));
                let sink = Arc::clone(&last);
                match source
                    .watch_value(watched)
                    .subscribe(move |entry| *sink.lock().unwrap() = Some(*entry))
                {
                    Ok(subscription) => watchers.push((watched, last, subscription)),
                    Err(error) => failures.push(format!("watch_value: {error:?}")),
                }
            }
            3 => {
                // Connect / dispose churn while the worker writes.
                let counter = Arc::clone(&transient_snapshots);
                match source.connect().subscribe(move |_| {
                    counter.fetch_add(1, atomic::Ordering::Relaxed);
                }) {
                    Ok(subscription) => drop(subscription),
                    Err(error) => failures.push(format!("transient connect: {error:?}")),
                }
            }
            4 => {
                if let Err(error) = source.key_values() {
                    failures.push(format!("key_values: {error:?}"));
                }
            }
            _ => {}
        }
        round += 1;
    }

    worker.join().expect("the worker thread panicked")?;
    assert!(failures.is_empty(), "foreground calls failed: {failures:?}");
    assert!(transient_snapshots.load(atomic::Ordering::Relaxed) > 0);

    let mut expected = source.key_values()?;
    expected.sort();
    let expected_map: BTreeMap<u32, u64> = expected
        .iter()
        .map(|(key, entry)| (*key, entry.1))
        .collect();
    assert!(!materialized.is_empty() && !manual.is_empty() && !watchers.is_empty());
    for (index, view) in materialized.iter().enumerate() {
        let mut actual = view.key_values()?;
        actual.sort();
        assert_eq!(actual, expected, "materialized mirror {index} converges");
    }
    for (index, (mirror, _subscription)) in manual.iter().enumerate() {
        assert_eq!(
            *mirror.lock().unwrap(),
            expected_map,
            "connect mirror {index} converges"
        );
    }
    for (watched, last, _subscription) in &watchers {
        assert_eq!(
            *last.lock().unwrap(),
            source.lookup(watched)?,
            "watch_value({watched}) ends on the current value"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// SCN-1.4: application recipes
// ---------------------------------------------------------------------------------------------

#[test]
fn scn_1_4_bounded_live_feed_keeps_the_newest_lines_first() -> Result<()> {
    const LIMIT: usize = 3;
    let log_lines = EventBus::<String>::new();
    let feed = log_lines
        .stream()
        .values()
        .to_list_change_set_with_options(ChangeSetAdapterOptions::limit_size_to(LIMIT))
        .reverse();

    // Two consumers of the same feed: a bound widget vector and a materialized list.
    let shown = Arc::new(Mutex::new(Vec::new()));
    let binding = feed.bind_to_vec(Arc::clone(&shown))?;
    let view = feed.as_observable_list()?;

    let newest_first = |last: usize| -> Vec<String> {
        (last.saturating_sub(LIMIT - 1).max(1)..=last)
            .rev()
            .map(|n| format!("line {n}"))
            .collect()
    };
    for n in 1..=40 {
        log_lines.emit(format!("line {n}"))?;
        assert_eq!(
            *shown.lock().unwrap(),
            newest_first(n),
            "bound feed after {n}"
        );
        assert_eq!(view.items()?, newest_first(n), "observable feed after {n}");
    }

    // A producer thread keeps logging; the feed stays bounded and ends on its last lines.
    let producer = {
        let log_lines = log_lines.clone();
        thread::spawn(move || -> dxn_reactive::Result<()> {
            for n in 41..=240 {
                log_lines.emit(format!("line {n}"))?;
            }
            Ok(())
        })
    };
    producer.join().expect("the producer panicked")?;
    assert_eq!(*shown.lock().unwrap(), newest_first(240));
    assert_eq!(view.items()?, newest_first(240));
    assert_eq!(view.len()?, LIMIT);

    drop(binding);
    log_lines.emit("after unbinding".to_string())?;
    assert_eq!(
        *shown.lock().unwrap(),
        newest_first(240),
        "an unbound vector stays put"
    );
    assert_eq!(view.items()?[0], "after unbinding");
    Ok(())
}

struct Field {
    name: &'static str,
    text: Signal<String>,
    valid: Computed<bool>,
}

impl Field {
    fn new(name: &'static str, rule: fn(&str) -> bool) -> Result<Arc<Self>> {
        let text = Signal::new(String::new());
        let valid = Computed::derive(text.read_only(), move |text| rule(text))?;
        Ok(Arc::new(Self { name, text, valid }))
    }
}

#[test]
fn scn_1_4_form_validation_drives_command_enablement() -> Result<()> {
    let fields = SourceCache::new(|field: &Arc<Field>| field.name);
    let email = Field::new("email", |text| text.contains('@'))?;
    let name = Field::new("name", |text| !text.trim().is_empty())?;
    fields.add_or_update_range([Arc::clone(&email), Arc::clone(&name)])?;

    let all_valid = Computed::from_stream(
        &fields.true_for_all_value(|field, _| field.valid.values(), |valid| *valid),
        false,
    )?;
    let submissions = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&submissions);
    let submit = Command::with_can_execute(all_valid.read_signal(), move || {
        counter.fetch_add(1, atomic::Ordering::SeqCst);
        Ok(())
    });

    assert!(!submit.can_execute().get()?);
    assert_eq!(submit.execute(), Err(CommandError::Disabled));

    email.text.set("ada@example.com".into())?;
    assert!(!submit.can_execute().get()?, "name is still empty");
    name.text.set("Ada".into())?;
    assert!(submit.can_execute().get()?);
    assert_eq!(submit.execute(), Ok(()));
    assert_eq!(submissions.load(atomic::Ordering::SeqCst), 1);

    // A field added later joins the rule at once.
    let age = Field::new("age", |text| text.parse::<u8>().is_ok())?;
    fields.add_or_update(Arc::clone(&age))?;
    assert!(
        !submit.can_execute().get()?,
        "the new empty field is invalid"
    );
    age.text.set("42".into())?;
    assert!(submit.can_execute().get()?);

    // Invalidating a field disables; removing the invalid field re-enables.
    email.text.set("not an address".into())?;
    assert!(!submit.can_execute().get()?);
    assert_eq!(submit.execute(), Err(CommandError::Disabled));
    fields.remove_key(&"email")?;
    assert!(
        submit.can_execute().get()?,
        "the remaining fields are valid"
    );
    email.text.set("still detached".into())?;
    assert!(
        submit.can_execute().get()?,
        "a removed field no longer counts"
    );

    // An empty form is vacuously valid.
    fields.clear()?;
    assert!(all_valid.get()?);
    assert_eq!(submit.execute(), Ok(()));
    assert_eq!(submissions.load(atomic::Ordering::SeqCst), 2);
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct CartLine {
    sku: u32,
    quantity: u32,
    price_cents: u64,
}

#[test]
fn scn_1_4_property_grid_mirrors_collection_aggregates_as_scalars() -> Result<()> {
    let cart = SourceCache::new(|line: &CartLine| line.sku);
    let line_count = Computed::from_stream(&cart.connect().count(), 0)?;
    let total_quantity = Computed::from_stream(&cart.connect().sum_by(|line| line.quantity), 0)?;
    let max_price = Computed::from_stream(&cart.connect().max_by(|line| line.price_cents, 0), 0)?;
    let summary = Computed::combine3(
        line_count.read_signal(),
        total_quantity.read_signal(),
        max_price.read_signal(),
        |lines, quantity, max| format!("{lines} lines, {quantity} items, max {max}"),
    )?;
    assert_eq!(summary.get()?, "0 lines, 0 items, max 0");

    let check = |step: &str| -> Result<()> {
        let lines = cart.items()?;
        let expected_quantity: u32 = lines.iter().map(|line| line.quantity).sum();
        let expected_max = lines.iter().map(|line| line.price_cents).max().unwrap_or(0);
        assert_eq!(line_count.get()?, lines.len(), "count after {step}");
        assert_eq!(total_quantity.get()?, expected_quantity, "sum after {step}");
        assert_eq!(max_price.get()?, expected_max, "max after {step}");
        assert_eq!(
            summary.get()?,
            format!(
                "{} lines, {expected_quantity} items, max {expected_max}",
                lines.len()
            ),
            "summary after {step}"
        );
        Ok(())
    };

    let line = |sku, quantity, price_cents| CartLine {
        sku,
        quantity,
        price_cents,
    };
    cart.add_or_update_range([line(1, 2, 500), line(2, 1, 1_250), line(3, 5, 99)])?;
    check("loading")?;
    cart.add_or_update(line(2, 3, 1_250))?;
    check("changing a quantity")?;
    cart.remove_key(&2)?; // the maximum leaves: the max must fall back to the next one
    check("removing the most expensive line")?;
    cart.add_or_update(line(1, 2, 50))?; // the new maximum is lowered in place
    check("lowering a price")?;
    cart.edit(|cache| {
        cache.add_or_update(line(4, 1, 9_999));
        let _ = cache.remove_key(&3);
    })?;
    check("one edit with an add and a removal")?;

    // A background sync writes; the property grid reads converged scalars afterwards.
    let worker = {
        let cart = cart.clone();
        thread::spawn(move || -> Result<()> {
            let mut rng = Prng::new(0xCA27);
            for _ in 0..500 {
                let sku = rng.below(20) as u32;
                if rng.below(4) == 0 {
                    cart.remove_key(&sku)?;
                } else {
                    cart.add_or_update(CartLine {
                        sku,
                        quantity: rng.below(9) as u32 + 1,
                        price_cents: rng.below(10_000) as u64,
                    })?;
                }
            }
            Ok(())
        })
    };
    worker.join().expect("the cart worker panicked")?;
    check("background sync")?;

    cart.clear()?;
    check("clearing")?;
    assert_eq!(summary.get()?, "0 lines, 0 items, max 0");
    Ok(())
}
