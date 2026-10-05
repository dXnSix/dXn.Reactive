use crate::collections::tests::support::*;

#[test]
fn cache_left_and_inner_join_track_keyed_views() -> Result<(), Box<dyn std::error::Error>> {
    let accounts = SourceCache::new(|account: &Account| account.id);
    let balances = SourceCache::new(|balance: &Balance| balance.id);
    accounts.edit(|cache| {
        cache.add_or_update(Account { id: 1, name: "Ada" });
        cache.add_or_update(Account { id: 2, name: "Lin" });
    })?;
    balances.add_or_update(Balance { id: 1, amount: 10 })?;

    let left_view = Arc::new(Mutex::new(Vec::<AccountBalance>::new()));
    let inner_view = Arc::new(Mutex::new(Vec::<AccountBalance>::new()));
    let _left_subscription = accounts
        .connect()
        .left_join(balances.connect(), |id, account, balance| AccountBalance {
            id: *id,
            name: Some(account.name),
            amount: balance.map(|balance| balance.amount),
        })
        .bind_to_vec(Arc::clone(&left_view))?;
    let _inner_subscription = accounts
        .connect()
        .inner_join(balances.connect(), |id, account, balance| AccountBalance {
            id: *id,
            name: Some(account.name),
            amount: Some(balance.amount),
        })
        .bind_to_vec(Arc::clone(&inner_view))?;

    assert_eq!(
        &*lock_vec(&left_view)?,
        &[
            AccountBalance {
                id: 1,
                name: Some("Ada"),
                amount: Some(10),
            },
            AccountBalance {
                id: 2,
                name: Some("Lin"),
                amount: None,
            },
        ]
    );
    assert_eq!(
        &*lock_vec(&inner_view)?,
        &[AccountBalance {
            id: 1,
            name: Some("Ada"),
            amount: Some(10),
        }]
    );

    balances.add_or_update(Balance { id: 2, amount: 20 })?;
    assert_eq!(
        &*lock_vec(&inner_view)?,
        &[
            AccountBalance {
                id: 1,
                name: Some("Ada"),
                amount: Some(10),
            },
            AccountBalance {
                id: 2,
                name: Some("Lin"),
                amount: Some(20),
            },
        ]
    );

    accounts.remove_key(&1)?;
    balances.remove_key(&2)?;
    assert_eq!(
        &*lock_vec(&left_view)?,
        &[AccountBalance {
            id: 2,
            name: Some("Lin"),
            amount: None,
        }]
    );
    assert!(lock_vec(&inner_view)?.is_empty());
    Ok(())
}

#[test]
fn cache_full_and_right_join_include_unmatched_sides() -> Result<(), Box<dyn std::error::Error>> {
    let accounts = SourceCache::new(|account: &Account| account.id);
    let balances = SourceCache::new(|balance: &Balance| balance.id);
    accounts.add_or_update(Account { id: 1, name: "Ada" })?;
    balances.edit(|cache| {
        cache.add_or_update(Balance { id: 1, amount: 10 });
        cache.add_or_update(Balance { id: 3, amount: 30 });
    })?;

    let full_view = Arc::new(Mutex::new(Vec::<AccountBalance>::new()));
    let right_view = Arc::new(Mutex::new(Vec::<AccountBalance>::new()));
    let _full_subscription = accounts
        .connect()
        .full_join(balances.connect(), |id, account, balance| AccountBalance {
            id: *id,
            name: account.map(|account| account.name),
            amount: balance.map(|balance| balance.amount),
        })
        .bind_to_vec(Arc::clone(&full_view))?;
    let _right_subscription = accounts
        .connect()
        .right_join(balances.connect(), |id, account, balance| AccountBalance {
            id: *id,
            name: account.map(|account| account.name),
            amount: Some(balance.amount),
        })
        .bind_to_vec(Arc::clone(&right_view))?;

    assert_eq!(
        &*lock_vec(&full_view)?,
        &[
            AccountBalance {
                id: 1,
                name: Some("Ada"),
                amount: Some(10),
            },
            AccountBalance {
                id: 3,
                name: None,
                amount: Some(30),
            },
        ]
    );
    assert_eq!(&*lock_vec(&right_view)?, &*lock_vec(&full_view)?);

    accounts.add_or_update(Account {
        id: 3,
        name: "Grace",
    })?;
    balances.remove_key(&1)?;

    assert_eq!(
        &*lock_vec(&full_view)?,
        &[
            AccountBalance {
                id: 1,
                name: Some("Ada"),
                amount: None,
            },
            AccountBalance {
                id: 3,
                name: Some("Grace"),
                amount: Some(30),
            },
        ]
    );
    assert_eq!(
        &*lock_vec(&right_view)?,
        &[AccountBalance {
            id: 3,
            name: Some("Grace"),
            amount: Some(30),
        }]
    );
    Ok(())
}

#[test]
fn cache_left_and_inner_join_many_reselect_when_groups_change()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Holding {
        id: u32,
        account_id: u32,
        amount: i32,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct AccountHoldings {
        id: u32,
        name: Option<&'static str>,
        count: usize,
        total: i32,
    }

    let accounts = SourceCache::new(|account: &Account| account.id);
    let holdings = SourceCache::new(|holding: &Holding| holding.id);
    accounts.edit(|cache| {
        cache.add_or_update(Account { id: 1, name: "Ada" });
        cache.add_or_update(Account { id: 2, name: "Lin" });
    })?;
    holdings.edit(|cache| {
        cache.add_or_update(Holding {
            id: 101,
            account_id: 1,
            amount: 10,
        });
        cache.add_or_update(Holding {
            id: 102,
            account_id: 1,
            amount: 5,
        });
    })?;

    let summarize =
        |id: &u32, name: Option<&'static str>, group: &RayCacheGroup<u32, u32, Holding>| {
            let holdings = group.items().unwrap_or_default();
            AccountHoldings {
                id: *id,
                name,
                count: holdings.len(),
                total: holdings.iter().map(|holding| holding.amount).sum(),
            }
        };

    let left_view = Arc::new(Mutex::new(Vec::<AccountHoldings>::new()));
    let inner_view = Arc::new(Mutex::new(Vec::<AccountHoldings>::new()));
    let _left_subscription = accounts
        .connect()
        .left_join_many(
            holdings.connect(),
            |holding| holding.account_id,
            move |id, account, group| summarize(id, Some(account.name), group),
        )
        .bind_to_vec(Arc::clone(&left_view))?;
    let _inner_subscription = accounts
        .connect()
        .inner_join_many(
            holdings.connect(),
            |holding| holding.account_id,
            move |id, account, group| summarize(id, Some(account.name), group),
        )
        .bind_to_vec(Arc::clone(&inner_view))?;

    assert_eq!(
        &*lock_vec(&left_view)?,
        &[
            AccountHoldings {
                id: 1,
                name: Some("Ada"),
                count: 2,
                total: 15,
            },
            AccountHoldings {
                id: 2,
                name: Some("Lin"),
                count: 0,
                total: 0,
            },
        ]
    );
    assert_eq!(
        &*lock_vec(&inner_view)?,
        &[AccountHoldings {
            id: 1,
            name: Some("Ada"),
            count: 2,
            total: 15,
        }]
    );

    holdings.add_or_update(Holding {
        id: 201,
        account_id: 2,
        amount: 20,
    })?;
    assert_eq!(
        &*lock_vec(&inner_view)?,
        &[
            AccountHoldings {
                id: 1,
                name: Some("Ada"),
                count: 2,
                total: 15,
            },
            AccountHoldings {
                id: 2,
                name: Some("Lin"),
                count: 1,
                total: 20,
            },
        ]
    );

    holdings.remove_key(&101)?;
    assert_eq!(
        lock_vec(&left_view)?[0],
        AccountHoldings {
            id: 1,
            name: Some("Ada"),
            count: 1,
            total: 5,
        }
    );
    Ok(())
}

#[test]
fn cache_full_and_right_join_many_include_right_only_groups()
-> Result<(), Box<dyn std::error::Error>> {
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Holding {
        id: u32,
        account_id: u32,
        amount: i32,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct AccountHoldings {
        id: u32,
        name: Option<&'static str>,
        total: i32,
    }

    let accounts = SourceCache::new(|account: &Account| account.id);
    let holdings = SourceCache::new(|holding: &Holding| holding.id);
    accounts.add_or_update(Account { id: 1, name: "Ada" })?;
    holdings.edit(|cache| {
        cache.add_or_update(Holding {
            id: 101,
            account_id: 1,
            amount: 10,
        });
        cache.add_or_update(Holding {
            id: 301,
            account_id: 3,
            amount: 30,
        });
    })?;

    let summarize =
        |id: &u32, account: Option<&Account>, group: &RayCacheGroup<u32, u32, Holding>| {
            let holdings = group.items().unwrap_or_default();
            AccountHoldings {
                id: *id,
                name: account.map(|account| account.name),
                total: holdings.iter().map(|holding| holding.amount).sum(),
            }
        };

    let full_view = Arc::new(Mutex::new(Vec::<AccountHoldings>::new()));
    let right_view = Arc::new(Mutex::new(Vec::<AccountHoldings>::new()));
    let _full_subscription = accounts
        .connect()
        .full_join_many(
            holdings.connect(),
            |holding| holding.account_id,
            move |id, account, group| summarize(id, account, group),
        )
        .bind_to_vec(Arc::clone(&full_view))?;
    let _right_subscription = accounts
        .connect()
        .right_join_many(
            holdings.connect(),
            |holding| holding.account_id,
            move |id, account, group| summarize(id, account, group),
        )
        .bind_to_vec(Arc::clone(&right_view))?;

    assert_eq!(
        &*lock_vec(&full_view)?,
        &[
            AccountHoldings {
                id: 1,
                name: Some("Ada"),
                total: 10,
            },
            AccountHoldings {
                id: 3,
                name: None,
                total: 30,
            },
        ]
    );
    assert_eq!(&*lock_vec(&right_view)?, &*lock_vec(&full_view)?);

    holdings.remove_key(&301)?;
    assert_eq!(
        &*lock_vec(&right_view)?,
        &[AccountHoldings {
            id: 1,
            name: Some("Ada"),
            total: 10,
        }]
    );

    accounts.remove_key(&1)?;
    assert_eq!(
        &*lock_vec(&full_view)?,
        &[AccountHoldings {
            id: 1,
            name: None,
            total: 10,
        }]
    );
    Ok(())
}
