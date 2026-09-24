// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Change notification for node-wide resources shared by partition
//! schedulers: a generation counter plus subscriber wakers. A subscriber
//! polls `changed` once per tick; it is told `true` once per change.

use std::sync::{Arc, Weak};
use std::task::Waker;

use futures::task::AtomicWaker;

#[derive(Debug, Default)]
pub(crate) struct ChangeNotifier {
    generation: u64,
    subscribers: Vec<Weak<AtomicWaker>>,
}

impl ChangeNotifier {
    /// Records a change; returns the subscribers to wake once the owner's
    /// lock is released (never wake while holding it).
    #[must_use]
    pub(crate) fn changed(&mut self) -> Vec<Arc<AtomicWaker>> {
        self.generation = self.generation.wrapping_add(1);
        let mut live = Vec::with_capacity(self.subscribers.len());
        self.subscribers.retain(|w| match w.upgrade() {
            Some(waker) => {
                live.push(waker);
                true
            }
            None => false,
        });
        live
    }

    pub(crate) fn subscribe(&mut self) -> ChangeSubscription {
        let waker = Arc::new(AtomicWaker::new());
        self.subscribers.push(Arc::downgrade(&waker));
        ChangeSubscription {
            waker,
            seen: self.generation,
        }
    }

    /// True (once per change) if something changed since the subscription
    /// last asked; registers `waker` for the next change.
    pub(crate) fn poll_changed(&self, sub: &mut ChangeSubscription, waker: &Waker) -> bool {
        sub.waker.register(waker);
        if self.generation != sub.seen {
            sub.seen = self.generation;
            true
        } else {
            false
        }
    }
}

/// A partition scheduler's subscription to a node-wide resource's changes.
#[derive(Debug)]
pub struct ChangeSubscription {
    waker: Arc<AtomicWaker>,
    seen: u64,
}

impl ChangeSubscription {
    /// A subscription to nothing (the resource is disabled).
    pub fn detached() -> Self {
        Self {
            waker: Arc::new(AtomicWaker::new()),
            seen: 0,
        }
    }
}

/// Wakes the returned subscribers, outside any lock.
pub(crate) fn wake_all(wakers: Vec<Arc<AtomicWaker>>) {
    wakers.iter().for_each(|w| w.wake());
}
