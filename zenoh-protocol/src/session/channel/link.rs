//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//
use async_std::sync::{Arc, Barrier, Mutex};
use async_std::task;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::{LinkQueue, KeepAliveEvent};

use crate::core::ZInt;
use crate::link::Link;
use crate::proto::{SeqNumGenerator, SessionMessage, ZenohMessage};

use zenoh_util::collections::{TimedEvent, TimedHandle, Timer};
use zenoh_util::core::ZResult;


// Consume task
async fn consume_task(
    queue: Arc<LinkQueue>, 
    link: Link,
    active: Arc<AtomicBool>,
    barrier: Arc<Barrier>
) -> ZResult<()> {
    // Keep draining the queue while active
    while active.load(Ordering::Relaxed) {
        // Pull a serialized batch from the queue
        let (batch, index) = queue.pull().await;
        // Send the buffer on the link
        link.send(batch.get_buffer()).await?;
        // Reinsert the batch into the queue
        queue.push_serialization_batch(batch, index).await;
    }

    // Drain what remains in the queue before exiting
    while let Some(batch) = queue.drain().await {
        link.send(batch.get_buffer()).await?;
    }

    // Synchronize with the close()
    barrier.wait().await;

    Ok(())
}

#[derive(Clone)]
pub(super) struct ChannelLink {
    link: Link,
    queue: Arc<LinkQueue>,
    active: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    handle: TimedHandle
}

impl ChannelLink {
    pub(super) fn new(
        link: Link,
        batch_size: usize,
        keep_alive: ZInt,
        sn_reliable: Arc<Mutex<SeqNumGenerator>>,
        sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
        timer: Timer
    ) -> ChannelLink {
        // The queue
        let queue = Arc::new(LinkQueue::new(
            batch_size.min(link.get_mtu()), link.is_streamed(), sn_reliable, sn_best_effort
        ));

        // Control variable
        let active = Arc::new(AtomicBool::new(true));

        // Barrier for sincronization
        let barrier = Arc::new(Barrier::new(2));

        // Keep alive event
        let event = KeepAliveEvent::new(queue.clone(), link.clone());
        // Keep alive interval is expressed in milliseconds
        let interval = Duration::from_millis(keep_alive);
        let event = TimedEvent::periodic(interval, event);
        // Get the handle of the periodic event
        let handle = event.get_handle();

        // Spawn the consume task
        let c_queue = queue.clone();
        let c_link = link.clone();
        let c_active = active.clone();
        let c_barrier = barrier.clone();
        let c_handle = handle.clone();
        task::spawn(async move {
            // Add the keep alive event to the timer
            timer.add(event).await;
            // Start
            let res = consume_task(c_queue, c_link, c_active, c_barrier).await;
            if res.is_err() {
                c_active.store(false, Ordering::Relaxed);
                c_handle.defuse();
            }
        });

        ChannelLink {
            link,
            queue,
            active,
            barrier,
            handle
        }
    }
}

impl ChannelLink {
    #[inline]    
    pub(super) fn get_link(&self) -> &Link {
        &self.link
    }

    pub(super) async fn schedule_zenoh_message(&self, msg: ZenohMessage, priority: usize) {
        if self.active.load(Ordering::Relaxed) {
            self.queue.push_zenoh_message(msg, priority).await;
        }
    }

    pub(super) async fn schedule_session_message(&self, msg: SessionMessage, priority: usize) {
        if self.active.load(Ordering::Relaxed) {
            self.queue.push_session_message(msg, priority).await;
        }
    }

    pub(super) async fn close(self) -> ZResult<()> {
        // Deactivate the consume task if active
        if self.active.swap(false, Ordering::Relaxed) {
            // Defuse the keep alive event
            self.handle.defuse();
            // Wait for the task to exit
            self.barrier.wait().await;
            // Close the underlying link
            self.link.close().await
        } else {
            Ok(())
        }
    }
}