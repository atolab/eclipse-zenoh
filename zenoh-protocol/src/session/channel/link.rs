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
use async_std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use super::{Channel, KeepAliveEvent, LinkLeaseEvent, TransmissionQueue};

use crate::core::ZInt;
use crate::link::Link;
use crate::proto::{SeqNumGenerator, SessionMessage, ZenohMessage};
use crate::session::defaults::{
    QUEUE_PRIO_CTRL,
    QUEUE_SIZE_CTRL,
    QUEUE_PRIO_RETX,
    QUEUE_SIZE_RETX,
    QUEUE_PRIO_DATA,
    QUEUE_SIZE_DATA,
};

use zenoh_util::collections::{TimedEvent, TimedHandle, Timer};
use zenoh_util::core::ZResult;


pub(super) struct LinkAlive {
    inner: AtomicBool
}

impl LinkAlive {
    fn new() -> LinkAlive {
        LinkAlive { 
            inner: AtomicBool::new(true)
        }
    }

    #[inline]    
    pub(super) fn mark(&self) {
        self.inner.store(true, Ordering::Relaxed);
    }

    #[inline]    
    pub(super) fn reset(&self) -> bool {
        self.inner.swap(false, Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub(super) struct ChannelLink {
    link: Link,
    ctrl: Arc<TransmissionQueue>,
    retx: Arc<TransmissionQueue>,
    data: Arc<TransmissionQueue>,
    active: Arc<AtomicBool>,
    alive: Arc<LinkAlive>,
    handles: Vec<TimedHandle>,
}

impl ChannelLink {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        link: Link,
        batch_size: usize,
        sn_reliable: Arc<Mutex<SeqNumGenerator>>,
        sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
    ) -> ChannelLink {
        // The link and batch characteristics
        let batch_size = batch_size.min(link.get_mtu());
        let is_streamed = link.is_streamed();
        let amlink = Arc::new(Mutex::new(link.clone()));

        // The state of the queues ready to transmit
        let state = Arc::new(AtomicUsize::new(0));

        // Build the transmission queues 
        let ctrl = Arc::new(TransmissionQueue::new(
            *QUEUE_SIZE_CTRL, QUEUE_PRIO_CTRL, batch_size, is_streamed, 
            amlink.clone(), state.clone(), sn_reliable.clone(), sn_best_effort.clone()
        ));
        let retx = Arc::new(TransmissionQueue::new(
            *QUEUE_SIZE_RETX, QUEUE_PRIO_RETX, batch_size, is_streamed, 
            amlink.clone(), state.clone(), sn_reliable.clone(), sn_best_effort.clone()
        ));
        let data = Arc::new(TransmissionQueue::new(
            *QUEUE_SIZE_DATA, QUEUE_PRIO_DATA, batch_size, is_streamed, 
            amlink, state, sn_reliable, sn_best_effort
        ));

        // Control variables
        let active = Arc::new(AtomicBool::new(false));
        let alive = Arc::new(LinkAlive::new());

        ChannelLink {
            link,
            ctrl,
            retx, 
            data,
            active,
            alive,
            handles: Vec::new()
        }
    }
}

impl ChannelLink {
    #[inline]    
    pub(super) fn get_link(&self) -> &Link {
        &self.link
    }

    #[inline]    
    pub(super) fn mark_alive(&self) {
        self.alive.mark();
    }

    pub(super) async fn start(
        &mut self,
        ch: Weak<Channel>,
        keep_alive: ZInt,
        lease: ZInt,
        timer: &Timer
    ) {
        // Keep alive event
        let event = KeepAliveEvent::new(self.ctrl.clone(), self.link.clone());
        // Keep alive interval is expressed in milliseconds
        let interval = Duration::from_millis(keep_alive);
        let ka_event = TimedEvent::periodic(interval, event);
        // Get the handle of the periodic event
        let ka_handle = ka_event.get_handle();

        // Lease event
        let event = LinkLeaseEvent::new(ch, self.alive.clone(), self.link.clone());
        // Keep alive interval is expressed in milliseconds
        let interval = Duration::from_millis(lease);
        let ll_event = TimedEvent::periodic(interval, event);
        // Get the handle of the periodic event
        let ll_handle = ll_event.get_handle();

        // Event handles
        self.handles.push(ka_handle);
        self.handles.push(ll_handle);

        // Add the events to the timer
        timer.add(ka_event).await;
        timer.add(ll_event).await;
    }

    pub(super) async fn schedule_zenoh_message(&self, msg: ZenohMessage, priority: usize) {
        match priority {
            QUEUE_PRIO_DATA => self.data.push_zenoh_message(msg).await,
            QUEUE_PRIO_RETX => self.retx.push_zenoh_message(msg).await,
            QUEUE_PRIO_CTRL => self.ctrl.push_zenoh_message(msg).await,
            _ => log::warn!("Message dropped becasue of unsupported priority: {:?}", msg)
        }
    }

    pub(super) async fn schedule_session_message(&self, msg: SessionMessage, priority: usize) {
        match priority {            
            QUEUE_PRIO_CTRL => self.ctrl.push_session_message(msg).await,
            QUEUE_PRIO_RETX => self.retx.push_session_message(msg).await,            
            QUEUE_PRIO_DATA => self.data.push_session_message(msg).await,
            _ => log::warn!("Message dropped becasue of unsupported priority: {:?}", msg)
        }
    }

    pub(super) async fn close(mut self) -> ZResult<()> {
        // Deactivate the consume task if active
        if self.active.swap(false, Ordering::AcqRel) {
            // Defuse the timed events
            for h in self.handles.drain(..) {
                h.defuse();
            } 
            // Stop all the transmission queue
            self.data.stop().await;
            self.retx.stop().await;
            self.ctrl.stop().await;
            // Close the underlying link
            self.link.close().await
        } else {
            Ok(())
        }
    }
}