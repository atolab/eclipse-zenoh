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
use async_std::sync::{Arc, Barrier, Mutex, RwLock, Weak};
use async_std::task;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::{ChannelInnerRx, ChannelInnerTx, ChannelLinks, KeepAliveEvent, LeaseEvent};

use crate::core::{PeerId, ZInt};
use crate::link::Link;
use crate::proto::{SessionMessage, WhatAmI, ZenohMessage};
use crate::session::{MsgHandler, SessionManagerInner};
use crate::session::defaults::{
    // Control buffer
    QUEUE_PRIO_CTRL,
    QUEUE_SIZE_CTRL,
    QUEUE_CRED_CTRL,
    // Fragmentation buffer
    QUEUE_SIZE_FRAG,
    QUEUE_CRED_FRAG,
    // Retransmission buffer
    QUEUE_SIZE_RETX,
    QUEUE_CRED_RETX,
    // Data buffer
    QUEUE_PRIO_DATA,
    QUEUE_SIZE_DATA,
    QUEUE_CRED_DATA,
    // Queue size
    QUEUE_CONCURRENCY,
};

use zenoh_util::{zasynclock, zasyncread, zasyncwrite, zerror};
use zenoh_util::collections::{CreditBuffer, CreditQueue, TimedEvent, Timer};
use zenoh_util::core::{ZResult, ZError, ZErrorKind};



#[derive(Clone, Debug)]
pub(super) enum MessageInner {
    Session(SessionMessage),
    Zenoh(ZenohMessage),
    Stop
}

#[derive(Clone, Debug)]
pub(super) struct MessageTx {
    // The inner message to transmit
    pub(super) inner: MessageInner,
    // The preferred link to transmit the Message on
    pub(super) link: Option<Link>
}

/*************************************/
/*           CHANNEL STRUCT          */
/*************************************/
pub(crate) struct Channel {
    // The manager this channel is associated to
    pub(super) manager: Arc<SessionManagerInner>,
    // The remote peer id
    pub(super) pid: PeerId,
    // The remote whatami
    pub(super) whatami: WhatAmI,
    // The session lease in seconds
    pub(super) lease: ZInt,
    // Keep alive interval
    pub(super) keep_alive: ZInt,
    // The SN resolution 
    pub(super) sn_resolution: ZInt,
    // Keep track whether the consume task is active
    pub(super) active: Mutex<bool>,
    // The callback has been set or not
    pub(super) has_callback: AtomicBool,
    // The message queue
    pub(super) queue: CreditQueue<MessageTx>,
    // The links associated to the channel
    pub(super) links: RwLock<ChannelLinks>,
    // The mutable data struct for transmission
    pub(super) tx: Mutex<ChannelInnerTx>,
    // The mutable data struct for reception
    pub(super) rx: Mutex<ChannelInnerRx>,
    // The internal timer
    pub(super) timer: Timer,
    // Barrier for syncrhonizing the stop() with the consume_task
    pub(super) barrier: Arc<Barrier>,
    // Weak reference to self
    pub(super) w_self: RwLock<Option<Weak<Self>>>
}

impl Channel {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        manager: Arc<SessionManagerInner>,
        pid: PeerId, 
        whatami: WhatAmI,
        lease: ZInt,
        keep_alive: ZInt,
        sn_resolution: ZInt, 
        initial_sn_tx: ZInt,
        initial_sn_rx: ZInt,
        batch_size: usize        
    ) -> Channel {
        // The buffer to send the Control messages. High priority
        let ctrl = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_CTRL,
            *QUEUE_CRED_CTRL,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the retransmission of messages. Medium-High priority
        let retx = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_RETX,
            *QUEUE_CRED_RETX,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the fragmented messages. Medium-Low priority
        let frag = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_FRAG,
            *QUEUE_CRED_FRAG,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the Data messages. Low priority
        let data = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_DATA,
            *QUEUE_CRED_DATA,
            // @TODO: Once the reliability queue is implemented, update the spending policy
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // Build the vector of buffer for the transmission queue.
        // A lower index in the vector means higher priority in the queue.
        // The buffer with index 0 has the highest priority.
        let queue_tx = vec![ctrl, retx, frag, data];

        Channel {
            manager,
            pid,
            whatami,
            lease,
            keep_alive,
            sn_resolution,
            has_callback: AtomicBool::new(false),
            queue: CreditQueue::new(queue_tx, *QUEUE_CONCURRENCY),
            active: Mutex::new(false),
            links: RwLock::new(ChannelLinks::new(batch_size)),
            tx: Mutex::new(ChannelInnerTx::new(sn_resolution, initial_sn_tx)),
            rx: Mutex::new(ChannelInnerRx::new(sn_resolution, initial_sn_rx)),
            timer: Timer::new(),
            barrier: Arc::new(Barrier::new(2)),
            w_self: RwLock::new(None)
        }        
    }

    pub(crate) async fn initialize(&self, w_self: Weak<Self>) {
        // Initialize the weak reference to self
        *zasyncwrite!(self.w_self) = Some(w_self.clone());
        // Initialize the lease event
        let event = LeaseEvent::new(w_self);
        let interval = Duration::from_millis(self.lease);
        let event = TimedEvent::periodic(interval, event);
        self.timer.add(event).await;
    }


    /*************************************/
    /*            ACCESSORS              */
    /*************************************/
    pub(crate) fn get_peer(&self) -> PeerId {
        self.pid.clone()
    }

    pub(crate) fn get_whatami(&self) -> WhatAmI {
        self.whatami
    }

    pub(crate) fn get_lease(&self) -> ZInt {
        self.lease
    }

    pub(crate) fn get_keep_alive(&self) -> ZInt {
        self.keep_alive
    }

    pub(crate) fn get_sn_resolution(&self) -> ZInt {
        self.sn_resolution
    }

    pub(crate) fn has_callback(&self) -> bool {
        self.has_callback.load(Ordering::Relaxed)
    }

    pub(crate) async fn set_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) {        
        let mut guard = zasynclock!(self.rx);
        self.has_callback.store(true, Ordering::Relaxed);
        guard.callback = Some(callback);
    }


    /*************************************/
    /*               TASK                */
    /*************************************/
    async fn start(&self) -> ZResult<()> {
        let mut guard = zasynclock!(self.active);   
        // If not already active, start the transmission loop
        if !*guard {    
            // Get the Arc to the channel
            let ch = zasyncread!(self.w_self).as_ref().unwrap().upgrade().unwrap();

            // Spawn the transmission loop
            task::spawn(async move {
                let res = super::tx::consume_task(ch.clone()).await;
                if res.is_err() {
                    let mut guard = zasynclock!(ch.active);
                    *guard = false;
                }
            });

            // Mark that now the task can be stopped
            *guard = true;

            return Ok(())
        }

        zerror!(ZErrorKind::Other {
            descr: format!("Can not start channel with peer {} because it is already active", self.get_peer())
        })
    }

    async fn stop(&self, priority: usize) -> ZResult<()> {  
        let mut guard = zasynclock!(self.active);         
        if *guard {
            let msg = MessageTx {
                inner: MessageInner::Stop,
                link: None
            };
            self.queue.push(msg, priority).await;
            self.barrier.wait().await;

            // Mark that now the task can be started
            *guard = false;

            return Ok(())
        }

        zerror!(ZErrorKind::Other {
            descr: format!("Can not stop channel with peer {} because it is already inactive", self.get_peer())
        })
    }


    /*************************************/
    /*           TERMINATION             */
    /*************************************/
    pub(super) async fn delete(&self) {
        // Stop the consume task with the lowest priority to give enough
        // time to send all the messages still present in the queue
        let res = self.stop(*QUEUE_PRIO_DATA).await;
        log::trace!("Delete: {:?}", res);

        // Delete the session on the manager
        let _ = self.manager.del_session(&self.pid).await;            

        // Notify the callback
        if let Some(callback) = &zasynclock!(self.rx).callback {
            callback.close().await;
        }
        
        // Close all the links
        // NOTE: del_link() is meant to be used thorughout the lifetime
        //       of the session and not for its termination.
        for l in self.get_links().await.drain(..) {
            let _ = l.close().await;
        }

        // Remove all the reference to the links
        zasyncwrite!(self.links).clear();
    }

    pub(crate) async fn close_link(&self, link: &Link, reason: u8) -> ZResult<()> {     
        log::trace!("Closing link {} with peer: {}", link, self.get_peer());

        // Close message to be sent on the target link
        let peer_id = Some(self.manager.config.pid.clone());
        let reason_id = reason;              
        let link_only = true;  // This is should always be true when closing a link              
        let attachment = None;  // No attachment here
        let msg = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

        let close = MessageTx {
            inner: MessageInner::Session(msg),
            link: Some(link.clone())
        };

        // NOTE: del_link() stops the consume task with priority QUEUE_PRIO_CTRL.
        //       The close message must be pushed with the same priority before
        //       the link is deleted
        self.queue.push(close, *QUEUE_PRIO_CTRL).await;

        // Remove the link from the channel
        self.del_link(&link).await?;

        // Close the underlying link
        let _ = link.close().await;

        Ok(())
    }

    pub(crate) async fn close(&self, reason: u8) -> ZResult<()> {
        log::trace!("Closing session with peer: {}", self.get_peer());

        // Atomically push the messages on the queue
        let mut messages: Vec<MessageTx> = Vec::new();

        // Close message to be sent on all the links
        let peer_id = Some(self.manager.config.pid.clone());
        let reason_id = reason;              
        let link_only = false;  // This is should always be false for user-triggered close              
        let attachment = None;  // No attachment here
        let msg = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

        for link in self.get_links().await.drain(..) {
            let close = MessageTx {
                inner: MessageInner::Session(msg.clone()),
                link: Some(link)
            };
            messages.push(close);
        }

        // Atomically push the close and stop messages to the queue
        self.queue.push_batch(messages, *QUEUE_PRIO_DATA).await;

        // Terminate and clean up the session
        self.delete().await;
        
        Ok(())
    }

    /*************************************/
    /*        SCHEDULE AND SEND TX       */
    /*************************************/
    /// Schedule a Zenoh message on the transmission queue
    pub(crate) async fn schedule(&self, message: ZenohMessage, link: Option<Link>) {
        let message = MessageTx {
            inner: MessageInner::Zenoh(message),
            link
        };
        // Wait for the queue to have space for the message
        self.queue.push(message, *QUEUE_PRIO_DATA).await;
    }

    /// Schedule a batch of Zenoh messages on the transmission queue
    pub(crate) async fn schedule_batch(&self, mut messages: Vec<ZenohMessage>, link: Option<Link>) {
        let messages = messages.drain(..).map(|x| {
            MessageTx {
                inner: MessageInner::Zenoh(x),
                link: link.clone(),
            }
        }).collect();
        // Wait for the queue to have space for the message
        self.queue.push_batch(messages, *QUEUE_PRIO_DATA).await;        
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // Get the Arc to the channel
        let ch = zasyncread!(self.w_self).as_ref().unwrap().upgrade().unwrap();
        
        // Initialize the event for periodically sending KEEP_ALIVE messages on the link
        let event = KeepAliveEvent::new(Arc::downgrade(&ch), link.clone());
        // Keep alive interval is expressed in millisesond
        let interval = Duration::from_millis(self.keep_alive);
        let event = TimedEvent::periodic(interval, event);
        // Get the handle of the periodic event
        let handle = event.get_handle();

        // Add the link along with the handle for defusing the KEEP_ALIVE messages
        zasyncwrite!(self.links).add(link.clone(), handle)?;

        // Add the link to the set of alive links
        zasynclock!(self.rx).alive.insert(link);

        // Add the periodic event to the timer
        ch.timer.add(event).await;

        // Stop the consume task to get the updated view on the links
        let _ = self.stop(*QUEUE_PRIO_CTRL).await;
        // Start the consume task with the new view on the links
        let _ = self.start().await;
        
        Ok(())
    }

    pub(crate) async fn del_link(&self, link: &Link) -> ZResult<()> { 
        // Try to remove the link
        let mut guard = zasyncwrite!(self.links);
        let handle = guard.del(link)?;
        let is_empty = guard.links.is_empty();
        // Drop the guard before doing any other operation
        drop(guard);
        
        // Defuse the periodic sending of KEEP_ALIVE messages on this link
        handle.defuse();
                
        // Stop the consume task to get the updated view on the links
        let _ = self.stop(*QUEUE_PRIO_CTRL).await;
        // Don't restart the consume task if there are no links left
        if !is_empty {
            // Start the consume task with the new view on the links
            let _ = self.start().await;
        }

        Ok(())    
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        zasyncread!(self.links).get()
    }
}