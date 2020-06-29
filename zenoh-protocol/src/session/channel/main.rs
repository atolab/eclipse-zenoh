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
use async_std::sync::{Arc, Mutex, RwLock, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::{ChannelInnerRx, ChannelLink, LeaseEvent, ReliabilityQueue};

use crate::core::{PeerId, ZInt};
use crate::link::Link;
use crate::io::WBuf;
use crate::proto::{FramePayload, SeqNumGenerator, SessionMessage, WhatAmI, ZenohMessage};
use crate::session::{MsgHandler, SessionManagerInner};
use crate::session::defaults::{QUEUE_PRIO_CTRL, QUEUE_PRIO_DATA};

use zenoh_util::{zasynclock, zasyncread, zasyncwrite};
use zenoh_util::collections::{CreditBuffer, CreditQueue, TimedEvent, Timer};
use zenoh_util::core::ZResult;


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
    // The batch size 
    pub(super) batch_size: usize,
    // The callback has been set or not
    pub(super) has_callback: AtomicBool,
    // The sn generator for the reliable channel
    pub(super) sn_reliable: Arc<Mutex<SeqNumGenerator>>,
    // The sn generator for the best_effort channel
    pub(super) sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
    // The links associated to the channel
    pub(super) links: Arc<RwLock<Vec<ChannelLink>>>,
    // The mutable data struct for reception
    pub(super) rx: Mutex<ChannelInnerRx>,
    // The internal timer
    pub(super) timer: Timer,
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
        Channel {
            manager,
            pid,
            whatami,
            lease,
            keep_alive,
            sn_resolution,
            batch_size,
            has_callback: AtomicBool::new(false),
            sn_reliable: Arc::new(Mutex::new(SeqNumGenerator::new(initial_sn_tx, sn_resolution))),
            sn_best_effort: Arc::new(Mutex::new(SeqNumGenerator::new(initial_sn_tx, sn_resolution))),
            links: Arc::new(RwLock::new(Vec::new())),
            rx: Mutex::new(ChannelInnerRx::new(sn_resolution, initial_sn_rx)),
            timer: Timer::new(),
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
    /*           TERMINATION             */
    /*************************************/
    pub(super) async fn delete(&self) {
        // // Stop the consume task with the lowest priority to give enough
        // // time to send all the messages still present in the queue
        // let res = self.stop(*QUEUE_PRIO_DATA).await;
        // log::trace!("Delete: {:?}", res);

        // // Delete the session on the manager
        // let _ = self.manager.del_session(&self.pid).await;            

        // // Notify the callback
        // if let Some(callback) = &zasynclock!(self.rx).callback {
        //     callback.close().await;
        // }
        
        // // Close all the links
        // // NOTE: del_link() is meant to be used thorughout the lifetime
        // //       of the session and not for its termination.
        // for l in self.get_links().await.drain(..) {
        //     let _ = l.close().await;
        // }

        // // Remove all the reference to the links
        // zasyncwrite!(self.links).clear();
    }

    pub(crate) async fn close_link(&self, link: &Link, reason: u8) -> ZResult<()> {     
        log::trace!("Closing link {} with peer: {}", link, self.get_peer());

        let guard = zasyncread!(self.links);
        if let Some(l) = guard.iter().find(|l| l.get_link() == link) {
            // Close message to be sent on the target link
            let peer_id = Some(self.manager.config.pid.clone());
            let reason_id = reason;              
            let link_only = true;  // This is should always be true when closing a link              
            let attachment = None;  // No attachment here
            let msg = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

            // Schedule the close message for transmission
            l.schedule_session_message(msg, QUEUE_PRIO_DATA).await;

            // Drop the guard
            drop(guard);
            
            // Remove the link from the channel
            self.del_link(&link).await?;
        }

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
        // Create a channel link from a link
        let link = ChannelLink::new(
            link, self.batch_size, self.keep_alive,
            self.sn_reliable.clone(), self.sn_best_effort.clone(), self.timer.clone()
        );

        // // Add the link along with the handle for defusing the KEEP_ALIVE messages
        // zasyncwrite!(self.links).add(link.clone(), handle)?;

        // // Add the link to the set of alive links
        // zasynclock!(self.rx).alive.insert(link);
        
        Ok(())
    }

    pub(crate) async fn del_link(&self, link: &Link) -> ZResult<()> { 
        // // Try to remove the link
        // let mut guard = zasyncwrite!(self.links);
        // let handle = guard.del(link)?;
        // let is_empty = guard.links.is_empty();
        // // Drop the guard before doing any other operation
        // drop(guard);
        
        // // Defuse the periodic sending of KEEP_ALIVE messages on this link
        // handle.defuse();
                
        // // Stop the consume task to get the updated view on the links
        // let _ = self.stop(*QUEUE_PRIO_CTRL).await;
        // // Don't restart the consume task if there are no links left
        // if !is_empty {
        //     // Start the consume task with the new view on the links
        //     let _ = self.start().await;
        // }

        Ok(())    
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        // zasyncread!(self.links).get()
        Vec::new()
    }
}