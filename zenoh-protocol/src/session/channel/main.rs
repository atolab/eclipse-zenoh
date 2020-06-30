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

use super::{ChannelLink, ChannelRxBestEffort, ChannelRxReliable, SessionLeaseEvent};

use crate::core::{PeerId, ZInt};
use crate::link::Link;
use crate::proto::{SeqNumGenerator, SessionMessage, WhatAmI, ZenohMessage};
use crate::session::{MsgHandler, SessionManagerInner};
use crate::session::defaults::QUEUE_PRIO_DATA;

use zenoh_util::{zasyncread, zasyncwrite, zerror, zasyncopt};
use zenoh_util::collections::{TimedEvent, Timer};
use zenoh_util::core::{ZError, ZErrorKind, ZResult};


macro_rules! zlinkget {
    ($guard:expr, $link:expr) => {
        $guard.iter().find(|l| l.get_link() == $link)
    };
}

macro_rules! zlinkindex {
    ($guard:expr, $link:expr) => {
        $guard.iter().position(|l| l.get_link() == $link)
    };
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
    // The batch size 
    pub(super) batch_size: usize,
    // The callback has been set or not
    pub(super) has_callback: AtomicBool,
    // The sn generator for the TX reliable channel
    pub(super) tx_sn_reliable: Arc<Mutex<SeqNumGenerator>>,
    // The sn generator for the TX best_effort channel
    pub(super) tx_sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
    // The RX reliable channel
    pub(super) rx_reliable: Mutex<ChannelRxReliable>,
    // The RX best effort channel
    pub(super) rx_best_effort: Mutex<ChannelRxBestEffort>,
    // The links associated to the channel
    pub(super) links: Arc<RwLock<Vec<ChannelLink>>>,
    // The internal timer
    pub(super) timer: Timer,
    // The callback
    pub(super) callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
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
            tx_sn_reliable: Arc::new(Mutex::new(SeqNumGenerator::new(initial_sn_tx, sn_resolution))),
            tx_sn_best_effort: Arc::new(Mutex::new(SeqNumGenerator::new(initial_sn_tx, sn_resolution))),            
            rx_reliable: Mutex::new(ChannelRxReliable::new(sn_resolution, initial_sn_rx)),
            rx_best_effort: Mutex::new(ChannelRxBestEffort::new(sn_resolution, initial_sn_rx)),
            links: Arc::new(RwLock::new(Vec::new())),
            timer: Timer::new(),
            callback: RwLock::new(None),
            w_self: RwLock::new(None)
        }        
    }

    pub(crate) async fn initialize(&self, w_self: Weak<Self>) {
        // Initialize the weak reference to self
        *zasyncwrite!(self.w_self) = Some(w_self.clone());
        // Lease event
        let event = SessionLeaseEvent::new(w_self);
        // Keep alive interval is expressed in milliseconds
        let interval = Duration::from_millis(self.lease);
        let event = TimedEvent::periodic(interval, event);
        // Add the event to the timer
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
        let mut guard = zasyncwrite!(self.callback);
        *guard = Some(callback.clone());
        self.has_callback.store(true, Ordering::Relaxed);
    }

    /*************************************/
    /*           TERMINATION             */
    /*************************************/
    pub(super) async fn delete(&self) {
        // Delete the session on the manager
        let _ = self.manager.del_session(&self.pid).await;            

        // Notify the callback
        zasyncopt!(self.callback).close().await;
        
        // Close all the links
        let mut guard = zasyncwrite!(self.links);
        for l in guard.drain(..) {
            let _ = l.close().await;
        }
    }

    pub(crate) async fn close_link(&self, link: &Link, reason: u8) -> ZResult<()> {     
        log::trace!("Closing link {} with peer: {}", link, self.get_peer());

        let guard = zasyncread!(self.links);
        if let Some(l) = zlinkget!(guard, link) {
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

        // Close message to be sent on all the links
        let peer_id = Some(self.manager.config.pid.clone());
        let reason_id = reason;              
        let link_only = false;  // This is should always be false for user-triggered close              
        let attachment = None;  // No attachment here
        let msg = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

        let mut links = zasyncread!(self.links).clone();
        for link in links.drain(..) {
            link.schedule_session_message(msg.clone(), QUEUE_PRIO_DATA).await;
        }

        // Terminate and clean up the session
        self.delete().await;
        
        Ok(())
    }

    /*************************************/
    /*        SCHEDULE AND SEND TX       */
    /*************************************/
    /// Schedule a Zenoh message on the transmission queue
    pub(crate) async fn schedule(&self, message: ZenohMessage, link: Option<Link>) {
        if let Some(link) = link {
            let guard = zasyncread!(self.links);
            if let Some(l) = zlinkget!(guard, &link) {
                l.schedule_zenoh_message(message, QUEUE_PRIO_DATA).await;
            } else {
                log::warn!("Zenoh message has been dropped because link {} does not exist\
                            in session with peer {}: {}", link, self.get_peer(), message);
            }
        } else {
            let guard = zasyncread!(self.links);
            if guard.is_empty() {                
                log::warn!("Zenoh message has been dropped because session with peer {} has no links: {}", self.get_peer(), message);
            } else {
                guard[0].schedule_zenoh_message(message, QUEUE_PRIO_DATA).await;
            }
        }
    }

    /*************************************/
    /*               LINK                */
    /*************************************/    
    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        let mut guard = zasyncwrite!(self.links);
        if zlinkget!(guard, &link).is_some() {
            return zerror!(ZErrorKind::InvalidLink { 
                descr: format!("Can not add Link {} with peer: {}", link, self.get_peer())
            });
        }

        // Create a channel link from a link
        let link = ChannelLink::new(
            zasyncopt!(self.w_self).clone(), link, self.batch_size, self.keep_alive, self.lease,
            self.tx_sn_reliable.clone(), self.tx_sn_best_effort.clone(), self.timer.clone()
        );

        // Add the link to the channel
        guard.push(link);
        
        Ok(())
    }

    pub(crate) async fn del_link(&self, link: &Link) -> ZResult<()> { 
        // Try to remove the link
        let mut guard = zasyncwrite!(self.links);
        if let Some(index) = zlinkindex!(guard, link) {
            let link = guard.remove(index);
            link.close().await
        } else {
            zerror!(ZErrorKind::InvalidLink { 
                descr: format!("Can not delete Link {} with peer: {}", link, self.get_peer())
            })
        }
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        zasyncread!(self.links).iter().map(|l| l.get_link().clone()).collect()
    }
}