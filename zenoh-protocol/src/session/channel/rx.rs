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
use async_std::sync::Arc;
use async_trait::async_trait;
use std::collections::HashSet;

use super::Channel;

use crate::core::{PeerId, ZInt};
use crate::link::Link;
use crate::proto::{FramePayload, SessionBody, SessionMessage, SeqNum};
use crate::session::{Action, MsgHandler, TransportTrait};

use zenoh_util::zasynclock;



/*************************************/
/*     CHANNEL INNER RX STRUCT       */
/*************************************/

// Structs to manage the sequence numbers of channels
pub(super) struct SeqNumRx {
    pub(super) reliable: SeqNum,
    pub(super) best_effort: SeqNum,
}

impl SeqNumRx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt) -> SeqNumRx {
        // Set the sequence number in the state as it had 
        // received a message with initial_sn - 1
        let initial_sn = if initial_sn == 0 {
            sn_resolution
        } else {
            initial_sn - 1
        };
        SeqNumRx {
            reliable: SeqNum::make(initial_sn, sn_resolution).unwrap(),
            best_effort: SeqNum::make(initial_sn, sn_resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
pub(super) struct ChannelInnerRx {    
    pub(super) sn: SeqNumRx,
    pub(super) alive: HashSet<Link>,
    pub(super) callback: Option<Arc<dyn MsgHandler + Send + Sync>>
}

impl ChannelInnerRx {
    pub(super) fn new(
        sn_resolution: ZInt,
        initial_sn: ZInt
    ) -> ChannelInnerRx {
        ChannelInnerRx {
            sn: SeqNumRx::new(sn_resolution, initial_sn),
            alive: HashSet::new(),
            callback: None
        }
    }
}

impl Channel {
    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    async fn process_reliable_frame(&self, sn: ZInt, payload: FramePayload) -> Action {
        // @TODO: Implement the reordering and reliability. Wait for missing messages.
        let mut guard = zasynclock!(self.rx);        
        if !(guard.sn.reliable.precedes(sn) && guard.sn.reliable.set(sn).is_ok()) {
            log::warn!("Reliable frame with invalid SN dropped: {}", sn);
            return Action::Read
        }

        let callback = if let Some(callback) = &guard.callback {
            callback
        } else {
            log::error!("Reliable frame dropped because callback is unitialized: {:?}", payload);
            return Action::Read
        };

        match payload {
            FramePayload::Fragment { .. } => {
                unimplemented!("Fragmentation not implemented");
            },
            FramePayload::Messages { mut messages } => {
                for msg in messages.drain(..) {
                    log::trace!("Session: {}. Message: {:?}", self.get_peer(), msg);
                    let _ = callback.handle_message(msg).await;
                }
            }
        }
        
        Action::Read
    }

    async fn process_best_effort_frame(&self, sn: ZInt, payload: FramePayload) -> Action {
        let mut guard = zasynclock!(self.rx);
        if !(guard.sn.best_effort.precedes(sn) && guard.sn.best_effort.set(sn).is_ok()) {
            log::warn!("Best-effort frame with invalid SN dropped: {}", sn);
            return Action::Read
        }

        let callback = if let Some(callback) = &guard.callback {
            callback
        } else {
            log::error!("Best-effort frame dropped because callback is unitialized: {:?}", payload);
            return Action::Read
        };

        match payload {
            FramePayload::Fragment { .. } => {
                unimplemented!("Fragmentation not implemented");
            },
            FramePayload::Messages { mut messages } => {
                for msg in messages.drain(..) {
                    log::trace!("Session: {}. Message: {:?}", self.get_peer(), msg);
                    let _ = callback.handle_message(msg).await;
                }              
            }
        }
        
        Action::Read
    }

    async fn process_close(&self, link: &Link, pid: Option<PeerId>, reason: u8, link_only: bool) -> Action {
        // Check if the PID is correct when provided
        if let Some(pid) = pid {
            if pid != self.pid {
                log::warn!("Received an invalid Close on link {} from peer {} with reason: {}. Ignoring.", link, pid, reason);
                return Action::Read
            }
        }        
        
        if link_only {
            // Delete only the link but keep the session open
            let _ = self.del_link(link).await;
            // Close the link
            let _ = link.close().await;
        } else { 
            // Close the whole session 
            self.delete().await;
        }
        
        Action::Close
    }

    async fn process_keep_alive(&self, link: &Link, pid: Option<PeerId>) -> Action {
        // Check if the PID is correct when provided
        if let Some(pid) = pid {
            if pid != self.pid {
                log::warn!("Received an invalid KeepAlive on link {} from peer: {}. Ignoring.", link, pid);
                return Action::Read
            }
        } 

        let mut guard = zasynclock!(self.rx);
        // Add the link to the list of alive links
        guard.alive.insert(link.clone());

        Action::Read
    }
}

#[async_trait]
impl TransportTrait for Channel {
    async fn receive_message(&self, link: &Link, message: SessionMessage) -> Action {
        log::trace!("Received from peer {} on link {}: {:?}", self.get_peer(), link, message);
        match message.body {
            SessionBody::Frame { ch, sn, payload } => {
                match ch {
                    true => self.process_reliable_frame(sn, payload).await,
                    false => self.process_best_effort_frame(sn, payload).await
                }
            },
            SessionBody::AckNack { .. } => {
                unimplemented!("Handling of AckNack Messages not yet implemented!");
            },
            SessionBody::Close { pid, reason, link_only } => {
                self.process_close(link, pid, reason, link_only).await
            },
            SessionBody::Hello { .. } => {
                unimplemented!("Handling of Hello Messages not yet implemented!");
            },
            SessionBody::KeepAlive { pid } => {
                self.process_keep_alive(link, pid).await
            },            
            SessionBody::Ping { .. } => {
                unimplemented!("Handling of Ping Messages not yet implemented!");
            },
            SessionBody::Pong { .. } => {
                unimplemented!("Handling of Pong Messages not yet implemented!");
            },
            SessionBody::Scout { .. } => {
                unimplemented!("Handling of Scout Messages not yet implemented!");
            },
            SessionBody::Sync { .. } => {
                unimplemented!("Handling of Sync Messages not yet implemented!");
            },            
            SessionBody::Open { .. } |
            SessionBody::Accept { .. } => {
                log::debug!("Unexpected Open/Accept message received in an already established session\
                             Closing the link: {}", link);
                Action::Close
            }
        }        
    }

    async fn link_err(&self, link: &Link) {
        log::warn!("Unexpected error on link {} with peer: {}", link, self.get_peer());
        let _ = self.del_link(link).await;
        let _ = link.close().await;
    }
}
