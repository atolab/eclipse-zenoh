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
use async_std::sync::Weak;
use async_trait::async_trait;
use std::collections::HashSet;
use std::iter::FromIterator;

use super::{Channel, MessageInner, MessageTx};

use crate::link::Link;
use crate::proto::{SessionMessage, smsg};
use crate::session::defaults::QUEUE_PRIO_CTRL;

use zenoh_util::zasynclock;
use zenoh_util::collections::Timed;

/*************************************/
/*            KEEP ALIVE             */
/*************************************/
pub(super) struct KeepAliveEvent {
    ch: Weak<Channel>,
    link: Link
}

impl KeepAliveEvent {
    pub(super) fn new(ch: Weak<Channel>, link: Link) -> KeepAliveEvent {
        KeepAliveEvent {
            ch,
            link
        }
    }
}

#[async_trait]
impl Timed for KeepAliveEvent {
    async fn run(&mut self) {
        log::trace!("Schedule KEEP_ALIVE messages for link: {}", self.link);        
        if let Some(ch) = self.ch.upgrade() {
            // Create the KEEP_ALIVE message
            let pid = None;
            let attachment = None;
            let message = MessageTx {
                inner: MessageInner::Session(SessionMessage::make_keep_alive(pid, attachment)),
                link: Some(self.link.clone())
            };

            // Push the KEEP_ALIVE messages on the queue
            ch.queue.push(message, *QUEUE_PRIO_CTRL).await;
        }
    }
}

/*************************************/
/*          SESSION LEASE            */
/*************************************/
pub(super) struct LeaseEvent {
    ch: Weak<Channel>
}

impl LeaseEvent {
    pub(super) fn new(ch: Weak<Channel>) -> LeaseEvent {
        LeaseEvent {
            ch
        }
    }
}

#[async_trait]
impl Timed for LeaseEvent {
    async fn run(&mut self) {        
        if let Some(ch) = self.ch.upgrade() {
            log::trace!("Verify session lease for peer: {}", ch.get_peer());

            // Create the set of current links
            let links: HashSet<Link> = HashSet::from_iter(ch.get_links().await.drain(..));
            // Get and reset the current status of active links
            let alive: HashSet<Link> = HashSet::from_iter(zasynclock!(ch.rx).alive.drain());
            // Create the difference set
            let mut difference: HashSet<Link> = HashSet::from_iter(links.difference(&alive).cloned());

            if links == difference {
                // We have no links left or all the links have expired: close the whole session
                log::warn!("Session with peer {} has expired", ch.get_peer());                
                let _ = ch.close(smsg::close_reason::EXPIRED).await;
            } else {
                // Remove only the links with expired lease
                for l in difference.drain() {
                    log::warn!("Link with peer {} has expired: {}", ch.get_peer(), l);
                    let _ = ch.close_link(&l, smsg::close_reason::EXPIRED).await;                
                }
            }            
        }
    }
}