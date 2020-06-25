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
use std::collections::HashMap;

use crate::link::Link;

use zenoh_util::zerror;
use zenoh_util::collections::TimedHandle;
use zenoh_util::core::{ZResult, ZError, ZErrorKind};


// Store the mutable data that need to be used for transmission
#[derive(Clone)]
pub(super) struct ChannelLinks {
    pub(super) batch_size: usize,
    pub(super) links: HashMap<Link, TimedHandle>
}

impl ChannelLinks {
    pub(super) fn new(batch_size: usize) -> ChannelLinks {
        ChannelLinks {
            batch_size,
            links: HashMap::new()
        }
    }

    pub(super) fn add(&mut self, link: Link, handle: TimedHandle) -> ZResult<()> {
        // Check if this link is not already present
        if self.links.contains_key(&link) {
            let e = format!("Can not add the link to the channel because it is already associated: {}", link);
            log::trace!("{}", e);
            return zerror!(ZErrorKind::InvalidLink {
                descr: e
            })
        }

        // Add the link to the channel
        self.links.insert(link, handle);

        Ok(())
    }

    pub(super) fn get(&self) -> Vec<Link> {
        self.links.iter().map(|(l, _)| l.clone()).collect()
    }

    pub(super) fn del(&mut self, link: &Link) -> ZResult<TimedHandle> {
        // Remove the link from the channel
        if let Some(handle) = self.links.remove(link) {
            Ok(handle)
        } else {
            let e = format!("Can not delete the link from the channel because it does not exist: {}", link);
            log::trace!("{}", e);
            zerror!(ZErrorKind::InvalidLink {
                descr: format!("{}", link)
            })
        }
    }    

    pub(super) fn clear(&mut self) {
        self.links.clear()
    }
}