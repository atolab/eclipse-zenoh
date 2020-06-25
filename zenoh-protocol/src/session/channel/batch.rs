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
use crate::io::WBuf;
use crate::link::Link;

use zenoh_util::core::{ZResult};


const TWO_BYTES: [u8; 2] = [0u8, 0u8];

#[derive(Debug)]
pub(super) struct SerializedBatch {
    // The buffer to perform the batching on
    pub(super) buffer: WBuf,
    // The link this batch is associated to
    pub(super) link: Link
}

impl SerializedBatch {
    pub(super) fn new(link: Link, size: usize) -> SerializedBatch {        
        // Create the buffer
        let mut buffer = WBuf::new(size, true);
        // Reserve two bytes if the link is streamed
        if link.is_streamed() {
            buffer.write_bytes(&TWO_BYTES);
        }

        SerializedBatch {
            buffer,
            link
        }
    }

    pub(super) fn clear(&mut self) {
        self.buffer.clear();
        if self.link.is_streamed() {
            self.buffer.write_bytes(&TWO_BYTES);
        }
    }

    pub(super) async fn transmit(&mut self) -> ZResult<()> {
        let mut length: u16 = self.buffer.len() as u16;
        if self.link.is_streamed() {
            // Remove from the total the 16 bits used for the length
            length -= 2;            
            // Write the length on the first 16 bits
            let bits = self.buffer.get_first_slice_mut(..2);
            bits.copy_from_slice(&length.to_le_bytes());
        }

        if length > 0 {
            let res = self.link.send(self.buffer.get_first_slice(..)).await;
            self.clear();            
            return res
        }

        Ok(())
    }
}