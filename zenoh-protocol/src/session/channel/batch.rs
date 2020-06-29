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
use async_std::sync::{Arc, Mutex};

use crate::core::ZInt;
use crate::io::WBuf;
use crate::proto::{SeqNumGenerator, SessionMessage, ZenohMessage};


type LengthType = u16;
const LENGTH_BYTES: [u8; 2] = [0u8, 0u8];

#[derive(Clone, Debug)]
enum CurrentFrame {
    Reliable,
    BestEffort,
    None
}

#[derive(Clone, Debug)]
pub(super) struct SerializationBatch {
    // The buffer to perform the batching on
    buffer: WBuf,    
    // It is a streamed batch
    is_streamed: bool,
    // The link this batch is associated to
    current_frame: CurrentFrame,
    // The sn generators
    sn_reliable: Arc<Mutex<SeqNumGenerator>>,
    sn_best_effort: Arc<Mutex<SeqNumGenerator>>
}

impl SerializationBatch {
    pub(super) fn new(
        size: usize, 
        is_streamed: bool,
        sn_reliable: Arc<Mutex<SeqNumGenerator>>,
        sn_best_effort: Arc<Mutex<SeqNumGenerator>>
    ) -> SerializationBatch {
        let mut batch = SerializationBatch {
            buffer: WBuf::new(size, true),
            is_streamed,
            current_frame: CurrentFrame::None,
            sn_reliable,
            sn_best_effort
        };

        // Bring the batch in a clear state
        batch.clear();

        batch
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn len(&self) -> usize {
        let len = self.buffer.len();
        if self.is_streamed() {            
            len - LENGTH_BYTES.len()
        } else {
            len
        }
    }

    pub(super) fn is_streamed(&self) -> bool {
        self.is_streamed
    }

    pub(super) fn clear(&mut self) {
        self.buffer.clear();
        if self.is_streamed() {
            self.buffer.write_bytes(&LENGTH_BYTES);
        }
    }

    pub(super) fn write_len(&mut self) {
        if self.is_streamed() {
            let mut length = self.len() as LengthType;            
            let bits = self.buffer.get_first_slice_mut(..LENGTH_BYTES.len());
            bits.copy_from_slice(&length.to_le_bytes());
        }
    }

    pub(super) fn get_buffer(&self) -> &[u8] {
        self.buffer.get_first_slice(..)
    }

    pub(super) async fn serialize_zenoh_fragment(
        &mut self, 
        reliable: bool, 
        sn: ZInt,
        buffer: &mut WBuf
    ) -> bool {
        // Compute the amount left
        let space_left = self.buffer.capacity() - self.buffer.len();        
        let still_to_write = buffer.len();

        // Check if it is the final fragment
        let is_final = if still_to_write > space_left {
            false
        } else {
            true
        };

        // Compute the right amount of bytes to write
        let to_write = still_to_write.min(space_left);

        // Mark the write operation
        self.buffer.mark();
        // Write the frame header
        let fragment = Some(is_final);
        let attachment = None;
        let res = self.buffer.write_frame_header(reliable, sn, fragment, attachment);
        if res {
            // Write a fragment of the ZenohMessage
            buffer.copy_into_wbuf(&mut self.buffer, to_write);
        } else {
            // Revert the write operation
            self.buffer.revert();
        }

        res
    }

    pub(super) async fn serialize_zenoh_message(&mut self, message: &ZenohMessage) -> bool {
        // Keep track of eventual new frame and new sn
        let mut new_frame = None;

        // Eventually update the current frame and sn based on the current status
        match self.current_frame {
            CurrentFrame::Reliable => if !message.is_reliable() {
                // A new best-effort frame needs to be started
                new_frame = Some(CurrentFrame::BestEffort);
            }, 
            CurrentFrame::BestEffort => if message.is_reliable() {    
                // A new reliable frame needs to be started
                new_frame = Some(CurrentFrame::Reliable);
            },
            CurrentFrame::None => if message.is_reliable() {
                // A new reliable frame needs to be started
                new_frame = Some(CurrentFrame::Reliable);
            } else {
                // A new best-effort frame needs to be started
                new_frame = Some(CurrentFrame::BestEffort);
            }
        }

        // Mark the write operation
        self.buffer.mark();

        // If a new sequence number has been provided, it means we are in the case we need 
        // to start a new frame. Write a new frame header.
        let res = if let Some(frame) = new_frame {
            // Acquire the lock on the sn generator
            let mut guard = if message.is_reliable() {
                zasynclock!(self.sn_reliable)
            } else {
                zasynclock!(self.sn_best_effort)
            };
            // Get a new sequence number
            let sn = guard.get();            

            // Serialize the new frame and the zenoh message
            let res = self.buffer.write_frame_header(message.is_reliable(), sn, None, None)
                        && self.buffer.write_zenoh_message(&message);
            if res {
                self.current_frame = frame;
            } else {
                // Restore the sequence number          
                guard.set(sn);
            }
            // Drop the guard
            drop(guard);
            // Return
            res
        } else {
            self.buffer.write_zenoh_message(&message)
        };

        if !res {
            // Revert the write operation
            self.buffer.revert();
        }
        
        res
    }

    pub(super) async fn serialize_session_message(&mut self, message: &SessionMessage) -> bool {
        self.current_frame = CurrentFrame::None;
        self.buffer.write_session_message(&message)
    }
}