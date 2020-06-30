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
            let length = self.len() as LengthType;            
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
        to_fragment: &mut WBuf,
        to_write: usize
    ) -> (usize, bool) {
        // Assume first that this is not the final fragment
        let mut is_final = false;
        loop {            
            // Mark the buffer for the writing operation
            self.buffer.mark();
            // Write the frame header
            let fragment = Some(is_final);
            let attachment = None;
            let res = self.buffer.write_frame_header(reliable, sn, fragment, attachment);
            if res {
                // Compute the amount left
                let space_left = self.buffer.capacity() - self.buffer.len();        
                // Check if it is really the final fragment
                if !is_final && (to_write <= space_left) {
                    // Revert the buffer
                    self.buffer.revert();
                    // It is really the finally fragment
                    is_final = true;                    
                    continue
                }
                // Write
                let written = to_write.min(space_left);
                to_fragment.copy_into_wbuf(&mut self.buffer, written);

                return (written, is_final)
            }  else {
                // Revert the buffer
                self.buffer.revert();
                return (0, is_final)
            }                 
        }
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

#[cfg(test)]
mod tests {
    use async_std::task;

    use super::*;

    use crate::core::ResKey;
    use crate::io::{RBuf, WBuf};
    use crate::proto::{SeqNumGenerator, ZenohMessage};
    use crate::session::defaults::SESSION_SEQ_NUM_RESOLUTION;    

    use zenoh_util::zasynclock;


    async fn run() {
        let seq = [8, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_535];
        // Zenoh data
        for batch_size in seq.iter() {
            for is_streamed in [false, true].iter() {
                let sn_reliable = Arc::new(Mutex::new(
                    SeqNumGenerator::new(0, *SESSION_SEQ_NUM_RESOLUTION)
                ));
                let sn_best_effort = Arc::new(Mutex::new(
                    SeqNumGenerator::new(0, *SESSION_SEQ_NUM_RESOLUTION)
                ));
        
                let mut batch = SerializationBatch::new(*batch_size, *is_streamed, sn_reliable.clone(), sn_best_effort.clone());

                for payload_size in seq.iter() {
                    for reliable in [false, true].iter() {
                        let key = ResKey::RName("test".to_string());
                        let info = None;
                        let payload = RBuf::from(vec![0u8; *payload_size]);
                        let reply_context = None;
                        let attachment = None;

                        let message = ZenohMessage::make_data(
                            *reliable, key, info, payload, reply_context, attachment
                        );

                        if payload_size < batch_size {
                            // Fragmentation is not needed
                            let res = batch.serialize_zenoh_message(&message).await;
                            assert!(res);
                            println!("Batch size: {}\tStreamed: {}\tPayload size: {}\tSerialized bytes: {}", batch_size, is_streamed, payload_size, batch.len());
                            batch.clear();
                        } else {
                            // We need to fragment
                            let mut wbuf = WBuf::new(*batch_size, false); 
                            wbuf.write_zenoh_message(&message);
                            println!("Batch size: {}\tStreamed: {}\tPayload size: {}\tBytes to fragment: {}", batch_size, is_streamed, payload_size, wbuf.len());

                            // Acquire the lock on the sn generators to ensure that we have
                            // sequential sequence numbers for all the fragments
                            let mut guard = if message.is_reliable() {
                                zasynclock!(sn_reliable)
                            } else {
                                zasynclock!(sn_best_effort)
                            };

                            // Fragment the whole message
                            let mut to_write = wbuf.len();
                            while to_write > 0 {
                                let (written, is_final) = batch.serialize_zenoh_fragment(message.is_reliable(), guard.get(), &mut wbuf, to_write).await;
                                println!("\tSerialized fragment of {} bytes\tFinal: {}", written, is_final);
                                to_write -= written;
                                assert_ne!(written, 0);
                                batch.clear();                                
                            }
                        }
                    }                    
                } 
            }
        }  
    }

    #[test]
    fn serialization_batch() {        
        task::block_on(run());             
    }
}