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
use async_std::sync::{Arc, Barrier, Mutex};
use async_std::task;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::SerializationBatch;

use crate::io::WBuf;
use crate::link::Link;
use crate::proto::{SeqNumGenerator, SessionMessage, ZenohMessage};
use crate::session::defaults::{
    // Configurable constants
    QUEUE_CONCURRENCY
};

use zenoh_util::zasynclock;
use zenoh_util::collections::FifoQueue;


macro_rules! zprioflag {
    ($prio:expr) => {
        1 << $prio
    };
}

macro_rules! zpriomask {
    ($prio:expr) => {
        !(usize::MAX << $prio)
    };
}

macro_rules! zbatchsend {
    ($state:expr, $mask:expr, $link:expr, $batch:expr) => {
        if !$batch.is_empty() {
            // Write the batch len if needed
            $batch.write_len();

            // Yield this task if there are more urgent messages to transmit
            while $state.load(Ordering::Acquire) & $mask != 0 {
                task::yield_now().await;
            }
            
            // Transmit the batch
            let _ = zasynclock!($link).send($batch.get_buffer()).await;
            // Clear the batch
            $batch.clear();
        }
    };
}

async fn serialize_session_message(
    message: &SessionMessage, 
    batch: &mut SerializationBatch,
    link: &Arc<Mutex<Link>>,
    state: &Arc<AtomicUsize>,
    prio_mask: usize
) -> bool { 
    // Attempt the first serialization
    if batch.serialize_session_message(&message).await {
        return true
    }

    // Send the current batch
    zbatchsend!(state, prio_mask, link, batch);

    // Attempt the second serialization
    if batch.serialize_session_message(&message).await {
        return true
    }

    false
}

async fn serialize_zenoh_message(
    message: &ZenohMessage,
    batch: &mut SerializationBatch,
    link: &Arc<Mutex<Link>>,
    state: &Arc<AtomicUsize>,
    prio_mask: usize,
    sn_reliable: &Arc<Mutex<SeqNumGenerator>>,
    sn_best_effort: &Arc<Mutex<SeqNumGenerator>>
) -> bool { 
    // Attempt the first serialization
    if batch.serialize_zenoh_message(&message).await {
        return true
    }

    // Send the current buffer
    zbatchsend!(state, prio_mask, link, batch);

    // Attempt the second serialization
    if batch.serialize_zenoh_message(&message).await {
        return true
    }

    fragment_zenoh_message(&message, batch, link, state, prio_mask, sn_reliable, sn_best_effort).await
}

async fn fragment_zenoh_message(
    message: &ZenohMessage, 
    batch: &mut SerializationBatch,
    link: &Arc<Mutex<Link>>,
    state: &Arc<AtomicUsize>,
    prio_mask: usize,
    sn_reliable: &Arc<Mutex<SeqNumGenerator>>,
    sn_best_effort: &Arc<Mutex<SeqNumGenerator>>
) -> bool {
    // Need to fragment
    let mut wbuf = WBuf::new(batch.capacity(), false); 
    wbuf.write_zenoh_message(&message);

    // Keep the lock on the SN generator until the whole message has been fragmented
    let mut sn_gen = if message.is_reliable() {
        zasynclock!(sn_reliable)
    } else {
        zasynclock!(sn_best_effort)
    };    

    let mut to_write = wbuf.len();
    while to_write > 0 {
        // Get the frame SN
        let sn = sn_gen.get();

        // Serialize the message
        let written = batch.serialize_zenoh_fragment(message.is_reliable(), sn, &mut wbuf, to_write);        

        if written != 0 {            
            zbatchsend!(state, prio_mask, link, batch);
            // Update the amount left to fragment
            to_write -= written;            
        } else { 
            // 0 bytes written means error
            log::warn!("Zenoh message dropped because it can not be fragmented: {:?}", message);
            // Reinsert the SN back to the pool
            sn_gen.set(sn);    
            // Clear the batch
            batch.clear();                
            return false
        }
    }
    
    return true
}

async fn consume_task(
    queue: Arc<FifoQueue<MessageTx>>,
    // The queue capacity
    capacity: usize,
    // The queue priority
    priority: usize,    
    // More important messages are coming
    state: Arc<AtomicUsize>,
    // The link to send the message on
    link: Arc<Mutex<Link>>,
    // The size and kind of the batch
    mut batch: SerializationBatch,
    // The sequence number generators
    sn_reliable: Arc<Mutex<SeqNumGenerator>>,
    sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
    // The barrier for synchronization upon termination
    barrier: Arc<Barrier>
) {
    // Compute the bit-flag of the priority
    let not_prio_flag = !zprioflag!(priority);
    // Compute the mask for other queue with higher priorities
    let prio_mask = zpriomask!(priority);

    // Allocate the buffer to drain the messages on
    let mut buffer: Vec<MessageTx> = Vec::with_capacity(capacity);
    loop {
        // Get a drain for all the data messages
        let mut drainer = queue.drain().await;        

        loop {
            // Drain all the data messages
            for msg in &mut drainer {
                buffer.push(msg);
            }
            drainer.drop().await;

            // Serialize and transmit all the messages
            for msg in buffer.drain(..) {
                match msg {
                    MessageTx::Zenoh(m) => if !serialize_zenoh_message(
                        &m, &mut batch, &link, &state, prio_mask, &sn_reliable, &sn_best_effort
                    ).await {
                        log::warn!("Zenoh message dropped because it can not be fragmented: {:?}", m);
                    },
                    MessageTx::Session(m) => if !serialize_session_message(
                        &m, &mut batch, &link, &state, prio_mask
                    ).await {
                        log::warn!("Session message dropped because it can not be fragmented: {:?}", m);
                    },                
                    MessageTx::Stop => {
                        // Send any remaining buffer
                        zbatchsend!(state, prio_mask, link, batch);
                        // Mark that we don't have any other message to send on this queue
                        state.fetch_and(not_prio_flag, Ordering::AcqRel);
                        // Synchronize the termination
                        barrier.wait().await;
                        return
                    }
                }
            }  
            
            // Try to drain more messages
            drainer = queue.try_drain().await;
            let (min, _) = drainer.size_hint();
            if min == 0 {
                // Send any remaining buffer
                zbatchsend!(state, prio_mask, link, batch);
                // Mark that we don't have any other message to send on this queue
                state.fetch_and(not_prio_flag, Ordering::AcqRel);
                break
            }
        }                                 
    }
}

pub enum MessageTx {
    Session(SessionMessage),
    Zenoh(ZenohMessage),
    Stop
}

/// Link queue
pub struct TransmissionQueue {    
    // A single Mutex for all the priority queues
    queue: Arc<FifoQueue<MessageTx>>,
    // More important messages are coming
    state: Arc<AtomicUsize>,
    prio_flag: usize,
    barrier: Arc<Barrier>
}

impl TransmissionQueue {
    /// Create a new link queue.
    pub fn new(
        capacity: usize,
        priority: usize,
        batch_size: usize,
        is_streamed: bool,
        link: Arc<Mutex<Link>>,
        state: Arc<AtomicUsize>,
        sn_reliable: Arc<Mutex<SeqNumGenerator>>,
        sn_best_effort: Arc<Mutex<SeqNumGenerator>>
    ) -> TransmissionQueue {
        let queue = Arc::new(FifoQueue::new(capacity, *QUEUE_CONCURRENCY));
        let barrier = Arc::new(Barrier::new(2));

        let c_queue = queue.clone();
        let c_state = state.clone();
        let c_barrier = barrier.clone();        
        task::spawn(async move {
            // Allocate the serialization batch
            let batch = SerializationBatch::new(batch_size, is_streamed, sn_reliable.clone(), sn_best_effort.clone());
            // Start the consume task
            consume_task(
                c_queue, capacity, priority, c_state, link, batch, sn_reliable, sn_best_effort, c_barrier
            ).await;
        });

        TransmissionQueue {
            queue,
            state,
            prio_flag: zprioflag!(priority),
            barrier
        }
    }

    #[inline]
    pub(super) async fn push_session_message(&self, message: SessionMessage) {
        self.queue.push(MessageTx::Session(message)).await;
        self.state.fetch_or(self.prio_flag, Ordering::AcqRel);
    }

    #[inline]
    pub(super) async fn push_zenoh_message(&self, message: ZenohMessage) {
        self.queue.push(MessageTx::Zenoh(message)).await;
        self.state.fetch_or(self.prio_flag, Ordering::AcqRel);
    }

    #[inline]
    pub(super) async fn stop(&self) {
        self.queue.push(MessageTx::Stop).await;
        self.barrier.wait().await;
    }
}

// #[cfg(test)]
// mod tests {
//     use async_std::sync::{Arc, Mutex};
//     use async_std::task;
//     use std::sync::atomic::{AtomicUsize, Ordering};
//     use std::time::Duration;

//     use crate::core::ResKey;
//     use crate::io::RBuf;
//     use crate::proto::{FramePayload, SeqNumGenerator, SessionBody, ZenohMessage};
//     use crate::session::defaults::{QUEUE_PRIO_DATA, SESSION_BATCH_SIZE, SESSION_SEQ_NUM_RESOLUTION};

//     use super::*;

//     #[test]
//     fn transmission_queue() {        
//         async fn schedule(
//             num_msg: usize,
//             payload_size: usize,
//             queue: Arc<TransmissionQueue>
//         ) {
//             // Send reliable messages
//             let reliable = true;
//             let key = ResKey::RName("test".to_string());
//             let info = None;
//             let payload = RBuf::from(vec![0u8; payload_size]);
//             let reply_context = None;
//             let attachment = None;

//             let message = ZenohMessage::make_data(
//                 reliable, key, info, payload, reply_context, attachment
//             );
            
//             for _ in 0..num_msg {
//                 queue.push_zenoh_message(message.clone(), QUEUE_PRIO_DATA).await;
//             }            
//         }

//         async fn consume(
//             queue: Arc<TransmissionQueue>, 
//             c_batches: Arc<AtomicUsize>, 
//             c_bytes: Arc<AtomicUsize>, 
//             c_messages: Arc<AtomicUsize>,
//             c_fragments: Arc<AtomicUsize>
//         ) {
//             loop {
//                 let (batch, priority) = queue.pull().await;   
//                 c_batches.fetch_add(1, Ordering::Relaxed);
//                 c_bytes.fetch_add(batch.len(), Ordering::Relaxed);           
//                 // Create a RBuf for deserialization starting from the batch
//                 let mut rbuf: RBuf = batch.get_serialized_messages().into();
//                 // Deserialize the messages
//                 while let Ok(msg) = rbuf.read_session_message() {
//                     match msg.body {
//                         SessionBody::Frame { payload, .. } => match payload {
//                             FramePayload::Messages { messages } => {
//                                 c_messages.fetch_add(messages.len(), Ordering::Relaxed);
//                             },
//                             FramePayload::Fragment { is_final, .. } => {
//                                 c_fragments.fetch_add(1, Ordering::Relaxed);
//                                 if is_final {
//                                     c_messages.fetch_add(1, Ordering::Relaxed);
//                                 }
//                             }
//                         },
//                         _ => { c_messages.fetch_add(1, Ordering::Relaxed); }
//                     }                 
//                 }
//                 // Reinsert the batch
//                 queue.push_serialization_batch(batch, priority).await;
//             }     
//         }

//         // Queue
//         let batch_size = *SESSION_BATCH_SIZE;
//         let is_streamed = true;
//         let sn_reliable = Arc::new(Mutex::new(
//             SeqNumGenerator::new(0, *SESSION_SEQ_NUM_RESOLUTION)
//         ));
//         let sn_best_effort = Arc::new(Mutex::new(
//             SeqNumGenerator::new(0, *SESSION_SEQ_NUM_RESOLUTION)
//         ));
//         let queue = Arc::new(TransmissionQueue::new(
//             batch_size, is_streamed, sn_reliable, sn_best_effort
//         ));       

//         // Counters
//         let counter_batches = Arc::new(AtomicUsize::new(0));
//         let counter_bytes = Arc::new(AtomicUsize::new(0));
//         let counter_messages = Arc::new(AtomicUsize::new(0));
//         let counter_fragments = Arc::new(AtomicUsize::new(0));

//         // Consume task
//         let c_queue = queue.clone();
//         let c_batches = counter_batches.clone();
//         let c_bytes = counter_bytes.clone();
//         let c_messages = counter_messages.clone();
//         let c_fragments = counter_fragments.clone();
//         task::spawn(async move {
//             consume(c_queue, c_batches, c_bytes, c_messages, c_fragments).await;
//         });
                
//         // Total amount of bytes to send in each test
//         let bytes: usize = 100_000_000;
//         let max_msgs: usize = 1_000;
//         // Paylod size of the messages
//         // let payload_sizes = [8, 64, 512, 1_024, 4_096, 8_192, 32_768, 262_144, 2_097_152];
//         let payload_sizes = [1_024, 4_096, 8_192, 32_768, 262_144, 2_097_152];
//         // Sleep time for reading the number of received messages after scheduling completion
//         let sleep = Duration::from_millis(1_000);
//         task::block_on(async {
//             for ps in payload_sizes.iter() {                
//                 let num_msg = max_msgs.min(bytes / ps);
//                 println!(">>> Sending {} messages with payload size: {}", num_msg, ps);
//                 schedule(num_msg, *ps, queue.clone()).await;
//                 task::sleep(sleep).await;

//                 let messages = counter_messages.swap(0, Ordering::Relaxed);
//                 let batches = counter_batches.swap(0, Ordering::Relaxed);
//                 let bytes = counter_bytes.swap(0, Ordering::Relaxed);    
//                 let fragments = counter_fragments.swap(0, Ordering::Relaxed);            
//                 println!("    Received {} messages, {} bytes, {} batches, {} fragments", messages, bytes, batches, fragments);
//                 assert_eq!(num_msg, messages);  
//             }    
//         }); 
//     }
// }