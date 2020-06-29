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
use std::collections::VecDeque;

use super::SerializationBatch;

use crate::io::WBuf;
use crate::proto::{SeqNumGenerator, SessionMessage, ZenohMessage};
use crate::session::defaults::{
    // Constants
    QUEUE_NUM,
    QUEUE_PRIO_CTRL,
    QUEUE_PRIO_RETX,
    QUEUE_PRIO_DATA,
    // Configurable constants
    QUEUE_SIZE_CTRL,   
    QUEUE_SIZE_RETX,   
    QUEUE_SIZE_DATA,
    QUEUE_CONCURRENCY
};

use zenoh_util::zasynclock;
use zenoh_util::sync::Condition;


struct CircularBatchIn {
    index: usize,
    batch_size: usize,
    inner: VecDeque<SerializationBatch>,
    sn_reliable: Arc<Mutex<SeqNumGenerator>>,
    sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
    state_out: Arc<Mutex<Vec<CircularBatchOut>>>,
    state_empty: Arc<Mutex<CircularBatchEmpty>>,
    not_full: Arc<Condition>,
    not_empty: Arc<Condition>
}

macro_rules! zrefill {
    ($batch:expr) => {
        // Refill the batches
        let mut empty_guard = zasynclock!($batch.state_empty);
        if empty_guard.is_empty() {
            // Drop the guard and wait for the batches to be available
            $batch.not_full.wait(empty_guard).await;
            // We have been notified that there are batches available:
            // reacquire the lock
            empty_guard = zasynclock!($batch.state_empty);
        }
        // Drain all the empty batches
        while let Some(batch) = empty_guard.pull() {
            $batch.inner.push_back(batch);
        }
    };
}


impl CircularBatchIn {
    fn new(
        index: usize,
        capacity: usize,
        batch_size: usize,
        is_streamed: bool,
        sn_reliable: Arc<Mutex<SeqNumGenerator>>,
        sn_best_effort: Arc<Mutex<SeqNumGenerator>>,
        state_out: Arc<Mutex<Vec<CircularBatchOut>>>,
        state_empty: Arc<Mutex<CircularBatchEmpty>>,
        not_full: Arc<Condition>,
        not_empty: Arc<Condition>
    ) -> CircularBatchIn {
        let mut inner = VecDeque::<SerializationBatch>::with_capacity(capacity);
        for _ in 0..capacity { 
            inner.push_back(SerializationBatch::new(
                batch_size, is_streamed, sn_reliable.clone(), sn_best_effort.clone()
            ));
        }

        CircularBatchIn {
            index,
            batch_size,
            inner,
            sn_reliable,
            sn_best_effort,
            state_out,
            state_empty,
            not_full,
            not_empty
        }
    }    

    async fn try_serialize_session_message(&mut self, message: &SessionMessage) -> bool {
        loop {
            // Get the current serialization batch
            let batch = if let Some(batch) = self.inner.front_mut() {
                batch
            } else {
                // Refill the batches
                zrefill!(self);
                continue
            };

            // Try to serialize the message on the current batch
            return batch.serialize_session_message(&message).await
        }
    }
    
    async fn serialize_session_message(&mut self, message: SessionMessage) { 
        // Attempt the serialization on the current batch
        if self.try_serialize_session_message(&message).await {
            return
        }

        // The first serialization attempt has failed. This means that the current
        // batch is full. Therefore:
        //   1) remove the current batch from the IN pipeline
        //   2) add the batch to the OUT pipeline        
        if let Some(batch) = self.pull() {
            // The previous batch wasn't empty
            let mut guard = zasynclock!(self.state_out);
            guard[self.index].push(batch);
            // Notify if needed
            if self.not_empty.has_waiting_list() {
                self.not_empty.notify(guard).await;
            } else {
                drop(guard);
            }

            // Attempt the serialization on a new empty batch
            if self.try_serialize_session_message(&message).await {
                return
            }
        }

        log::warn!("Session message dropped because it can not be fragmented: {:?}", message);
    }

    async fn fragment_zenoh_message(&mut self, message: &ZenohMessage) {
        // Create an expandable buffer and serialize the totality of the message
        let mut wbuf = WBuf::new(self.batch_size, false); 
        wbuf.write_zenoh_message(&message);

        // Acquire the lock on the SN generator to ensure that we have all 
        // sequential sequence numbers for the fragments
        let mut guard = if message.is_reliable() {
            zasynclock!(self.sn_reliable)
        } else {
            zasynclock!(self.sn_best_effort)
        };

        // Fragment the whole message
        while !wbuf.is_empty() {
            // Get the current serialization batch
            let batch = if let Some(batch) = self.inner.front_mut() {
                batch
            } else {
                // Refill the batches
                zrefill!(self);
                continue
            };

            let res = batch.serialize_zenoh_fragment(message.is_reliable(), guard.get(), &mut wbuf).await;

            if res {
                // Move the serialization batch into the OUT pipeline
                let batch = self.inner.pop_front().unwrap();
                let mut guard = zasynclock!(self.state_out);
                guard[self.index].push(batch);
                // Notify if needed
                if self.not_empty.has_waiting_list() {
                    self.not_empty.notify(guard).await;
                }
            } else {
                log::warn!("Zenoh message dropped because it can not be fragmented: {:?}", message);
                break
            }
        }
    }

    async fn try_serialize_zenoh_message(&mut self, message: &ZenohMessage) -> bool {
        loop {
            // Get the current serialization batch
            let batch = if let Some(batch) = self.inner.front_mut() {
                batch
            } else {
                // Refill the batches
                zrefill!(self);
                continue
            };

            // Try to serialize the message on the current batch
            return batch.serialize_zenoh_message(&message).await
        }
    }

    async fn serialize_zenoh_message(&mut self, message: ZenohMessage) { 
        // Attempt the serialization on the current batch
        if self.try_serialize_zenoh_message(&message).await {
            // Notify if needed
            if self.not_empty.has_waiting_list() {
                let batch = self.inner.pop_front().unwrap();
                let mut guard = zasynclock!(self.state_out);
                guard[self.index].push(batch);
                self.not_empty.notify(guard).await;
            }
            return
        }

        // The first serialization attempt has failed. This means that the current
        // batch is either full or the message is too large. In case of the former,
        // try to do:
        //   1) remove the current batch from the IN pipeline
        //   2) add the batch to the OUT pipeline        
        if let Some(batch) = self.pull() {
            // The previous batch wasn't empty
            let mut guard = zasynclock!(self.state_out);
            guard[self.index].push(batch);
            // Notify if needed
            if self.not_empty.has_waiting_list() {
                self.not_empty.notify(guard).await;
            } else {
                drop(guard);
            }

            // Attempt the serialization on a new empty batch
            if self.try_serialize_zenoh_message(&message).await {
                return
            }
        }

        // The second serialization attempt has failed. This means that the message is 
        // too large for the current batch size: we need to fragment.
        self.fragment_zenoh_message(&message).await;
    }

    fn pull(&mut self) -> Option<SerializationBatch> {
        if let Some(batch) = self.inner.front() {
            if !batch.is_empty() {
                // There is an incomplete batch, pop it
                return self.inner.pop_front()
            } 
        }
        None
    }
}

struct CircularBatchOut {
    inner: VecDeque<SerializationBatch>
}

impl CircularBatchOut {
    fn new(capacity: usize) -> CircularBatchOut {
        CircularBatchOut {
            inner: VecDeque::<SerializationBatch>::with_capacity(capacity)
        }
    }
    
    #[inline]
    pub(crate) fn push(&mut self, mut batch: SerializationBatch) {
        batch.write_len();
        self.inner.push_back(batch);        
    }

    pub(crate) fn pull(&mut self) -> Option<SerializationBatch> {
        self.inner.pop_front()    
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

struct CircularBatchEmpty {
    inner: VecDeque<SerializationBatch>,
}

impl CircularBatchEmpty {
    fn new(capacity: usize) -> CircularBatchEmpty {
        CircularBatchEmpty {            
            inner: VecDeque::<SerializationBatch>::with_capacity(capacity)
        }
    }

    #[inline]
    fn push(&mut self, mut batch: SerializationBatch) {
        batch.clear();
        self.inner.push_back(batch);
    }

    #[inline]
    fn pull(&mut self) -> Option<SerializationBatch> {
        self.inner.pop_front()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// Link queue
pub struct LinkQueue {
    // Each priority queue has its own Mutex
    state_in: Vec<Arc<Mutex<CircularBatchIn>>>,
    // Each priority queue has its own Mutex
    state_empty: Vec<Arc<Mutex<CircularBatchEmpty>>>,
    // A single Mutex for all the priority queues
    state_out: Arc<Mutex<Vec<CircularBatchOut>>>,
    // Each priority queue has its own Conditional variable
    // The conditional variable requires a MutexGuard from state_empty
    not_full: Vec<Arc<Condition>>,
    // A signle confitional variable for all the priority queues
    // The conditional variable requires a MutexGuard from state_out
    not_empty: Arc<Condition>
}

impl LinkQueue {
    /// Create a new link queue.
    pub fn new(
        batch_size: usize,
        is_streamed: bool,
        sn_reliable: Arc<Mutex<SeqNumGenerator>>,
        sn_best_effort: Arc<Mutex<SeqNumGenerator>>
    ) -> LinkQueue {
        // Conditional variables        
        let not_full = vec![Arc::new(Condition::new(*QUEUE_CONCURRENCY)); QUEUE_NUM];
        let not_empty = Arc::new(Condition::new(*QUEUE_CONCURRENCY));

        // Build the state EMPTY
        let mut state_empty = Vec::with_capacity(QUEUE_NUM);
        state_empty.push(Arc::new(Mutex::new(CircularBatchEmpty::new(*QUEUE_SIZE_CTRL))));
        state_empty.push(Arc::new(Mutex::new(CircularBatchEmpty::new(*QUEUE_SIZE_RETX))));
        state_empty.push(Arc::new(Mutex::new(CircularBatchEmpty::new(*QUEUE_SIZE_DATA))));

        // Build the state OUT
        let mut state_out = Vec::with_capacity(QUEUE_NUM);
        state_out.push(CircularBatchOut::new(*QUEUE_SIZE_CTRL));
        state_out.push(CircularBatchOut::new(*QUEUE_SIZE_RETX));
        state_out.push(CircularBatchOut::new(*QUEUE_SIZE_DATA));
        let state_out = Arc::new(Mutex::new(state_out));

        // Build the state IN
        let mut state_in = Vec::with_capacity(QUEUE_NUM);
        state_in.push(Arc::new(Mutex::new(CircularBatchIn::new(
            QUEUE_PRIO_CTRL, *QUEUE_SIZE_CTRL, batch_size, is_streamed, sn_reliable.clone(),
            sn_best_effort.clone(), state_out.clone(), state_empty[QUEUE_PRIO_CTRL].clone(), 
            not_full[QUEUE_PRIO_CTRL].clone(), not_empty.clone()
        )))); 
        state_in.push(Arc::new(Mutex::new(CircularBatchIn::new(
            QUEUE_PRIO_RETX, *QUEUE_SIZE_RETX, batch_size, is_streamed, sn_reliable.clone(), 
            sn_best_effort.clone(), state_out.clone(), state_empty[QUEUE_PRIO_RETX].clone(), 
            not_full[QUEUE_PRIO_RETX].clone(), not_empty.clone()
        ))));
        state_in.push(Arc::new(Mutex::new(CircularBatchIn::new(
            QUEUE_PRIO_DATA, *QUEUE_SIZE_DATA, batch_size, is_streamed, sn_reliable.clone(), 
            sn_best_effort.clone(), state_out.clone(), state_empty[QUEUE_PRIO_DATA].clone(), 
            not_full[QUEUE_PRIO_DATA].clone(), not_empty.clone()
        ))));
         
        LinkQueue { 
            state_in,
            state_out,
            state_empty,            
            not_full,
            not_empty
        }
    }

    pub(super) async fn push_session_message(&self, message: SessionMessage, priority: usize) {
        zasynclock!(self.state_in[priority]).serialize_session_message(message).await;
    }

    pub(super) async fn push_zenoh_message(&self, message: ZenohMessage, priority: usize) {
        zasynclock!(self.state_in[priority]).serialize_zenoh_message(message).await;
    }

    pub(super) async fn push_serialization_batch(&self, batch: SerializationBatch, priority: usize) {
        let mut guard = zasynclock!(self.state_empty[priority]);
        guard.push(batch);
        if self.not_full[priority].has_waiting_list() {
            self.not_full[priority].notify(guard).await;
        }
    }

    pub(super) async fn pull(&self) -> (SerializationBatch, usize) {
        loop {
            let mut guard = zasynclock!(self.state_out);
            for priority in 0usize..guard.len() {
                if let Some(batch) = guard[priority].pull() {
                    return (batch, priority)
                }
            }
            self.not_empty.wait(guard).await;
        }
    }

    pub(super) async fn drain(&self) -> Option<SerializationBatch> {
        // First try to drain the state OUT pipeline
        let mut guard = zasynclock!(self.state_out);
        for priority in 0usize..guard.len() {
            if let Some(batch) = guard[priority].pull() {
                return Some(batch)
            }
        }
        drop(guard);
        
        // Then try to drain what left in the state IN pipeline
        for priority in 0usize..self.state_in.len() {
            let mut guard = zasynclock!(self.state_in[priority]);
            if let Some(batch) = guard.pull() {
                return Some(batch)
            }
        }        

        None
    }
}

#[cfg(test)]
mod tests {
    use async_std::sync::{Arc, Mutex};
    use async_std::task;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::core::ResKey;
    use crate::io::RBuf;
    use crate::proto::{SeqNumGenerator, SessionMessage, ZenohMessage};
    use crate::session::defaults::{QUEUE_PRIO_CTRL, QUEUE_PRIO_RETX, QUEUE_PRIO_DATA, SESSION_BATCH_SIZE, SESSION_SEQ_NUM_RESOLUTION};

    use super::*;


    async fn schedule(queue: Arc<LinkQueue>, counter: Arc<AtomicUsize>) {
        // Send reliable messages
        let reliable = true;
        let key = ResKey::RName("test".to_string());
        let info = None;
        let payload = RBuf::from(vec![0u8; 8_000]);
        let reply_context = None;
        let attachment = None;

        let message = ZenohMessage::make_data(
            reliable, key, info, payload, reply_context, attachment
        );

        loop {
            queue.push_zenoh_message(message.clone(), QUEUE_PRIO_DATA).await;
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn consume(queue: Arc<LinkQueue>, counter: Arc<AtomicUsize>) {
        loop {
            let (batch, index) = queue.pull().await;
            counter.fetch_add(batch.len(), Ordering::Relaxed);   
            queue.push_serialization_batch(batch, index).await;
        }        
    }

    async fn stats(c_messages: Arc<AtomicUsize>, c_bytes: Arc<AtomicUsize>) {
        loop {
            task::sleep(Duration::from_millis(1_000)).await;
            let messages = c_messages.swap(0, Ordering::Relaxed);
            let bytes = c_bytes.swap(0, Ordering::Relaxed);
            println!("Messages: {}\tBytes: {}", messages, bytes);
        }
    }

    #[test]
    fn link_queue() {
        let batch_size = *SESSION_BATCH_SIZE;
        let is_streamed = true;
        let sn_reliable = Arc::new(Mutex::new(
            SeqNumGenerator::new(0, *SESSION_SEQ_NUM_RESOLUTION)
        ));
        let sn_best_effort = Arc::new(Mutex::new(
            SeqNumGenerator::new(0, *SESSION_SEQ_NUM_RESOLUTION)
        ));

        let queue = Arc::new(LinkQueue::new(
            batch_size, is_streamed, sn_reliable, sn_best_effort
        ));
        let counter_messages = Arc::new(AtomicUsize::new(0));
        let counter_bytes = Arc::new(AtomicUsize::new(0));

        let c_messages = counter_messages.clone();
        let c_bytes = counter_bytes.clone();
        task::spawn(async move {
            stats(c_messages, c_bytes).await;
        });

        let c_queue = queue.clone();
        task::spawn(async move {
            consume(c_queue, counter_bytes).await;
        });

        task::block_on(schedule(queue, counter_messages));
    }    
}