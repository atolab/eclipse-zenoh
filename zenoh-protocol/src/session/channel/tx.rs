// //
// // Copyright (c) 2017, 2020 ADLINK Technology Inc.
// //
// // This program and the accompanying materials are made available under the
// // terms of the Eclipse Public License 2.0 which is available at
// // http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// // which is available at https://www.apache.org/licenses/LICENSE-2.0.
// //
// // SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// //
// // Contributors:
// //   ADLINK zenoh team, <zenoh@adlink-labs.tech>
// //
// use async_std::sync::Arc;
// use async_std::task;

// use super::{Channel, MessageInner, MessageTx, SerializedBatch};

// use crate::core::ZInt;
// use crate::proto::SeqNumGenerator;
// use crate::session::defaults::QUEUE_SIZE_TOT;

// use zenoh_util::{zasynclock, zasyncread, zerror};
// use zenoh_util::collections::credit_queue::Drain as CreditQueueDrain;
// use zenoh_util::core::{ZResult, ZError, ZErrorKind};



// /*************************************/
// /*           CHANNEL TASK            */
// /*************************************/

// // Always send on the first link for the time being
// const DEFAULT_LINK_INDEX: usize = 0; 

// fn map_messages_on_links(    
//     drain: &mut CreditQueueDrain<'_, MessageTx>,
//     batches: &[SerializedBatch],
//     messages: &mut Vec<Vec<MessageInner>>
// ) {    
//     // Drain all the messages from the queue and map them on the links
//     for msg in drain {   
//         log::trace!("Scheduling: {:?}", msg.inner);   
//         // Find the right index for the message
//         let index = if let Some(link) = &msg.link {
//             // Check if the target link exists, otherwise drop it            
//             if let Some(index) = batches.iter().position(|x| &x.link == link) {
//                 index
//             } else {
//                 log::debug!("Message dropped because link {} does not exist: {:?}", link, msg.inner);
//                 // Silently drop the message            
//                 continue
//             }
//         } else {
//             DEFAULT_LINK_INDEX
//         };
//         // Add the message to the right link
//         messages[index].push(msg.inner);
//     }
// }

// async fn batch_fragment_transmit(
//     inner: &mut ChannelInnerTx,
//     messages: &mut Vec<MessageInner>,
//     batch: &mut SerializedBatch
// ) -> bool {  
//     enum CurrentFrame {
//         Reliable,
//         BestEffort,
//         None
//     }
    
//     let mut current_frame = CurrentFrame::None;
    
//     // Process all the messages
//     for msg in messages.drain(..) {
//         log::trace!("Serializing: {:?}", msg);

//         let mut has_failed = false;
//         let mut current_sn = None;
//         let mut is_first = true;

//         // The loop works as follows:
//         // - first iteration tries to serialize the message on the current batch;
//         // - second iteration tries to serialize the message on a new batch if the first iteration failed
//         // - third iteration fragments the message if the second iteration failed
//         loop {
//             // Mark the write operation       
//             batch.buffer.mark();
//             // Try to serialize the message on the current batch
//             let res = match &msg {
//                 MessageInner::Zenoh(m) => {
//                     // The message is reliable or not
//                     let reliable = m.is_reliable();
//                     // Eventually update the current frame and sn based on the current status
//                     match current_frame {
//                         CurrentFrame::Reliable => {
//                             if !reliable {
//                                 // A new best-effort frame needs to be started
//                                 current_frame = CurrentFrame::BestEffort;
//                                 current_sn = Some(inner.sn.best_effort.get());
//                                 is_first = true;
//                             }
//                         }, 
//                         CurrentFrame::BestEffort => {
//                             if reliable {    
//                                 // A new reliable frame needs to be started
//                                 current_frame = CurrentFrame::Reliable;
//                                 current_sn = Some(inner.sn.reliable.get());
//                                 is_first = true;
//                             }
//                         },
//                         CurrentFrame::None => {
//                             if !has_failed || !is_first {
//                                 if reliable {
//                                     // A new reliable frame needs to be started
//                                     current_frame = CurrentFrame::Reliable;
//                                     current_sn = Some(inner.sn.reliable.get());
//                                 } else {
//                                     // A new best-effort frame needs to be started
//                                     current_frame = CurrentFrame::BestEffort;
//                                     current_sn = Some(inner.sn.best_effort.get());
//                                 }
//                                 is_first = true;
//                             }
//                         }
//                     }

//                     // If a new sequence number has been provided, it means we are in the case we need 
//                     // to start a new frame. Write a new frame header.
//                     if let Some(sn) = current_sn {                        
//                         // Serialize the new frame and the zenoh message
//                         batch.buffer.write_frame_header(reliable, sn, None, None)
//                         && batch.buffer.write_zenoh_message(&m)
//                     } else {
//                         is_first = false;
//                         batch.buffer.write_zenoh_message(&m)
//                     }                    
//                 },
//                 MessageInner::Session(m) => {
//                     current_frame = CurrentFrame::None;
//                     batch.buffer.write_session_message(&m)
//                 },
//                 MessageInner::Stop => {
//                     let _ = batch.transmit().await;
//                     return false
//                 },
//                 MessageInner::Fragment(..) |
//                 MessageInner::Retransmission(..)  => unimplemented!("Type of message not covered")
//             };

//             // We have been succesfull in writing on the current batch: breaks the loop
//             // and tries to write more message on the same batch
//             if res {
//                 break
//             }

//             // An error occured, revert the batch buffer
//             batch.buffer.revert();
//             // Reset the current frame but not the sn which is carried over the next iteration
//             current_frame = CurrentFrame::None; 

//             // This is the second time that the serialization fails, we should fragment
//             if has_failed {
//                 // @TODO: Implement fragmentation here
//                 //        Drop the message for the time being
//                 log::warn!("Message dropped because fragmentation is not yet implemented: {:?}", msg);
//                 batch.clear();
//                 break
//             }

//             // Send the batch
//             let _ = batch.transmit().await;

//             // Mark the serialization attempt as failed
//             has_failed = true;
//         }
//     }

//     true
// }

// // Task for draining the queue
// async fn drain_queue(
//     ch: Arc<Channel>,
//     mut batches: Vec<SerializedBatch>,    
//     mut messages: Vec<Vec<MessageInner>>
// ) {
//     // @TODO: Implement reliability
//     // @TODO: Implement fragmentation

//     // Keep the lock on the inner transmission structure
//     let mut inner = zasynclock!(ch.tx);

//     // Control variable
//     let mut active = true; 

//     while active {    
//         // log::trace!("Waiting for messages in the transmission queue of channel: {}", ch.get_peer());
//         // Get a Drain iterator for the queue
//         // drain() waits for the queue to be non-empty
//         let mut drain = ch.queue.drain().await;
        
//         // Try to always fill the batch
//         while active {
//             // Map the messages on the links. This operation drains messages from the Drain iterator
//             map_messages_on_links(&mut drain, &batches, &mut messages);
            
//             // The drop() on Drain object needs to be manually called since an async
//             // destructor is not yet supported in Rust. More information available at:
//             // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47 
//             drain.drop().await;
            
//             // Concurrently send on all the selected links
//             for (i, mut batch) in batches.iter_mut().enumerate() {
//                 // active = batch_fragment_transmit(link, &mut inner, &mut context[i]);
//                 // Check if the batch is ready to send      
//                 let res = batch_fragment_transmit(&mut inner, &mut messages[i], &mut batch).await;
//                 active = active && res;
//             }

//             // Try to drain messages from the queue
//             // try_drain does not wait for the queue to be non-empty
//             drain = ch.queue.try_drain().await;
//             // Check if we can drain from the Drain iterator
//             let (min, _) = drain.size_hint();
//             if min == 0 {
//                 // The drop() on Drain object needs to be manually called since an async
//                 // destructor is not yet supported in Rust. More information available at:
//                 // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47
//                 drain.drop().await;
//                 break
//             }
//         }

//         // Send any leftover on the batches
//         for batch in batches.iter_mut() {
//             // Check if the batch is ready to send      
//             let _ = batch.transmit().await;        
//         }

//         // Deschedule the task to allow other tasks to be scheduled and eventually push on the queue
//         task::yield_now().await;
//     }
// }

// // Consume task
// async fn consume_task(ch: Arc<Channel>) -> ZResult<()> {
//     // Acquire the lock on the links
//     let guard = zasyncread!(ch.links);

//     // Check if we have links to send on
//     if guard.links.is_empty() {
//         let e = "Unable to start the consume task, no links available".to_string();
//         log::debug!("{}", e);
//         return zerror!(ZErrorKind::Other {
//             descr: e
//         })
//     }

//     // Allocate batches and messages buffers
//     let mut batches: Vec<SerializedBatch> = Vec::with_capacity(guard.links.len());
//     let mut messages: Vec<Vec<MessageInner>> = Vec::with_capacity(guard.links.len());

//     // Initialize the batches based on the current links parameters      
//     for link in guard.get().drain(..) {
//         let size = guard.batch_size.min(link.get_mtu());
//         batches.push(SerializedBatch::new(link, size));
//         messages.push(Vec::with_capacity(*QUEUE_SIZE_TOT));
//     }

//     // Drop the mutex guard
//     drop(guard);

//     // Drain the queue until a Stop signal is received
//     drain_queue(ch.clone(), batches, messages).await;

//     // Barrier to synchronize with the stop()
//     ch.barrier.wait().await;

//     log::trace!("Exiting consume task for channel: {}", ch.get_peer());

//     Ok(())
// }

// /*************************************/
// /*      CHANNEL INNER TX STRUCT      */
// /*************************************/
// // Structs to manage the sequence numbers of channels
// pub(super) struct SeqNumTx {
//     pub(super) reliable: SeqNumGenerator,
//     pub(super) best_effort: SeqNumGenerator,
// }

// impl SeqNumTx {
//     fn new(sn_resolution: ZInt, initial_sn: ZInt) -> SeqNumTx {
//         SeqNumTx {
//             reliable: SeqNumGenerator::make(initial_sn, sn_resolution).unwrap(),
//             best_effort: SeqNumGenerator::make(initial_sn, sn_resolution).unwrap(),
//         }
//     }
// }

// // Store the mutable data that need to be used for reception
// pub(super) struct ChannelInnerTx {
//     pub(super) sn: SeqNumTx
//     // Add reliability queue
//     // Add message to fragment
// }

// impl ChannelInnerTx {
//     pub(super) fn new(sn_resolution: ZInt, initial_sn: ZInt) -> ChannelInnerTx {
//         ChannelInnerTx {
//             sn: SeqNumTx::new(sn_resolution, initial_sn)
//         } 
//     }
// }

// impl Channel {
//         /*************************************/
//     /*               TASK                */
//     /*************************************/
//     pub(super) async fn start(&self) -> ZResult<()> {
//         let mut guard = zasynclock!(self.active);   
//         // If not already active, start the transmission loop
//         if !*guard {    
//             // Get the Arc to the channel
//             let ch = zasyncread!(self.w_self).as_ref().unwrap().upgrade().unwrap();

//             // Spawn the transmission loop
//             task::spawn(async move {
//                 let res = consume_task(ch.clone()).await;
//                 if res.is_err() {
//                     let mut guard = zasynclock!(ch.active);
//                     *guard = false;
//                 }
//             });

//             // Mark that now the task can be stopped
//             *guard = true;

//             return Ok(())
//         }

//         zerror!(ZErrorKind::Other {
//             descr: format!("Can not start channel with peer {} because it is already active", self.get_peer())
//         })
//     }

//     pub(super) async fn stop(&self, priority: usize) -> ZResult<()> {  
//         let mut guard = zasynclock!(self.active);         
//         if *guard {
//             let msg = MessageTx {
//                 inner: MessageInner::Stop,
//                 link: None
//             };
//             self.queue.push(msg, priority).await;
//             self.barrier.wait().await;

//             // Mark that now the task can be started
//             *guard = false;

//             return Ok(())
//         }

//         zerror!(ZErrorKind::Other {
//             descr: format!("Can not stop channel with peer {} because it is already inactive", self.get_peer())
//         })
//     }
// }
