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
use std::fmt;
use std::collections::HashMap;
use pin_project_lite::pin_project;
use async_std::sync::{Arc, RwLock, Sender, Receiver, TrySendError};
use async_std::stream::Stream;
use log::error;

pub use zenoh_protocol::io::RBuf;
pub use zenoh_protocol::core::{
    ZInt,
    ResourceId,
    ResKey,
    PeerId,
};
pub use zenoh_protocol::proto::{
    Reliability,
    SubMode,
    Period,
    SubInfo,
    Target,
    QueryTarget,
    QueryConsolidation,
    Reply,
};
pub use zenoh_protocol::proto::Primitives;
pub use zenoh_util::core::{ZError, ZErrorKind, ZResult};
use crate::net::Session;


pub type Properties = HashMap<ZInt, Vec<u8>>;

pub type DataHandler = dyn FnMut(/*res_name:*/ &str, /*payload:*/ RBuf, /*data_info:*/ Option<RBuf>) + Send + Sync + 'static;

pub(crate) type Id = usize;

#[derive(Clone)]
pub struct Publisher {
    pub(crate) id: Id,
    pub(crate) reskey: ResKey,
}

impl PartialEq for Publisher {
    fn eq(&self, other: &Publisher) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Publisher{{ id:{} }}", self.id)
    }
}

pub type Sample = (/*res_name:*/ String, /*payload:*/ RBuf, /*data_info:*/ Option<RBuf>);

pin_project! {
    #[derive(Clone)]
    pub struct Subscriber {
        pub(crate) id: Id,
        pub(crate) reskey: ResKey,
        pub(crate) resname: String,
        pub(crate) session: Session,
        #[pin]
        pub(crate) sender: Sender<Sample>,
        #[pin]
        pub(crate) receiver: Receiver<Sample>,
    }
}

impl Subscriber {
    pub async fn pull(&self) -> ZResult<()> {
        self.session.pull(&self.reskey).await
    }
}

impl Stream for Subscriber {
    type Item = Sample;

    #[inline(always)]
    fn poll_next(self: async_std::pin::Pin<&mut Self>, cx: &mut async_std::task::Context) -> async_std::task::Poll<Option<Self::Item>> {
        self.project().receiver.poll_next(cx)
    }
}

impl PartialEq for Subscriber {
    fn eq(&self, other: &Subscriber) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Subscriber{{ id:{}, resname:{} }}", self.id, self.resname)
    }
}


#[derive(Clone)]
pub struct DirectSubscriber {
    pub(crate) id: Id,
    pub(crate) reskey: ResKey,
    pub(crate) resname: String,
    pub(crate) session: Session,
    pub(crate) dhandler: Arc<RwLock<DataHandler>>,
}

impl DirectSubscriber {
    pub async fn pull(&self) -> ZResult<()> {
        self.session.pull(&self.reskey).await
    }
}

impl PartialEq for DirectSubscriber {
    fn eq(&self, other: &DirectSubscriber) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for DirectSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DirectSubscriber{{ id:{}, resname:{} }}", self.id, self.resname)
    }
}

pub type Query = (/*res_name:*/ String, /*predicate:*/ String, /*replies_sender*/ RepliesSender);

pin_project! {
    #[derive(Clone)]
    pub struct Queryable {
        pub(crate) id: Id,
        pub(crate) reskey: ResKey,
        pub(crate) kind: ZInt,
        #[pin]
        pub(crate) req_sender: Sender<Query>,
        #[pin]
        pub(crate) req_receiver: Receiver<Query>,
    }
}

impl Stream for Queryable {
    type Item = Query;

    #[inline(always)]
    fn poll_next(self: async_std::pin::Pin<&mut Self>, cx: &mut async_std::task::Context) -> async_std::task::Poll<Option<Self::Item>> {
        self.project().req_receiver.poll_next(cx)
    }
}

impl PartialEq for Queryable {
    fn eq(&self, other: &Queryable) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Queryable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Queryable{{ id:{} }}", self.id)
    }
}

pub struct RepliesSender{
    pub(crate) kind: ZInt,
    pub(crate) sender: Sender<(ZInt, Option<Sample>)>,
}

impl RepliesSender{
    pub async fn send(&'_ self, msg: Sample) {
        self.sender.send((self.kind, Some(msg))).await
    }

    pub fn try_send(&self, msg: Sample) -> Result<(), TrySendError<Sample>> {
        match self.sender.try_send((self.kind, Some(msg))) {
            Ok(()) => {Ok(())}
            Err(TrySendError::Full(sample)) => {Err(TrySendError::Full(sample.1.unwrap()))}
            Err(TrySendError::Disconnected(sample)) => {Err(TrySendError::Disconnected(sample.1.unwrap()))}
        }
    }

    pub fn capacity(&self) -> usize {
        self.sender.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.sender.is_full()
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }
}

impl Drop for RepliesSender {
    fn drop(&mut self) {
        match self.sender.try_send((self.kind, None)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                error!("Cannot send SourceFinal message : channel is full ! ") // @TODO
            }
            Err(TrySendError::Disconnected(_)) => {
                error!("Cannot send SourceFinal message : channel is disconnected ! ") // Should never happen
            }
        }
    }
}
