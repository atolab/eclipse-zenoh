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
#![feature(async_closure)]

use clap::{App, Arg};
use futures::prelude::*;
use zenoh::*;
use zenoh::net::Config;
use std::pin::Pin;


#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();

    let args = App::new("zenoh put example")
        .arg(Arg::from_usage("-m, --mode=[MODE] 'The zenoh session mode.")
            .possible_values(&["peer", "client"]).default_value("peer"))
        .arg(Arg::from_usage("-e, --peer=[LOCATOR]...  'Peer locators used to initiate the zenoh session.'"))
        .arg(Arg::from_usage("-s, --selector=[SELECTOR] 'The selection of resources to get'")
            .default_value("/demo/example/**"))
        .get_matches();

    let config = Config::new(args.value_of("mode").unwrap()).unwrap()
        .add_peers(args.values_of("peer").map(|p| p.collect()).or_else(|| Some(vec![])).unwrap());
    let selector = args.value_of("selector").unwrap();

    println!("New zenoh...");
    let zenoh = Zenoh::new(config, None).await.unwrap();
    
    println!("New workspace...");
    let workspace = zenoh.workspace(None).await.unwrap();

    println!("Get Data from {}'...\n", selector);
    let s: Pin<Box<dyn Stream<Item=Data>>> = 
    workspace.get(&selector.into())
        .await.unwrap();
        s.for_each( async move |data| 
            println!(">> [Reply handler] received reply data {} : {:?}",
                data.path, data.value.as_rbuf())
        ).await;

    zenoh.close().await.unwrap();
}
