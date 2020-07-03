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
use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};
use async_std::future;
use async_std::task;
use clap::{App, Arg};
use zenoh_protocol::proto::whatami;
use zenoh_router::plugins::PluginsMgr;
use zenoh_router::runtime::{ AdminSpace, Runtime, Config };

const PLUGINS_PREFIX: &str = "zplugin_";

fn get_plugins_from_args() -> Vec<String> {
    let mut result: Vec<String> = vec![]; 
    let mut iter = std::env::args();
    while let Some(arg) = iter.next() {
        if arg == "-P" {
            if let Some(arg2) = iter.next() { result.push(arg2); }
        } else if arg.starts_with("--plugin=") {
            result.push(arg);
        }
    }
    result
}


fn main() {
    task::block_on(async {
        env_logger::init();

        let app = App::new("The zenoh router")
        .arg(Arg::from_usage("-l, --listener=[LOCATOR]... \
            'A locator on which this router will listen for incoming sessions. \
            Repeat this option to open several listeners.'")
            .default_value("tcp/0.0.0.0:7447"))
        .arg(Arg::from_usage("-e, --peer=[LOCATOR]... \
            'A peer locator this router will try to connect to. \
            Repeat this option to connect to several peers.'"))
        .arg(Arg::from_usage("-P, --plugin=[PATH_TO_PLUGIN]... \
             'A plugin that must be loaded. Repeat this option to load several plugins.
             Note that when set this option disable the automatic search and load of plugins.'"));

        let mut plugins_mgr = PluginsMgr::new();
        // Get specified plugins from command line 
        let plugins = get_plugins_from_args();
        if plugins.is_empty() {
            plugins_mgr.search_and_load_plugins(&format!("{}{}" ,DLL_PREFIX, PLUGINS_PREFIX), DLL_SUFFIX).await;
        } else {
            plugins_mgr.load_plugins(plugins)
        }

        // Add plugins' expected args and parse command line
        let args = app.args(&plugins_mgr.get_plugins_args()).get_matches();

        let listeners = args.values_of("listener").map(|v| v.map(|l| l.parse().unwrap()).collect())
            .or_else(|| Some(vec![])).unwrap();
        let peers = args.values_of("peer").map(|v| v.map(|l| l.parse().unwrap()).collect())
            .or_else(|| Some(vec![])).unwrap();

        let config = Config {
            whatami: whatami::BROKER,
            peers,
            listeners,
            multicast_interface: "auto".to_string(),
            scouting_delay: std::time::Duration::new(0, 200_000_000),
        };

        let runtime = match Runtime::new(0, config).await {
            Ok(runtime) => runtime,
            _ => std::process::exit(-1),
        };
        
        plugins_mgr.start_plugins(&runtime, &args).await;

        AdminSpace::start(&runtime, plugins_mgr).await;

        future::pending::<()>().await;
    });
}