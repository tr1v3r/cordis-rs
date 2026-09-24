//! The typed event bus: emit listeners, a bail query, and a waterfall
//! middleware chain. Run with `cargo run --example events -p cordis-core`.

use cordis_core::{App, EventKey, ListenerConfig, QueryKey, ShutdownOptions, WaterfallKey, define};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

struct Msg {
    text: String,
}

#[tokio::main]
async fn main() {
    let app = App::builder().name("events-demo").build().expect("app");
    let root = app.context();
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let plugin = {
        let log = Arc::clone(&log);
        define("bus", move |ctx, _: Arc<()>| {
            let log = Arc::clone(&log);
            let note = EventKey::<Msg>::new("note");
            let lookup = QueryKey::<Msg, String>::new("lookup");
            let greet = WaterfallKey::<Msg, String>::new("greet");
            async move {
                // Emit: sync listeners run in registration order.
                ctx.on_emit(
                    note,
                    move |m: &Msg| {
                        log.lock().unwrap().push(m.text.clone());
                        Ok(())
                    },
                    ListenerConfig::default(),
                )
                .await?;
                // Bail: `Break` short-circuits with the typed response.
                ctx.on_bail(
                    lookup,
                    move |m: &Msg| {
                        if m.text == "42" {
                            Ok(ControlFlow::Break("answer".to_owned()))
                        } else {
                            Ok(ControlFlow::Continue(()))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                // Waterfall: modify the payload on the way down, wrap
                // the result on the way up.
                ctx.on_waterfall(
                    greet,
                    |mut m: Msg, next| async move {
                        m.text.push('!');
                        let inner = next.run(m).await?;
                        Ok(format!("hello {inner}"))
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };

    let receipt = root.load(&plugin, ()).await.expect("load admitted");
    receipt.operation.wait().await.expect("active");
    let note = EventKey::<Msg>::new("note");
    let report = root
        .emit(note, Msg { text: "hi".into() })
        .await
        .expect("emit");
    assert!(report.is_clean() && report.delivered == 1);
    println!("emit delivered: {}", report.delivered);
    let lookup = QueryKey::<Msg, String>::new("lookup");
    let answer = root
        .bail(lookup, Msg { text: "42".into() })
        .await
        .expect("bail dispatch");
    println!("bail answered: {answer:?}");
    let greet = WaterfallKey::<Msg, String>::new("greet");
    let greeting = root
        .waterfall(
            greet,
            Msg {
                text: "cordis".into(),
            },
            |m| async move { Ok(m.text) },
        )
        .await
        .expect("waterfall dispatch");
    println!("waterfall result: {greeting}");

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    println!("shutdown: {} fibers disposed", report.fibers_disposed);
}
