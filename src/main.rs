use std::time::Instant;

use clap::Parser;
use datafusion::prelude::*;
use fastfull_search::index::BaseHandler;
use fastfull_search::{Result, FastArgs, Handler};
use fastfull_search::index::handler::HandlerT;
use rand::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let args = FastArgs::parse();

    let mut handler: Box<dyn HandlerT> = match args.handler {
        Handler::Base => {
            Box::new(BaseHandler::new(&args.path.unwrap()))
        },
        Handler::DataFusion => unimplemented!()
    };
    let batch = handler.to_recordbatch()?;
    // declare a new context.
    let ctx = SessionContext::new();

    ctx.register_batch("table", batch)?;
    let time = Instant::now();
    let df = ctx.table("table").await?.cache().await?;
    println!("cache table: {}", time.elapsed().as_millis());
    let mut test_keys: Vec<String> = handler.get_words(100);
    test_keys.shuffle(&mut rand::thread_rng());
    let test_keys = test_keys[..2].to_vec();
    let i = &test_keys[0];
    let j = &test_keys[1];
    let time = Instant::now();
    df.select_columns(&["__id__", i, j])?
        .filter(col(i).eq(lit(1))
        .and(col(j).eq(lit(1))))?
        .show().await?;
    println!("query time: {}", time.elapsed().as_millis());
    Ok(())
}

// fn raw_process(path: &str) ->