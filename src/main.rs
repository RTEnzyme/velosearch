
use clap::Parser;
use datafusion::prelude::*;
use fastfull_search::index::BaseHandler;
use fastfull_search::{Result, FastArgs, Handler};
use fastfull_search::index::handler::HandlerT;
use tokio::time::Instant;
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

    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?.cache().await?;
    

    let mut test_keys: Vec<String> = handler.get_words(100);
    test_keys.shuffle(&mut rand::thread_rng());
    let test_keys = test_keys[..2].to_vec();
    let i = &test_keys[0];
    let j = &test_keys[1];
    let time = Instant::now();
    df.clone().explain(false, true)?.show().await?;
    println!("bare select time: {}", time.elapsed().as_millis());
    let time = Instant::now();
    df
        .filter(col(i).is_not_null()
        .and(col(j).is_not_null()))?
        .select_columns(&["__id__", i, j])? 
        .explain(false, true)?
        // .collect().await?
        .show().await?;
    // println!("{}", format!("select {i}, {j} from t where {i} is not null and {j} is not null"));
    // ctx.sql(&format!("select `{i}`, `{j}` from t where `{i}` is not null and `{j}` is not null")).await?;
    let query_time = time.elapsed().as_millis();
    println!("query time: {}", query_time);
    Ok(())
}
