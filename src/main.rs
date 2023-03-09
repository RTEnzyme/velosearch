
use clap::Parser;

use fastfull_search::index::{BaseHandler, SplitHandler};
use fastfull_search::{Result, FastArgs, Handler};
use fastfull_search::index::handler::HandlerT;
use tracing::{info, Level};



#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    info!("main execution");
    let args = FastArgs::parse();

    let mut handler: Box<dyn HandlerT> = match args.handler {
        Handler::Base => {
            Box::new(BaseHandler::new(&args.path.unwrap()))
        },
        Handler::SplitBase => Box::new(SplitHandler::new(&args.path.unwrap()))
    };
    handler.execute().await?;
    Ok(())
}
