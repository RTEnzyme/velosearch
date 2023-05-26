
use clap::Parser;
use fastfull_search::index::{BaseHandler, SplitHandler, SplitO1, SplitConstruct, BooleanQueryHandler, PostingHandler};
use fastfull_search::{Result, FastArgs, Handler};
use fastfull_search::index::handler::HandlerT;
use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
    info!("main execution");
    let args = FastArgs::parse();

    let mut handler: Box<dyn HandlerT> = match args.handler {
        Handler::Base => {
            Box::new(BaseHandler::new(&args.path.unwrap()))
        }
        Handler::SplitBase => Box::new(SplitHandler::new(&args.path.unwrap())),
        Handler::SplitO1 => Box::new(SplitO1::new(&args.path.unwrap())),
        Handler::LoadData =>  {
            SplitConstruct::new(&args.path.unwrap()).split("").await?;
            return Ok(())
        }
        Handler::BooleanQuery => {
            Box::new(BooleanQueryHandler::new(&args.path.unwrap()))
        }
        Handler::PostingTable => {
            Box::new(PostingHandler::new(&args.path.unwrap(), args.partition_num.expect("Should have partition_num arg")))
        }
    };
    handler.execute().await?;
    Ok(())
}
