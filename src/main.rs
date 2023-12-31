use clap::Parser;
use fastfull_search::index::{BaseHandler, SplitHandler, SplitO1, BooleanQueryHandler, PostingHandler, TantivyHandler};
use fastfull_search::{Result, FastArgs, Handler};
use fastfull_search::index::handler::HandlerT;
use jemallocator::Jemalloc;
use tracing::{info, Level};

// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    info!("main execution");
    let args = FastArgs::parse();

    let mut handler: Box<dyn HandlerT> = match args.handler {
        Handler::Base => {
            Box::new(BaseHandler::new(&args.path[0]))
        }
        Handler::SplitBase => Box::new(SplitHandler::new(&args.path[0])),
        Handler::SplitO1 => Box::new(SplitO1::new(&args.path[0])),
        Handler::LoadData =>  {
            // SplitConstruct::new(&args.path[0]).split("").await?;
            return Ok(())
        }
        Handler::BooleanQuery => {
            Box::new(BooleanQueryHandler::new(&args.path[0]))
        }
        Handler::PostingTable => {
            Box::new(PostingHandler::new(
                args.base,
                args.path,
                args.partition_num.expect("Should have partition_num arg"),
                args.batch_size.unwrap_or(512),
                args.dump_path,
            ))
        }
        Handler::Tantivy => {
            Box::new(TantivyHandler::new(args.base, args.path, args.partition_num.unwrap() as usize).unwrap())
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    
    let res = runtime.block_on(async {handler.execute().await.unwrap() });
    println!("{:}", res);
    Ok(())
}
