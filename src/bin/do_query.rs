use std::{env, sync::Arc, time::Instant};

use datafusion::{sql::TableReference, datasource::provider_as_source};
use fastfull_search::{utils::{Result, builder::deserialize_posting_table}, BooleanContext, jit::AOT_PRIMITIVES, query::boolean_query::BooleanPredicateBuilder};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    main_inner(args[1].to_owned()).await.unwrap();
}

async fn main_inner(index_dir: String) -> Result<()> {
    let posting_table = deserialize_posting_table(index_dir).unwrap();
    let ctx = BooleanContext::new();
    ctx.register_index(TableReference::Bare { table: "__table__".into() }, Arc::new(posting_table))?;
    let _ = AOT_PRIMITIVES.len();
    let provider = ctx.index_provider("__table__").await?;
    let schema = &provider.schema();
    let table_source = provider_as_source(Arc::clone(&provider));

    let stdin = std::io::stdin();
    for line_res in stdin.lines() {
        let line = line_res?;
        let query = line.split("\t").skip(1).collect::<String>();
        let fields = query.split(" ");
        let trim_fields: Vec<String> = fields
            .map(|s| s.chars().skip(1).collect())
            .collect();
        let predicate = BooleanPredicateBuilder::must(&trim_fields.iter().map(|v| v.as_str()).collect::<Vec<&str>>())?;
        let predicate = predicate.build();
        let index = ctx.boolean_with_provider(table_source.clone(), &schema, predicate, false).await.unwrap();
        index.collect().await.unwrap();
        println!("1");
    }

    Ok(())
}
