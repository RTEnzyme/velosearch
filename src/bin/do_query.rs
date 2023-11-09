use std::{env, sync::Arc};
use peg;
use datafusion::{prelude::*, sql::TableReference, datasource::provider_as_source,arrow::array::UInt64Array, prelude::Expr};
use fastfull_search::{utils::{Result, builder::deserialize_posting_table}, BooleanContext, jit::AOT_PRIMITIVES};
use jemallocator::Jemalloc;
use tantivy::tokenizer::{TextAnalyzer, SimpleTokenizer, RemoveLongFilter, LowerCaser};
use lazy_static::lazy_static;

lazy_static!{
    pub static ref TOKENIZER: TextAnalyzer = TextAnalyzer::from(SimpleTokenizer)
    .filter(RemoveLongFilter::limit(40))
    .filter(LowerCaser);
}

fn col_tokenized(token: &str) -> Expr {
    let mut process = TOKENIZER.token_stream(&token);
    let token = &process.next().unwrap().text;
    Expr::Column(token.into())
}

peg::parser!{
    grammar boolean_parser() for str {
        pub rule boolean() -> Expr = precedence!{
            x:and() _ y:(@) { boolean_and(x, y) }
            x:or() _ y:(@) { boolean_or(x, y)}
            x:and() { x }
            x:or() { x }
            --
            "(" e:boolean() ")" { e }
            "+(" e:boolean() ")" { e }
        }
        
        rule or() -> Expr
            = l:term() { l }

        rule and() -> Expr
            = "+" e:term() {e}

        rule term() -> Expr 
            = e:$(['a'..='z' | 'A'..='Z' | '_']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) { col_tokenized(e) }

        rule _() =  quiet!{[' ' | '\t']*}
    }
}

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
        let predicate = boolean_parser::boolean(&line).unwrap();
        let index = ctx.boolean_with_provider(table_source.clone(), &schema, predicate, false).await.unwrap();
        let res = index.collect().await.unwrap();
        let res: u64 = res[0].column(0).as_any().downcast_ref::<UInt64Array>().unwrap().value(0);
        println!("{:}", res);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::boolean_parser;

    #[test]
    fn simple_and_test() {
        let expr = boolean_parser::boolean("+ast +ast").unwrap();
        assert_eq!(format!("{:?}", expr), "ast & ast");
    }

    #[test]
    fn somple_or_test() {
        let expr = boolean_parser::boolean("ast ast").unwrap();
        assert_eq!(format!("{:?}", expr), "ast | ast");
    }

    #[test]
    fn combination_test() {
        let expr = boolean_parser::boolean("+ast +ast +(ast ast (+ast +ast))").unwrap();
        assert_eq!(format!("{:?}", expr), "ast & ast & (ast | ast | ast & ast)");
    }
}