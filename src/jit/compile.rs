//! Compile Expr to JIT'd function

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::expressions::Dnf as DDnf;
use datafusion::physical_expr::BooleanQueryEvalFunc;

use crate::jit::ast::{Literal, TypedLit};
use crate::utils::Result;
use super::BOOLEAN_EVAL_FUNC;
use super::api::Assembler;
use super::ast::{JITType, Dnf, U8, BooleanExpr};
use super::{
    api::GeneratedFunction,
    ast::{Expr as JITExpr, I64, PTR_SIZE},
};

pub fn create_boolean_query_fn(
    cnf: &Vec<DDnf>,
    inputs: &Vec<&str>,
) -> Arc<BooleanQueryEvalFunc> {
    let term2idx: HashMap<&str, i64> = inputs
        .into_iter()
        .enumerate()
        .map(|(i, &s)| (s, i as i64))
        .collect();
    let mut cnf_list = Vec::new();
    let cnf_num = cnf.len();
    let mut code: usize = 0;
    cnf
    .into_iter()
    .enumerate()
    .for_each(|(i, v)| {
        if v.predicates.len() == 2 {
            code |= 1 << i;
        }
        for item in v.iter() {
            cnf_list.push(term2idx[item.as_str()]);
        }
    });
    let gen_fn = BOOLEAN_EVAL_FUNC[2_usize.pow(cnf_num as u32) - 2 + code].clone();
    Arc::new(
        BooleanQueryEvalFunc::new(
            gen_fn.func,
            cnf_list,
        )
    )
}

/// Wrap JIT Expr to array compute function.
pub fn build_calc_fn(
    assembler: &Assembler,
    jit_expr: JITExpr,
    inputs: Vec<(String, JITType)>,
    _ret_type: JITType,
) -> Result<GeneratedFunction> {
    // Alias pointer type.
    // The raw pointer `R64` or `R32` is not compatible with integers.
    const PTR_TYPE: JITType = I64;

    let builder = assembler.new_func_builder("calc_fn");
    // Declare in-param.
    // Each input takes one position, following by a pointer to place result,
    // and the last is the length of inputs/output arrays.
    // for (name, _) in &inputs {
    //     builder = builder.param(format!("{name}_array"), PTR_TYPE);
    // }
    let mut builder = builder
        .param("batch", PTR_TYPE)
        .param("result", PTR_TYPE)
        .param("len", I64);

    // Start build function body.
    // It's loop that calculates the result one by one.
    let mut fn_body = builder.enter_block();
    fn_body.declare_as("index", fn_body.lit_i64(0))?;
    for (id, (name, _)) in inputs.iter().enumerate() {
        let ptr = fn_body.add(
            fn_body.id("batch")?,
            fn_body.mul(
                fn_body.lit_i64(id as i64),
                fn_body.lit_i64(PTR_SIZE as i64),
            )?,
        )?;
        fn_body.declare_as(
            format!("{name}_array"),
            fn_body.load(ptr, PTR_TYPE)?,
        )?;
    }
    fn_body.while_block(
        |cond| cond.lt(cond.id("index")?, cond.id("len")?),
        |w| {
            w.declare_as("offset", w.mul(w.id("index")?, w.lit_i64(1))?)?;
            for (name, ty) in &inputs {
                w.declare_as(
                    format!("{name}_ptr"),
                    w.add(w.id(format!("{name}_array"))?, w.id("offset")?)?,
                )?;
                w.declare_as(name, w.load(w.id(format!("{name}_ptr"))?, *ty)?)?;
            }
            w.declare_as("res_ptr", w.add(w.id("result")?, w.id("offset")?)?)?;
            w.declare_as("res", jit_expr.clone())?;
            w.store(w.id("res")?, w.id("res_ptr")?)?;
            w.assign("index", w.add(w.id("index")?, w.lit_i64(1))?)?;
            Ok(())
        },
    )?;

    let gen_func = fn_body.build();
    Ok(gen_func)
}

pub fn build_boolean_query(
    assembler: &Assembler,
    jit_expr: JITExpr,
) -> Result<GeneratedFunction> {
    // Alias pointer type.
    // The raw pointer `R64` or `R32` is not compatible with integers
    const PTR_TYPE: JITType = I64;

    let builder = assembler.new_func_builder("eval_fn");
    // Declare in-param.
    // Each input takes one position, following by a pointer to place result,
    // and the last is the lenght of inputs/output arrays.
    let mut builder = builder
        .param("batch", PTR_TYPE)
        .param("cnf", PTR_TYPE)  // Run-Time cnf predicate
        .param("result", PTR_TYPE)
        .param("len", I64);

    // Start build function body.
    // It's loop that calculates the result one by one
    let mut fn_body = builder.enter_block();
    fn_body.declare_as("index", fn_body.lit_i64(0))?;
    fn_body.while_block(
        |cond| cond.lt(cond.id("index")?, cond.id("len")?),
        |b| {
            b.declare_as("offset", b.id("index")?)?;
            b.declare_as("res_ptr", b.add(b.id("result")?, b.id("offset")?)?)?;

        
            b.declare_as("res", jit_expr.clone())?;
            b.store(b.id("res")?, b.id("res_ptr")?)?;
            b.assign("index", b.add(b.id("index")?, b.lit_i64(1))?)?;
            Ok(())
        },
    )?;

    let gen_func = fn_body.build();
    Ok(gen_func)
}

#[cfg(test)]
mod test {
    use tracing::Level;

    use crate::jit::{ast::{Expr, BooleanExpr, Dnf, U8}, api::Assembler};

    use super::{build_calc_fn, build_boolean_query};

    #[test]
    fn boolean_query_simple() {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
        let jit_expr = Expr::BooleanExpr(BooleanExpr {
            cnf: vec![1, 2, 1],
        });
        // allocate memory for result
        let result: Vec<u8> = vec![0x0; 2];
        let test1 = vec![0x01, 0x0];
        let test2 = vec![0x11, 0x23];
        let test3 = vec![0x21, 0xFF];
        let test4 = vec![0x21, 0x12];
        let input = vec![
            test1.as_ptr(),
            test2.as_ptr(),
            test3.as_ptr(),
            test4.as_ptr(),
        ];
        let cnf = vec![0, 1, 2, 3 ];


        // compile and run JIT code
        let assembler = Assembler::default();
        let gen_func = build_boolean_query(&assembler, jit_expr).unwrap();
        println!("{}", gen_func);

        let mut jit = assembler.create_jit();
        let gen_func = jit.compile(gen_func).unwrap();
        let code_fn = unsafe {
            core::mem::transmute::<_, fn(*const *const u8, *const i64, *const u8, i64) -> ()>(gen_func)
        };
        code_fn(input.as_ptr(), cnf.as_ptr(), result.as_ptr(), 2);
        println!("res: {:?}", result);
    }

    // #[test]
    // fn array_add() {
    //     let jit_expr = Expr::BooleanExpr(BooleanExpr {
    //         cnf: vec![
    //             vec![Dnf::Normal(Expr::Identifier("test".to_string(), U8))],
    //             vec![
    //                 Dnf::Normal(Expr::Identifier("test2".to_string(), U8)),
    //                 Dnf::Normal(Expr::Identifier("test3".to_string(), U8))
    //             ],
    //         ],
    //     });
    //     // allocate memory for calc result
    //     let result: Vec<u8> = vec![0x0; 2];

    //     // compile and run JIT code
    //     let assembler = Assembler::default();
    //     let input_fields = vec![
    //         ("test".to_string(), U8),
    //         ("test2".to_string(), U8),
    //         ("test3".to_string(), U8),
    //     ];
    //     let gen_func = build_calc_fn(&assembler, jit_expr, input_fields, U8).unwrap();
    //     println!("{}", &gen_func);
    //     let mut jit = assembler.create_jit();
    //     let code_ptr = jit.compile(gen_func).unwrap();
    //     let code_fn = unsafe {
    //         core::mem::transmute::<_, fn(*const *const u8, *const u8, i64) -> ()>(
    //             code_ptr,
    //         )
    //     };
    //     let test = vec![0x11, 0x01];
    //     let test2 = vec![0x01, 0x01];
    //     let test3 = vec![0x10, 0xFF];
    //     let values = vec![
    //         test.as_ptr(),
    //         test2.as_ptr(),
    //         test3.as_ptr(),
    //     ];
    //     println!("{:?}", values);
    //     code_fn(
    //         values.as_ptr(),
    //         result.as_ptr(),
    //         2,
    //     );
    //     assert_eq!(result, vec![17, 1]);
    // }
}