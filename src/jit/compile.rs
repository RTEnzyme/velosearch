//! Compile Expr to JIT'd function

use crate::utils::Result;
use super::Boolean;
use super::api::Assembler;
use super::ast::JITType;
use super::{
    api::GeneratedFunction,
    ast::{Expr as JITExpr, I64},
};

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
    let cnf_nums = if let JITExpr::BooleanExpr(ref bool) = jit_expr {
        &bool.cnf
    } else {
        unreachable!()
    };
    let mut cur = 0;
    let mut cnf = Vec::new();
    for i in cnf_nums {
        cnf.push((i, cur));
        cur += *i;
    }
    for (i, n) in cnf.into_iter().enumerate() {
        let offset = fn_body.add(fn_body.id("cnf")?, fn_body.lit_i64(8 * n.1))?;
        fn_body.declare_as(format!("p{i}_1").as_str(), fn_body.load(offset, I64)?)?;
        if *n.0 == 2 {
            let offset = fn_body.add(fn_body.id("cnf")?, fn_body.lit_i64(8 * n.1 + 8))?;
            fn_body.declare_as(format!("p{i}_2").as_str(), fn_body.load(offset, I64)?)?;
        }
    }
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

pub fn jit_short_circuit_primitive(
    assembler: &Assembler,
    jit_expr: Boolean,
    leaf_num: usize,
) -> Result<GeneratedFunction> {
    let jit_expr = JITExpr::Boolean(jit_expr);
    // Alias pointer type.
    // The raw pointer `R64` or `R32` is not compatible with integers
    const PTR_TYPE: JITType = I64;

    let builder = assembler.new_func_builder("short_circuit_primitive");
    // Declare in-param
    // Each input takes one position, following by a pointer to place result,
    // and the last is the length of inputs/output arrays.
    let mut builder = builder
        .param("batch", PTR_TYPE)
        .param("init_v", PTR_TYPE)
        .param("result", PTR_TYPE)
        .param("len", I64);

    // Start build function body.
    // It's loop that calculate the result one by one to enable
    // loop unrolling
    let mut fn_body = builder.enter_block();
    for i in 0..leaf_num {
        let offset = fn_body.add(fn_body.id("batch")?, fn_body.lit_i64(8 * i as i64))?;
        fn_body.declare_as(format!("p{i}").as_str(), fn_body.load(offset, I64)?)?;
    }
    fn_body.declare_as("index", fn_body.lit_i64(0))?;
    fn_body.while_block(
        |cond| cond.lt(cond.id("index")?, cond.id("len")?),
        |b| {
            b.declare_as("offset", b.id("index")?)?;
            b.declare_as("res_ptr", b.add(b.id("result")?, b.id("offset")?)?)?;
            b.declare_as("res", jit_expr.clone())?;
            // b.declare_as("res", b.lit_i64(66))?;
            b.store(b.id("res")?, b.id("res_ptr")?)?;
            b.assign("index", b.add(b.id("index")?, b.lit_i64(1))?)?;
            Ok(())
        }
    )?;
    let gen_func = fn_body.build();
    Ok(gen_func)

}

/// Convert the Louds encoding to JIT expr
pub fn louds_2_boolean(node_num: usize, has_child: Vec<u8>, louds: Vec<u8>) -> Boolean {
    // Unexpected condition:
    // 1. the top level has only one node.
    
    unimplemented!()
}

#[cfg(test)]
mod test {
    use tracing::{Level, debug};

    use crate::jit::{ast::{Expr, BooleanExpr, Boolean, Predicate}, api::Assembler};

    use super::{build_boolean_query, jit_short_circuit_primitive};

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

        let mut jit = assembler.create_jit();
        let gen_func = jit.compile(gen_func).unwrap();
        let code_fn = unsafe {
            core::mem::transmute::<_, fn(*const *const u8, *const i64, *const u8, i64) -> ()>(gen_func)
        };
        code_fn(input.as_ptr(), cnf.as_ptr(), result.as_ptr(), 2);
        assert_eq!(result, vec![0x01, 0]);
    }

    #[test]
    fn boolean_query_v2_simple() {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
        let jit_expr = Boolean {
            predicate: Predicate::And { 
                args: vec![
                    Predicate::Leaf { idx: 0 },
                    Predicate::Or { args: vec![
                        Predicate::Leaf { idx: 1 },
                        Predicate::Leaf { idx: 2 },
                    ] },
                    Predicate::Leaf { idx: 3 },
                ] 
            },
            start_idx: 0,
        };
        // allocate memory for result
        let result: Vec<u8> = vec![0x0; 2];
        let test1 = vec![0x01, 0x0];
        let test2 = vec![0x11, 0x23];
        let test3 = vec![0x21, 0xFF];
        let test4 = vec![0x21, 0x12];
        let init_v: Vec<u8> = vec![1, 1];
        let batch = vec![
            test1.as_ptr(),
            test2.as_ptr(),
            test3.as_ptr(),
            test4.as_ptr(),
        ];

        // Compile and run JIT code
        let assembler = Assembler::default();
        let gen_func = jit_short_circuit_primitive(&assembler, jit_expr, 4).unwrap();

        let mut jit = assembler.create_jit();
        let gen_func = jit.compile(gen_func).unwrap();
        debug!("start transmute");
        let code_fn = unsafe {
            core::mem::transmute::<_, fn(*const *const u8, *const u8, *const u8, i64) -> ()>(gen_func)
        };
        debug!("end transmute");
        code_fn(batch.as_ptr(), init_v.as_ptr(), result.as_ptr(), 2);
        assert_eq!(result, vec![1, 0]);
    }

    #[test]
    fn boolean_query_nested() {
        tracing_subscriber::fmt().with_max_level(Level::DEBUG).init();
        let jit_expr = Boolean {
            predicate: Predicate::And {
                args: vec![
                    Predicate::Leaf { idx: 0 },
                    Predicate::Or { args: vec![
                        Predicate::Leaf { idx: 1 },
                        Predicate::And { args: vec![
                            Predicate::Leaf { idx: 2 },
                            Predicate::Leaf { idx: 3 },
                        ] }
                    ] },
                ] 
            },
            start_idx: 0,
        };
        // allocate memory for result
        let result: Vec<u8> = vec![0x0; 2];
        let test1 = vec![0x31, 0x0];
        let test2 = vec![0x11, 0x23];
        let test3 = vec![0x21, 0xFF];
        let test4 = vec![0x21, 0x12];
        let init_v: Vec<u8> = vec![u8::MAX, 1];
        let batch = vec![
            test1.as_ptr(),
            test2.as_ptr(),
            test3.as_ptr(),
            test4.as_ptr(),
        ];

        // Compile and run JIT code
        let assembler = Assembler::default();
        let gen_func = jit_short_circuit_primitive(&assembler, jit_expr, 4).unwrap();

        let mut jit = assembler.create_jit();
        let gen_func = jit.compile(gen_func).unwrap();
        debug!("start transmute");
        let code_fn = unsafe {
            core::mem::transmute::<_, fn(*const *const u8, *const u8, *const u8, i64) -> ()>(gen_func)
        };
        debug!("end transmute");
        code_fn(batch.as_ptr(), init_v.as_ptr(), result.as_ptr(), 2);
        assert_eq!(result, vec![0x31, 0]);
    }
}