pub mod ast;
pub mod api;
pub mod compile;
pub mod jit;

use std::sync::Arc;

pub use compile::create_boolean_query_fn;
use lazy_static::lazy_static;

pub use crate::jit::{api::Assembler, ast::{Expr, Boolean, BooleanExpr}, compile::build_boolean_query};
use crate::utils::Result;

use self::compile::jit_short_circuit_primitive;

pub struct PrecompiledBooleanEval {
    func: fn(*const *const u8, *const i64, *const u8, i64) -> (),
}

unsafe impl Send for PrecompiledBooleanEval {}

unsafe impl Sync for PrecompiledBooleanEval {}

const LIMIT_CNT: usize = 6;
lazy_static!(
    pub static ref BOOLEAN_EVAL_FUNC: Vec<Arc<PrecompiledBooleanEval>> = {
        let mut func_vec = Vec::new();
        let assembler = Assembler::default();
        let mut cnt: usize = 0;
        while cnt < LIMIT_CNT {
            let mut code: usize = 0;
            while code <= (1 << (cnt + 1)) - 1 {
                let cnf_vec: Vec<i64> = (0..(cnt + 1)).map(|v| {
                    if (1 << v) & code == 0 {
                        1
                    } else {
                        2
                    }
                })
                .collect();
                let jit_expr = Expr::BooleanExpr(BooleanExpr {
                    cnf: cnf_vec,
                });
                let gen_func = build_boolean_query(&assembler, jit_expr).unwrap();
                let mut jit = assembler.create_jit();
                let gen_func = jit.compile(gen_func).unwrap();
                let code_fn = unsafe {
                    core::mem::transmute::<_, fn(*const *const u8, *const i64, *const u8, i64) -> ()>(gen_func)
                };
                func_vec.push(Arc::new(PrecompiledBooleanEval { func: code_fn }));
                code += 1;
            }
            cnt += 1;
        }
        func_vec
    };
);

pub fn jit_short_circuit(expr: Boolean, leaf_num: usize) -> Result<fn(*const *const u8, *const u8, *const u8, i64)> {
    let assembler = Assembler::default();
    let gen_func = jit_short_circuit_primitive(&assembler, expr, leaf_num)?;

    let mut jit = assembler.create_jit();
    let gen_func = jit.compile(gen_func)?;
    let code_fn = unsafe {
        core::mem::transmute::<_, fn(*const *const u8, *const u8, *const u8, i64)->()>(gen_func)
    };
    Ok(code_fn)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{arrow::{array::BooleanArray, datatypes::{Schema, DataType, Field}, record_batch::RecordBatch}, physical_plan::expressions::Dnf};

    use crate::{utils::Result, jit::{api::Assembler, ast::U16, create_boolean_query_fn}};

    use super::{jit::JIT, api::GeneratedFunction};

    #[test]
    fn global_static_boolean_eval() {
        let schema = Schema::new(
            vec![
                Field::new(format!("test1"), DataType::Boolean, false),
                Field::new(format!("test2"), DataType::Boolean, false),
                Field::new(format!("test3"), DataType::Boolean, false),
                Field::new(format!("test4"), DataType::Boolean, false),
            ],
        );
        let cnf = vec![
            Dnf::new(vec![0,]),
            Dnf::new(vec![1, 2]),
            Dnf::new(vec![3]),
        ];
        let test1 = Arc::new(BooleanArray::from(vec![true, false, true, false, false ,true, true, false]));
        let test2 = Arc::new(BooleanArray::from(vec![false, true, true, false, false, true, true, false]));
        let test3 = Arc::new(BooleanArray::from(vec![false, false, true, true, false, false, true ,false])); 
        let test4 = Arc::new(BooleanArray::from(vec![true, false, false, false, true, false, true, false]));
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![test1, test2, test3, test4],
        ).unwrap();


        // compile and run JIT code
        let gen_func = create_boolean_query_fn(&cnf);
        gen_func.eval(&batch);
    }

    #[test]
    fn iterative_fib() -> Result<()> {
        let expected = r#"fn iterative_fib_0(n: i16) -> r: i16 {
    if n == 0 {
        r = 0;
    } else {
        n = n - 1;
        let a: i16;
        a = 0;
        r = 1;
        while n != 0 {
            let t: i16;
            t = r;
            r = r + a;
            a = t;
            n = n - 1;
        }
    }
}"#;
        let assembler = Assembler::default();
        let mut builder = assembler
            .new_func_builder("iterative_fib")
            .param("n", U16)
            .ret("r", U16);
        let mut fn_body = builder.enter_block();

        fn_body.if_block(
            |cond| cond.eq(cond.id("n")?, cond.lit_u16(0 as u16)),
            |t| {
                t.assign("r", t.lit_u16(0 as u16))?;
                Ok(())
            },
            |e| {
                e.assign("n", e.sub(e.id("n")?, e.lit_u16(1 as u16))?)?;
                e.declare_as("a", e.lit_u16(0 as u16))?;
                e.assign("r", e.lit_u16(1 as u16))?;
                e.while_block(
                    |cond| cond.ne(cond.id("n")?, cond.lit_u16(0 as u16)),
                    |w| {
                        w.declare_as("t", w.id("r")?)?;
                        w.assign("r", w.add(w.id("r")?, w.id("a")?)?)?;
                        w.assign("a", w.id("t")?)?;
                        w.assign("n", w.sub(w.id("n")?, w.lit_u16(1 as u16))?)?;
                        Ok(())
                    },
                )?;
                Ok(())
            },
        )?;

        let gen_func = fn_body.build();
        assert_eq!(format!("{}", &gen_func), expected);
        let mut jit = assembler.create_jit();
        assert_eq!(55, run_iterative_fib_code(&mut jit, gen_func, 10)?);
        Ok(())
    }

    unsafe fn run_code<I, O>(
        jit: &mut JIT,
        code: GeneratedFunction,
        input: I,
    ) -> Result<O> {
        // Pass the string to the JIT, and it returns a raw pointer to machine code.
        let code_ptr = jit.compile(code)?;
        // Cast the raw pointer to a typed function pointer. This is unsafe, because
        // this is the critical point where you have to trust that the generated code
        // is safe to be called.
        let code_fn = core::mem::transmute::<_, fn(I) -> O>(code_ptr);
        // And now we can call it!
        Ok(code_fn(input))
    }

    fn run_iterative_fib_code(
        jit: &mut JIT,
        code: GeneratedFunction,
        input: isize,
    ) -> Result<isize> {
        unsafe { run_code(jit, code, input) }
    }
}