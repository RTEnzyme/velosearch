pub mod ast;
pub mod api;
pub mod compile;
pub mod jit;


#[cfg(test)]
mod test {
    use crate::{utils::Result, jit::{api::Assembler, ast::U16}};

    use super::{jit::JIT, api::GeneratedFunction};

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