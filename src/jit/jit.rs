use super::api::GeneratedFunction;
use super::ast::{U8, U16, BinaryExpr, Expr, JITType, Literal, Stmt, TypedLit, BOOL, NIL, BooleanExpr, Dnf, I64};
use crate::utils::{Result, FastErr};
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};
use tracing::debug;
use std::collections::HashMap;

/// The basic JIT class
#[allow(clippy::upper_case_acronyms)]
pub struct JIT {
    /// The function builder context, which is reused across multiple
    /// FunctionBuilder instances.
    builder_context: FunctionBuilderContext,

    /// The main Cranelift context, which holds the state for codegen. Cranelift
    /// separates this from `Module` to allow for parallel compilation, with a
    /// context per thread, though this is not the case now.
    ctx: codegen::Context,

    /// The module, with the jit backend, which manages the JIT'd
    /// functions.
    module: JITModule,
}

impl Default for JIT {
    #[cfg(target_arch = "x86_64")]
    fn default() -> Self {
        let builder = JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        let module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }
    }

    #[cfg(target_arch = "aarch64")]
    fn default() -> Self {
        let mut flag_builder = settings::builder();
        // On at least AArch64, "colocated" calls use shorter-range relocations,
        // which might not reach all definitions; we can't handle that here, so
        // we require long-range relocation types.
        flag_builder.set("use_colocated_libcalls", "false").unwrap();
        flag_builder.set("is_pic", "false").unwrap();
        let isa_builder = cranelift_native::builder().unwrap_or_else(|msg| {
            panic!("host machine is not supported: {msg}");
        });
        let isa = isa_builder
            .finish(settings::Flags::new(flag_builder))
            .unwrap_or_else(|msg| {
                panic!("host machine is not supported: {msg}");
            });
        let builder =
            JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        let module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }
    }
}

impl JIT {
    /// New while registering external functions
    pub fn new<It, K>(symbols: It) -> Self
    where
        It: IntoIterator<Item = (K, *const u8)>,
        K: Into<String>,
    {
        let mut flag_builder = settings::builder();
        flag_builder.set("use_colocated_libcalls", "false").unwrap();

        #[cfg(target_arch = "x86_64")]
        flag_builder.set("is_pic", "true").unwrap();

        #[cfg(target_arch = "aarch64")]
        flag_builder.set("is_pic", "false").unwrap();

        flag_builder.set("opt_level", "speed").unwrap();
        flag_builder.set("enable_simd", "true").unwrap();
        let isa_builder = cranelift_native::builder().unwrap_or_else(|msg| {
            panic!("host machine is not supported: {msg}");
        });
        let isa = isa_builder
            .finish(settings::Flags::new(flag_builder))
            .unwrap();
        let mut builder =
            JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        builder.symbols(symbols);
        let module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }
    }

    /// Compile the generated function into machine code.
    pub fn compile(&mut self, func: GeneratedFunction) -> Result<*const u8> {
        let GeneratedFunction {
            name,
            params,
            body,
            ret,
        } = func;

        // Translate the AST nodes into Cranelift IR.
        self.translate(params, ret, body)?;

        // Next, declare the function to jit. Functions must be declared
        // before they can be called, or defined.
        let id = self
            .module
            .declare_function(&name, Linkage::Export, &self.ctx.func.signature)
            .map_err(|e| {
                FastErr::InternalErr(format!(
                    "failed in declare the function to jit: {e:?}"
                ))
            })?;

        // Define the function to jit. This finishes compilation, although
        // there may be outstanding relocations to perform. Currently, jit
        // cannot finish relocations until all functions to be called are
        // defined. For now, we'll just finalize the function below.
        self.module
            .define_function(id, &mut self.ctx)
            .map_err(|e| {
                FastErr::InternalErr(format!(
                    "failed in define the function to jit: {e:?}"
                ))
            })?;

        // Now that compilation is finished, we can clear out the context state.
        self.module.clear_context(&mut self.ctx);

        // Finalize the functions which we just defined, which resolves any
        // outstanding relocations (patching in addresses, now that they're
        // available).
        self.module.finalize_definitions().unwrap();
        // We can now retrieve a pointer to the machine code.
        let code = self.module.get_finalized_function(id);

        Ok(code)
    }

    // Translate into Cranelift IR.
    fn translate(
        &mut self,
        params: Vec<(String, JITType)>,
        the_return: Option<(String, JITType)>,
        stmts: Vec<Stmt>,
    ) -> Result<()> {
        for param in &params {
            self.ctx
                .func
                .signature
                .params
                .push(AbiParam::new(param.1.native));
        }

        let mut void_return: bool = false;

        // We currently only supports one return value, though
        // Cranelift is designed to support more.
        match the_return {
            None => void_return = true,
            Some(ref ret) => {
                self.ctx
                    .func
                    .signature
                    .returns
                    .push(AbiParam::new(ret.1.native));
            }
        }

        // Create the builder to build a function.
        let mut builder =
            FunctionBuilder::new(&mut self.ctx.func, &mut self.builder_context);

        // Create the entry block, to start emitting code in.
        let entry_block = builder.create_block();

        // Since this is the entry block, add block parameters corresponding to
        // the function's parameters.
        builder.append_block_params_for_function_params(entry_block);

        // Tell the builder to emit code in this block.
        builder.switch_to_block(entry_block);

        // And, tell the builder that this block will have no further
        // predecessors. Since it's the entry block, it won't have any
        // predecessors.
        builder.seal_block(entry_block);

        // Walk the AST and declare all variables.
        let variables = declare_variables(
            &mut builder,
            &params,
            the_return.as_ref(),
            &stmts,
            entry_block,
        );

        // Now translate the statements of the function body.
        let mut trans = FunctionTranslator {
            builder,
            variables,
            module: &mut self.module,
        };
        for stmt in stmts {
            trans.translate_stmt(stmt)?;
        }

        if !void_return {
            // Set up the return variable of the function. Above, we declared a
            // variable to hold the return value. Here, we just do a use of that
            // variable.
            let return_variable = trans
                .variables
                .get(&the_return.as_ref().unwrap().0)
                .unwrap();
            let return_value = trans.builder.use_var(*return_variable);

            // Emit the return instruction.
            trans.builder.ins().return_(&[return_value]);
        } else {
            trans.builder.ins().return_(&[]);
        }

        // Tell the builder we're done with this function.
        debug!("{:?}",trans.builder.func);
        trans.builder.finalize();
        Ok(())
    }
}

/// A collection of state used for translating from AST nodes
/// into Cranelift IR.
struct FunctionTranslator<'a> {
    builder: FunctionBuilder<'a>,
    variables: HashMap<String, Variable>,
    module: &'a mut JITModule,
}

impl<'a> FunctionTranslator<'a> {
    fn translate_stmt(&mut self, stmt: Stmt) -> Result<()> {
        match stmt {
            Stmt::IfElse(condition, then_body, else_body) => {
                self.translate_if_else(*condition, then_body, else_body)
            }
            Stmt::WhileLoop(condition, loop_body) => {
                self.translate_while_loop(*condition, loop_body)
            }
            Stmt::Assign(name, expr) => self.translate_assign(name, *expr),
            Stmt::Call(name, args) => {
                self.translate_call_stmt(name, args, NIL)?;
                Ok(())
            }
            Stmt::Declare(_, _) => Ok(()),
            Stmt::Store(value, ptr) => self.translate_store(*ptr, *value),
        }
    }

    fn translate_typed_lit(&mut self, tl: TypedLit) -> Value {
        match tl {
            TypedLit::Bool(b) => self.builder.ins().iconst(BOOL.native, b as i64),
            TypedLit::U8(i) => self.builder.ins().iconst(U8.native, i as i64),
            TypedLit::U16(u) => self.builder.ins().iconst(U16.native, u as i64),
            TypedLit::I64(i) => self.builder.ins().iconst(I64.native, i),
        }
    }

    /// When you write out instructions in Cranelift, you get back `Value`s. You
    /// can then use these references in other instructions.
    fn translate_expr(&mut self, expr: Expr) -> Result<Value> {
        match expr {
            Expr::Literal(nl) => self.translate_literal(nl),
            Expr::Identifier(name, _) => {
                // `use_var` is used to read the value of a variable.
                let variable = self.variables.get(&name).ok_or_else(|| {
                    FastErr::InternalErr("variable not defined".to_owned())
                })?;
                Ok(self.builder.use_var(*variable))
            }
            Expr::Binary(b) => self.translate_binary_expr(b),
            Expr::BooleanExpr(b) => self.translate_boolean_expr(b),
            Expr::Call(name, args, ret) => self.translate_call_expr(name, args, ret),
            Expr::Load(ptr, ty) => self.translate_deref(*ptr, ty),
        }
    }

    fn translate_literal(&mut self, expr: Literal) -> Result<Value> {
        match expr {
            Literal::Parsing(literal, ty) => self.translate_string_lit(literal, ty),
            Literal::Typed(lt) => Ok(self.translate_typed_lit(lt)),
        }
    }

    fn translate_boolean_expr(&mut self, expr: BooleanExpr) -> Result<Value> {
        let mut body_block;
        let else_block = self.builder.create_block();
        self.builder.append_block_param(else_block, U8.native);
        let mut init_v = self.builder.ins().iconst(U8.native, 0xFF);
        for mut e in expr.cnf.into_iter() {
            let mut value = match e.remove(0) {
                Dnf::Normal(v) => self.translate_expr(v).unwrap(),
                Dnf::Not(v) => {
                    let v = self.translate_expr(v).unwrap();
                    self.builder.ins().bnot(v)
                }
            };
            if e.len() != 0 {
                e
                .into_iter()
                .for_each(|v| {
                    value = self.translate_dnf_expr(value, v)
                })
            }
            // let value = self.builder.ins().ireduce(U8.native, value);
            init_v = self.builder.ins().band(init_v, value);
            body_block = self.builder.create_block();
            self.builder.append_block_param(body_block, U8.native);
            self.builder.ins().brif(
                init_v,
                body_block,
                &[init_v],
                else_block,
                &[init_v],
            );
            self.builder.switch_to_block(body_block);
            self.builder.seal_block(body_block);
        }
        self.builder.ins().jump(else_block, &[init_v]);
        self.builder.switch_to_block(else_block);
        self.builder.seal_block(else_block);
        Ok(self.builder.block_params(else_block)[0])
    }

    fn translate_dnf_expr(&mut self, lhs: Value, rhs: Dnf) -> Value {
        match rhs {
            Dnf::Not(rhs) => {
                let rhs = self.translate_expr(rhs).unwrap();
                self.translate_bitwise_or(lhs, rhs)
            }
            Dnf::Normal(rhs) => {
                let rhs = self.translate_expr(rhs).unwrap();
                self.translate_bitwise_or(lhs, rhs)
            }
        }
    }

    fn translate_bitwise_or(&mut self, lhs: Value, rhs: Value) -> Value {
        self.builder.ins().bor(lhs, rhs)
    }

    fn translate_binary_expr(&mut self, expr: BinaryExpr) -> Result<Value> {
        match expr {
            BinaryExpr::Eq(lhs, rhs) => {
                let ty = lhs.get_type();
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    self.translate_icmp(IntCC::Equal, *lhs, *rhs)
                } else {
                    internal_err!("Unsupported type {} for equal comparison", ty)
                }
            }
            BinaryExpr::Ne(lhs, rhs) => {
                let ty = lhs.get_type();
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    self.translate_icmp(IntCC::NotEqual, *lhs, *rhs)
                } else {
                    internal_err!("Unsupported type {} for not equal comparison", ty)
                }
            }
            BinaryExpr::Lt(lhs, rhs) => {
                let ty = lhs.get_type();
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    self.translate_icmp(IntCC::SignedLessThan, *lhs, *rhs)
                } else {
                    internal_err!("Unsupported type {} for less than comparison", ty)
                }
            }
            BinaryExpr::Le(lhs, rhs) => {
                let ty = lhs.get_type();
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    self.translate_icmp(IntCC::SignedLessThanOrEqual, *lhs, *rhs)
                } else {
                    internal_err!(
                        "Unsupported type {} for less than or equal comparison",
                        ty
                    )
                }
            }
            BinaryExpr::Gt(lhs, rhs) => {
                let ty = lhs.get_type();
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    self.translate_icmp(IntCC::SignedGreaterThan, *lhs, *rhs)
                } else {
                    internal_err!("Unsupported type {} for greater than comparison", ty)
                }
            }
            BinaryExpr::Ge(lhs, rhs) => {
                let ty = lhs.get_type();
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    self.translate_icmp(IntCC::SignedGreaterThanOrEqual, *lhs, *rhs)
                } else {
                    internal_err!(
                        "Unsupported type {} for greater than or equal comparison",
                        ty
                    )
                }
            }
            BinaryExpr::Add(lhs, rhs) => {
                let ty = lhs.get_type();
                let lhs = self.translate_expr(*lhs)?;
                let rhs = self.translate_expr(*rhs)?;
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    Ok(self.builder.ins().iadd(lhs, rhs))
                } else {
                    internal_err!("Unsupported type {} for add", ty)
                }
            }
            BinaryExpr::Sub(lhs, rhs) => {
                let ty = lhs.get_type();
                let lhs = self.translate_expr(*lhs)?;
                let rhs = self.translate_expr(*rhs)?;
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    Ok(self.builder.ins().isub(lhs, rhs))
                } else {
                    internal_err!("Unsupported type {} for sub", ty)
                }
            }
            BinaryExpr::Mul(lhs, rhs) => {
                let ty = lhs.get_type();
                let lhs = self.translate_expr(*lhs)?;
                let rhs = self.translate_expr(*rhs)?;
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    Ok(self.builder.ins().imul(lhs, rhs))
                } else {
                    internal_err!("Unsupported type {} for mul", ty)
                }
            }
            BinaryExpr::Div(lhs, rhs) => {
                let ty = lhs.get_type();
                let lhs = self.translate_expr(*lhs)?;
                let rhs = self.translate_expr(*rhs)?;
                if ty.code >= 0x76 && ty.code <= 0x79 {
                    Ok(self.builder.ins().udiv(lhs, rhs))
                } else {
                    internal_err!("Unsupported type {} for div", ty)
                }
            }
            BinaryExpr::BitwiseAnd(lhs, rhs) => {
                let lhs = self.translate_expr(*lhs)?;
                let rhs = self.translate_expr(*rhs)?;
                Ok(self.builder.ins().band(lhs, rhs))
            }
            BinaryExpr::BitwiseOr(lhs, rhs) => {
                let lhs = self.translate_expr(*lhs)?;
                let rhs = self.translate_expr(*rhs)?;
                Ok(self.builder.ins().bor(lhs, rhs))
            }
        }
    }

    fn translate_string_lit(&mut self, lit: String, ty: JITType) -> Result<Value> {
        match ty.code {
            // 0x70 => {
            //     let b = lit.parse::<bool>().unwrap();
            //     Ok(self.builder.ins().iconst(ty.native, b as []))
            // }
            0x76 => {
                let i = lit.parse::<i8>().unwrap();
                Ok(self.builder.ins().iconst(ty.native, i as i64))
            }
            0x77 => {
                let i = lit.parse::<i16>().unwrap();
                Ok(self.builder.ins().iconst(ty.native, i as i64))
            }
            0x78 => {
                let i = lit.parse::<i32>().unwrap();
                Ok(self.builder.ins().iconst(ty.native, i as i64))
            }
            0x79 => {
                let i = lit.parse::<i64>().unwrap();
                Ok(self.builder.ins().iconst(ty.native, i))
            }
            0x7b => {
                let f = lit.parse::<f32>().unwrap();
                Ok(self.builder.ins().f32const(f))
            }
            0x7c => {
                let f = lit.parse::<f64>().unwrap();
                Ok(self.builder.ins().f64const(f))
            }
            _ => internal_err!("Unsupported type {} for string literal", ty),
        }
    }

    fn translate_assign(&mut self, name: String, expr: Expr) -> Result<()> {
        // `def_var` is used to write the value of a variable. Note that
        // variables can have multiple definitions. Cranelift will
        // convert them into SSA form for itself automatically.
        let new_value = self.translate_expr(expr)?;
        let variable = self.variables.get(&*name).unwrap();
        self.builder.def_var(*variable, new_value);
        Ok(())
    }

    fn translate_deref(&mut self, ptr: Expr, ty: JITType) -> Result<Value> {
        let ptr = self.translate_expr(ptr)?;
        Ok(self.builder.ins().load(ty.native, MemFlags::new(), ptr, 0))
    }

    fn translate_store(&mut self, ptr: Expr, value: Expr) -> Result<()> {
        let ptr = self.translate_expr(ptr)?;
        let value = self.translate_expr(value)?;
        self.builder.ins().store(MemFlags::new(), value, ptr, 0);
        Ok(())
    }

    fn translate_icmp(&mut self, cmp: IntCC, lhs: Expr, rhs: Expr) -> Result<Value> {
        let lhs = self.translate_expr(lhs)?;
        let rhs = self.translate_expr(rhs)?;
        let c = self.builder.ins().icmp(cmp, lhs, rhs);
        Ok(c)
    }

    fn translate_if_else(
        &mut self,
        condition: Expr,
        then_body: Vec<Stmt>,
        else_body: Vec<Stmt>,
    ) -> Result<()> {
        let condition_value = self.translate_expr(condition)?;

        let then_block = self.builder.create_block();
        let else_block = self.builder.create_block();
        let merge_block = self.builder.create_block();

        // Test the if condition and conditionally branch.
        self.builder.ins().brif(
            condition_value,
            then_block,
            &[],
            else_block,
            &[],
        );

        self.builder.switch_to_block(then_block);
        self.builder.seal_block(then_block);
        for stmt in then_body {
            self.translate_stmt(stmt)?;
        }

        // Jump to the merge block, passing it the block return value.
        self.builder.ins().jump(merge_block, &[]);

        self.builder.switch_to_block(else_block);
        self.builder.seal_block(else_block);
        for stmt in else_body {
            self.translate_stmt(stmt)?;
        }

        // Jump to the merge block, passing it the block return value.
        self.builder.ins().jump(merge_block, &[]);

        // Switch to the merge block for subsequent statements.
        self.builder.switch_to_block(merge_block);

        // We've now seen all the predecessors of the merge block.
        self.builder.seal_block(merge_block);
        Ok(())
    }

    fn translate_while_loop(
        &mut self,
        condition: Expr,
        loop_body: Vec<Stmt>,
    ) -> Result<()> {
        let header_block = self.builder.create_block();
        let body_block = self.builder.create_block();
        let exit_block = self.builder.create_block();

        self.builder.ins().jump(header_block, &[]);
        self.builder.switch_to_block(header_block);

        let condition_value = self.translate_expr(condition)?;
        self.builder.ins().brif(
            condition_value,
            body_block,
            &[],
            exit_block,
            &[],
        );

        self.builder.switch_to_block(body_block);
        self.builder.seal_block(body_block);

        for stmt in loop_body {
            self.translate_stmt(stmt)?;
        }
        self.builder.ins().jump(header_block, &[]);

        self.builder.switch_to_block(exit_block);

        // We've reached the bottom of the loop, so there will be no
        // more backedges to the header to exits to the bottom.
        self.builder.seal_block(header_block);
        self.builder.seal_block(exit_block);
        Ok(())
    }

    fn translate_call_expr(
        &mut self,
        name: String,
        args: Vec<Expr>,
        ret: JITType,
    ) -> Result<Value> {
        let mut sig = self.module.make_signature();

        // Add a parameter for each argument.
        for arg in &args {
            sig.params.push(AbiParam::new(arg.get_type().native));
        }

        if ret.code == 0 {
            return internal_err!(
                "Call function {}(..) has void type, it can not be an expression",
                &name
            );
        } else {
            sig.returns.push(AbiParam::new(ret.native));
        }

        let callee = self
            .module
            .declare_function(&name, Linkage::Import, &sig)
            .expect("problem declaring function");
        let local_callee = self.module.declare_func_in_func(callee, self.builder.func);

        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.translate_expr(arg)?)
        }
        let call = self.builder.ins().call(local_callee, &arg_values);
        Ok(self.builder.inst_results(call)[0])
    }

    fn translate_call_stmt(
        &mut self,
        name: String,
        args: Vec<Expr>,
        ret: JITType,
    ) -> Result<()> {
        let mut sig = self.module.make_signature();

        // Add a parameter for each argument.
        for arg in &args {
            sig.params.push(AbiParam::new(arg.get_type().native));
        }

        if ret.code != 0 {
            sig.returns.push(AbiParam::new(ret.native));
        }

        let callee = self
            .module
            .declare_function(&name, Linkage::Import, &sig)
            .expect("problem declaring function");
        let local_callee = self.module.declare_func_in_func(callee, self.builder.func);

        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.translate_expr(arg)?)
        }
        let _ = self.builder.ins().call(local_callee, &arg_values);
        Ok(())
    }
}

fn typed_zero(typ: JITType, builder: &mut FunctionBuilder) -> Value {
    match typ.code {
        0x76 => builder.ins().iconst(typ.native, 0),
        0x77 => builder.ins().iconst(typ.native, 0),
        0x78 => builder.ins().iconst(typ.native, 0),
        0x79 => builder.ins().iconst(typ.native, 0),
        0x7b => builder.ins().f32const(0.0),
        0x7c => builder.ins().f64const(0.0),
        0x7e => builder.ins().null(typ.native),
        0x7f => builder.ins().null(typ.native),
        _ => panic!("unsupported type"),
    }
}

fn declare_variables(
    builder: &mut FunctionBuilder,
    params: &[(String, JITType)],
    the_return: Option<&(String, JITType)>,
    stmts: &[Stmt],
    entry_block: Block,
) -> HashMap<String, Variable> {
    let mut variables = HashMap::new();
    let mut index = 0;

    for (i, name) in params.iter().enumerate() {
        let val = builder.block_params(entry_block)[i];
        let var = declare_variable(builder, &mut variables, &mut index, &name.0, name.1);
        builder.def_var(var, val);
    }

    if let Some(ret) = the_return {
        let zero = typed_zero(ret.1, builder);
        let return_variable =
            declare_variable(builder, &mut variables, &mut index, &ret.0, ret.1);
        builder.def_var(return_variable, zero);
    }

    for stmt in stmts {
        declare_variables_in_stmt(builder, &mut variables, &mut index, stmt);
    }

    variables
}

/// Recursively descend through the AST, translating all declarations.
fn declare_variables_in_stmt(
    builder: &mut FunctionBuilder,
    variables: &mut HashMap<String, Variable>,
    index: &mut usize,
    stmt: &Stmt,
) {
    match *stmt {
        Stmt::IfElse(_, ref then_body, ref else_body) => {
            for stmt in then_body {
                declare_variables_in_stmt(builder, variables, index, stmt);
            }
            for stmt in else_body {
                declare_variables_in_stmt(builder, variables, index, stmt);
            }
        }
        Stmt::WhileLoop(_, ref loop_body) => {
            for stmt in loop_body {
                declare_variables_in_stmt(builder, variables, index, stmt);
            }
        }
        Stmt::Declare(ref name, typ) => {
            declare_variable(builder, variables, index, name, typ);
        }
        _ => {}
    }
}

/// Declare a single variable declaration.
fn declare_variable(
    builder: &mut FunctionBuilder,
    variables: &mut HashMap<String, Variable>,
    index: &mut usize,
    name: &str,
    typ: JITType,
) -> Variable {
    let var = Variable::new(*index);
    if !variables.contains_key(name) {
        variables.insert(name.into(), var);
        builder.declare_var(var, typ.native);
        *index += 1;
    }
    var
}

macro_rules! internal_err {
    ($($arg:tt)*) => {
        Err(FastErr::InternalErr(format!($($arg)*)))
    };
}

use internal_err;