use std::{sync::Arc};

use chrono::{DateTime, Utc};
use datafusion::{execution::{context::SessionState, runtime_env::RuntimeEnv}, prelude::SessionConfig, sql::TableReference, logical_expr::LogicalPlanBuilder, datasource::{provider_as_source, TableProvider}, error::DataFusionError};
use parking_lot::RwLock;

use crate::{query::boolean_query::BooleanQuery, datasources::posting_table::PostingTable, utils::FastErr};
use crate::utils::Result;

#[derive(Clone)]
pub struct BooleanContext {
    /// UUID for this context
    session_id: String, 
    /// Session start time
    session_start_time: DateTime<Utc>,
    ///  Shared session state for the session
    state: Arc<RwLock<SessionState>>,
}

impl Default for BooleanContext {
    fn default() -> Self {
        Self::new()
    }
}

impl BooleanContext {
    pub fn new() -> Self {
        Self::with_config(SessionConfig::new())
    }

    /// Creates a new session context using the provided session configuration.
    pub fn with_config(config: SessionConfig) -> Self {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt(config, runtime)
    }

     /// Creates a new session context using the provided configuration and [`RuntimeEnv`].
     pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let state = SessionState::with_config_rt(config, runtime);
        Self::with_state(state)
    }

    /// Creates a new session context using the provided session state.
    pub fn with_state(state: SessionState) -> Self {
        Self {
            session_id: state.session_id().to_string(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Retrieves a [`Index`] representing a table previously
    /// registered by calling the [`register_index`] funciton
    /// 
    /// Return an error if no tables has been registered with the 
    /// provided reference.
    /// 
    /// [`register_index`]: BooleanContext::register_index
    pub async fn index<'a>(
        &self,
        index_ref: impl Into<TableReference<'a>>,
    ) -> Result<BooleanQuery> {
        let index_ref = index_ref.into();
        let index = index_ref.table().to_owned();
        let provider = self.index_provider(index_ref).await?;
        let plan = LogicalPlanBuilder::scan(
            &index,
            provider_as_source(Arc::clone(&provider)),
            None,
        )?.build()?;
        Ok(BooleanQuery::new(plan, self.state()))
    }

    /// Return a [`IndexProvider`] for the specified table.
    pub async fn index_provider<'a>(
        &self,
        index_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let index_ref = index_ref.into();
        let index = index_ref.table().to_owned();
        let schema = self.state.read().schema_for_ref(index_ref)?;
        match schema.table(&index).await {
            Some(ref provider) => Ok(Arc::clone(provider)),
            _ => Err(FastErr::DataFusionErr(DataFusionError::Plan(format!("No index named '{index}'")))),
        }
    }

    /// Snapshots the [`SessionState`] of this [`BooleanContext`] setting the
    /// `query_execution_start_time` to the current time
    pub fn state(&self) -> SessionState {
        let mut state = self.state.read().clone();
        state.execution_props.start_execution();
        state
    }

    /// Register a [`TableProvider`] as a table that can be
    /// referenced from SQL statements executed against this context.
    /// 
    /// Return the [`TableProvider`] previously registered for this 
    /// reference, if any
    pub fn register_index<'a>(
        &'a self,
        index_ref: impl Into<TableReference<'a>>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let index_ref = index_ref.into();
        let index = index_ref.table().to_owned();
        self.state
            .read()
            .schema_for_ref(index_ref)?
            .register_table(index, provider)
            .map_err(|e| e.into())
    }

    /// Return the session_id
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the session_start_time
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time.clone()
    }
}