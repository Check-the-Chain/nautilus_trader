// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Python bindings for native Rust execution algorithms.

use std::{
    cell::{RefCell, UnsafeCell},
    collections::HashMap,
    fmt::Debug,
    rc::Rc,
};

use nautilus_common::{
    actor::{Actor, DataActor, registry::get_actor_registry},
    cache::Cache,
    clock::Clock,
    component::{Component, get_component_registry},
    enums::ComponentState,
    python::{cache::PyCache, clock::PyClock, logging::PyLogger},
};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::identifiers::{ExecAlgorithmId, TraderId};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    ExecutionAlgorithmConfig, ImportableExecutionAlgorithmConfig, LimitChaserAlgorithm,
    LimitChaserAlgorithmConfig, TwapAlgorithm,
};

fn default_twap_exec_algorithm_id() -> ExecAlgorithmId {
    ExecAlgorithmId::new("TWAP")
}

fn json_config_from_pydict(
    py: Python<'_>,
    config: Py<PyDict>,
) -> PyResult<HashMap<String, serde_json::Value>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("default", py.eval(pyo3::ffi::c_str!("str"), None, None)?)?;
    let json_str: String = PyModule::import(py, "json")?
        .call_method("dumps", (config.bind(py),), Some(&kwargs))?
        .extract()?;

    let json_value: serde_json::Value = serde_json::from_str(&json_str).map_err(to_pyvalue_err)?;
    match json_value {
        serde_json::Value::Object(map) => Ok(map.into_iter().collect()),
        _ => Err(to_pyvalue_err("Config must be a dictionary")),
    }
}

fn register_native_exec_algorithm<T>(
    algo: &mut T,
    trader_id: TraderId,
    clock: Rc<RefCell<dyn Clock>>,
    cache: Rc<RefCell<Cache>>,
) -> anyhow::Result<()>
where
    T: DataActor + Component + Debug + 'static,
{
    Component::register(algo, trader_id, clock, cache)
}

#[allow(unsafe_code)]
fn register_native_exec_algorithm_in_global_registries<T>(inner: Rc<UnsafeCell<T>>)
where
    T: DataActor + Component + Debug + 'static,
{
    let component_id = Component::component_id(unsafe { &*inner.get() }).inner();
    let actor_id = Actor::id(unsafe { &*inner.get() });

    let component_trait_ref: Rc<UnsafeCell<dyn Component>> = inner.clone();
    get_component_registry().insert(component_id, component_trait_ref);

    let actor_trait_ref: Rc<UnsafeCell<dyn Actor>> = inner;
    get_actor_registry().insert(actor_id, actor_trait_ref);
}

#[pyo3::pymethods]
impl ExecutionAlgorithmConfig {
    #[new]
    #[pyo3(signature = (exec_algorithm_id=None, log_events=true, log_commands=true))]
    fn py_new(
        exec_algorithm_id: Option<ExecAlgorithmId>,
        log_events: bool,
        log_commands: bool,
    ) -> Self {
        Self {
            exec_algorithm_id,
            log_events,
            log_commands,
        }
    }

    #[getter]
    fn exec_algorithm_id(&self) -> Option<ExecAlgorithmId> {
        self.exec_algorithm_id
    }

    #[getter]
    fn log_events(&self) -> bool {
        self.log_events
    }

    #[getter]
    fn log_commands(&self) -> bool {
        self.log_commands
    }
}

#[pyo3::pymethods]
impl ImportableExecutionAlgorithmConfig {
    #[new]
    fn py_new(
        exec_algorithm_path: String,
        config_path: String,
        config: Py<PyDict>,
    ) -> PyResult<Self> {
        let json_config = Python::attach(|py| json_config_from_pydict(py, config))?;

        Ok(Self {
            exec_algorithm_path,
            config_path,
            config: json_config,
        })
    }

    #[getter]
    fn exec_algorithm_path(&self) -> &String {
        &self.exec_algorithm_path
    }

    #[getter]
    fn config_path(&self) -> &String {
        &self.config_path
    }

    #[getter]
    fn config(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let py_dict = PyDict::new(py);
        for (key, value) in &self.config {
            let json_str = serde_json::to_string(value).map_err(to_pyvalue_err)?;
            let py_value = PyModule::import(py, "json")?.call_method("loads", (json_str,), None)?;
            py_dict.set_item(key, py_value)?;
        }
        Ok(py_dict.unbind())
    }
}

#[pyo3::pymethods]
impl LimitChaserAlgorithmConfig {
    #[new]
    #[pyo3(signature = (
        exec_algorithm_id=None,
        log_events=true,
        log_commands=true,
        follow_offset_ticks=0,
        aggressive_offset_ticks=0,
        aggressive_after_secs=None,
        max_child_quantity=None,
        reprice_interval_ms=250,
        min_reprice_delta_ticks=1
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        exec_algorithm_id: Option<ExecAlgorithmId>,
        log_events: bool,
        log_commands: bool,
        follow_offset_ticks: u32,
        aggressive_offset_ticks: u32,
        aggressive_after_secs: Option<f64>,
        max_child_quantity: Option<f64>,
        reprice_interval_ms: u64,
        min_reprice_delta_ticks: u32,
    ) -> Self {
        Self {
            exec_algorithm_id,
            log_events,
            log_commands,
            follow_offset_ticks,
            aggressive_offset_ticks,
            aggressive_after_secs,
            max_child_quantity,
            reprice_interval_ms,
            min_reprice_delta_ticks,
        }
    }

    #[getter]
    fn exec_algorithm_id(&self) -> Option<ExecAlgorithmId> {
        self.exec_algorithm_id
    }

    #[getter]
    fn follow_offset_ticks(&self) -> u32 {
        self.follow_offset_ticks
    }

    #[getter]
    fn aggressive_offset_ticks(&self) -> u32 {
        self.aggressive_offset_ticks
    }

    #[getter]
    fn aggressive_after_secs(&self) -> Option<f64> {
        self.aggressive_after_secs
    }

    #[getter]
    fn max_child_quantity(&self) -> Option<f64> {
        self.max_child_quantity
    }

    #[getter]
    fn reprice_interval_ms(&self) -> u64 {
        self.reprice_interval_ms
    }

    #[getter]
    fn min_reprice_delta_ticks(&self) -> u32 {
        self.min_reprice_delta_ticks
    }
}

#[derive(Clone)]
enum NativeExecutionAlgorithm {
    Twap(Rc<UnsafeCell<TwapAlgorithm>>),
    LimitChaser(Rc<UnsafeCell<LimitChaserAlgorithm>>),
}

impl NativeExecutionAlgorithm {
    fn exec_algorithm_id(&self) -> ExecAlgorithmId {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => unsafe { &*inner.get() }.core.id(),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => unsafe { &*inner.get() }.core.id(),
        }
    }

    fn trader_id(&self) -> Option<TraderId> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => unsafe { &*inner.get() }.trader_id(),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => unsafe { &*inner.get() }.trader_id(),
        }
    }

    fn clock_rc(&self) -> Rc<RefCell<dyn Clock>> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => unsafe { &*inner.get() }.clock_rc(),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => unsafe { &*inner.get() }.clock_rc(),
        }
    }

    fn cache_rc(&self) -> Rc<RefCell<Cache>> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => unsafe { &*inner.get() }.cache_rc(),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => unsafe { &*inner.get() }.cache_rc(),
        }
    }

    fn state(&self) -> ComponentState {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => unsafe { &*inner.get() }.state(),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => unsafe { &*inner.get() }.state(),
        }
    }

    fn is_ready(&self) -> bool {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::is_ready(unsafe { &*inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::is_ready(unsafe { &*inner.get() }),
        }
    }

    fn is_running(&self) -> bool {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::is_running(unsafe { &*inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::is_running(unsafe { &*inner.get() }),
        }
    }

    fn is_stopped(&self) -> bool {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::is_stopped(unsafe { &*inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::is_stopped(unsafe { &*inner.get() }),
        }
    }

    fn is_disposed(&self) -> bool {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::is_disposed(unsafe { &*inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::is_disposed(unsafe { &*inner.get() }),
        }
    }

    fn is_degraded(&self) -> bool {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::is_degraded(unsafe { &*inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::is_degraded(unsafe { &*inner.get() }),
        }
    }

    fn is_faulted(&self) -> bool {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::is_faulted(unsafe { &*inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::is_faulted(unsafe { &*inner.get() }),
        }
    }

    fn register(
        &mut self,
        trader_id: TraderId,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => register_native_exec_algorithm(
                unsafe { &mut *inner.get() },
                trader_id,
                clock,
                cache,
            ),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => register_native_exec_algorithm(
                unsafe { &mut *inner.get() },
                trader_id,
                clock,
                cache,
            ),
        }
    }

    fn register_in_global_registries(&self) {
        match self {
            Self::Twap(inner) => register_native_exec_algorithm_in_global_registries(inner.clone()),
            Self::LimitChaser(inner) => {
                register_native_exec_algorithm_in_global_registries(inner.clone());
            }
        }
    }

    fn start(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::start(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::start(unsafe { &mut *inner.get() }),
        }
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::stop(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::stop(unsafe { &mut *inner.get() }),
        }
    }

    fn resume(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::resume(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::resume(unsafe { &mut *inner.get() }),
        }
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::reset(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::reset(unsafe { &mut *inner.get() }),
        }
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::dispose(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::dispose(unsafe { &mut *inner.get() }),
        }
    }

    fn degrade(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::degrade(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::degrade(unsafe { &mut *inner.get() }),
        }
    }

    fn fault(&mut self) -> anyhow::Result<()> {
        match self {
            #[allow(unsafe_code)]
            Self::Twap(inner) => Component::fault(unsafe { &mut *inner.get() }),
            #[allow(unsafe_code)]
            Self::LimitChaser(inner) => Component::fault(unsafe { &mut *inner.get() }),
        }
    }
}

#[pyclass(
    module = "nautilus_trader.execution.native",
    name = "ExecutionAlgorithm",
    unsendable
)]
pub struct PyExecutionAlgorithm {
    inner: NativeExecutionAlgorithm,
    logger: PyLogger,
}

impl Debug for PyExecutionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(PyExecutionAlgorithm))
            .field("id", &self.exec_algorithm_id())
            .finish()
    }
}

impl PyExecutionAlgorithm {
    fn from_twap(config: Option<ExecutionAlgorithmConfig>) -> Self {
        let mut config = config.unwrap_or_default();
        if config.exec_algorithm_id.is_none() {
            config.exec_algorithm_id = Some(default_twap_exec_algorithm_id());
        }

        let inner_algo = TwapAlgorithm::new(config);
        let logger = PyLogger::new(inner_algo.core.actor.actor_id.as_str());

        Self {
            inner: NativeExecutionAlgorithm::Twap(Rc::new(UnsafeCell::new(inner_algo))),
            logger,
        }
    }

    fn from_limit_chaser(config: Option<LimitChaserAlgorithmConfig>) -> Self {
        let inner_algo = LimitChaserAlgorithm::new(config.unwrap_or_default());
        let logger = PyLogger::new(inner_algo.core.actor.actor_id.as_str());

        Self {
            inner: NativeExecutionAlgorithm::LimitChaser(Rc::new(UnsafeCell::new(inner_algo))),
            logger,
        }
    }

    pub fn exec_algorithm_id(&self) -> ExecAlgorithmId {
        self.inner.exec_algorithm_id()
    }

    pub fn register(
        &mut self,
        trader_id: TraderId,
        clock: Rc<RefCell<dyn Clock>>,
        cache: Rc<RefCell<Cache>>,
    ) -> anyhow::Result<()> {
        self.inner.register(trader_id, clock, cache)
    }

    pub fn register_in_global_registries(&self) {
        self.inner.register_in_global_registries();
    }
}

#[pyo3::pymethods]
impl PyExecutionAlgorithm {
    #[getter]
    fn id(&self) -> ExecAlgorithmId {
        self.exec_algorithm_id()
    }

    #[getter]
    fn trader_id(&self) -> Option<TraderId> {
        self.inner.trader_id()
    }

    #[getter]
    fn clock(&self) -> PyResult<PyClock> {
        if self.inner.trader_id().is_none() {
            return Err(to_pyruntime_err(
                "Execution algorithm must be registered before accessing clock",
            ));
        }
        Ok(PyClock::from_rc(self.inner.clock_rc()))
    }

    #[getter]
    fn cache(&self) -> PyResult<PyCache> {
        if self.inner.trader_id().is_none() {
            return Err(to_pyruntime_err(
                "Execution algorithm must be registered before accessing cache",
            ));
        }
        Ok(PyCache::from_rc(self.inner.cache_rc()))
    }

    #[getter]
    fn log(&self) -> PyLogger {
        self.logger.clone()
    }

    fn state(&self) -> ComponentState {
        self.inner.state()
    }

    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }

    fn is_disposed(&self) -> bool {
        self.inner.is_disposed()
    }

    fn is_degraded(&self) -> bool {
        self.inner.is_degraded()
    }

    fn is_faulted(&self) -> bool {
        self.inner.is_faulted()
    }

    fn start(&mut self) -> PyResult<()> {
        self.inner.start().map_err(to_pyruntime_err)
    }

    fn stop(&mut self) -> PyResult<()> {
        self.inner.stop().map_err(to_pyruntime_err)
    }

    fn resume(&mut self) -> PyResult<()> {
        self.inner.resume().map_err(to_pyruntime_err)
    }

    fn reset(&mut self) -> PyResult<()> {
        self.inner.reset().map_err(to_pyruntime_err)
    }

    fn dispose(&mut self) -> PyResult<()> {
        self.inner.dispose().map_err(to_pyruntime_err)
    }

    fn degrade(&mut self) -> PyResult<()> {
        self.inner.degrade().map_err(to_pyruntime_err)
    }

    fn fault(&mut self) -> PyResult<()> {
        self.inner.fault().map_err(to_pyruntime_err)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pyfunction(name = "TwapAlgorithm")]
#[pyo3(signature = (config=None))]
pub fn py_twap_algorithm(config: Option<ExecutionAlgorithmConfig>) -> PyExecutionAlgorithm {
    PyExecutionAlgorithm::from_twap(config)
}

#[pyfunction(name = "LimitChaserAlgorithm")]
#[pyo3(signature = (config=None))]
pub fn py_limit_chaser_algorithm(
    config: Option<LimitChaserAlgorithmConfig>,
) -> PyExecutionAlgorithm {
    PyExecutionAlgorithm::from_limit_chaser(config)
}
