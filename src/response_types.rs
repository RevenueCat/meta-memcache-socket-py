use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;

use crate::response_flags::ResponseFlags;

#[pyclass(frozen, eq, skip_from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct Miss {}

#[pymethods]
impl Miss {
    #[new]
    pub fn new() -> Self {
        Miss {}
    }

    pub fn __repr__(&self) -> &'static str {
        "Miss()"
    }

    pub fn __bool__(&self) -> bool {
        false
    }
}

#[pyclass(frozen, eq, skip_from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct NotStored {}

#[pymethods]
impl NotStored {
    #[new]
    pub fn new() -> Self {
        NotStored {}
    }

    pub fn __repr__(&self) -> &'static str {
        "NotStored()"
    }

    pub fn __bool__(&self) -> bool {
        false
    }
}

#[pyclass(frozen, eq, skip_from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct Conflict {}

#[pymethods]
impl Conflict {
    #[new]
    pub fn new() -> Self {
        Conflict {}
    }

    pub fn __repr__(&self) -> &'static str {
        "Conflict()"
    }

    pub fn __bool__(&self) -> bool {
        false
    }
}

#[pyclass(subclass, skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct Success {
    #[pyo3(get)]
    pub flags: ResponseFlags,
}

#[pymethods]
impl Success {
    #[new]
    pub fn new(flags: ResponseFlags) -> Self {
        Success { flags }
    }

    pub fn __repr__(&self) -> String {
        format!("Success(flags={})", self.flags.__str__())
    }
}

#[pyclass(extends=Success, skip_from_py_object)]
pub struct Value {
    #[pyo3(get)]
    pub size: u32,
    #[pyo3(get, set)]
    pub value: Option<Py<PyAny>>,
}

#[pymethods]
impl Value {
    #[new]
    pub fn new(size: u32, flags: ResponseFlags, value: Option<Py<PyAny>>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(Success::new(flags)).add_subclass(Value { size, value })
    }

    pub fn __repr__(slf: PyRef<'_, Self>) -> String {
        let super_ = slf.as_super();
        format!(
            "Value(size={}, flags={}, value={:?})",
            slf.size,
            super_.flags.__str__(),
            slf.value.as_ref().map(|_| "..."),
        )
    }
}
