use datafusion::{arrow::error::ArrowError, error::DataFusionError};
use failure::Fail;
pub type Result<T> = std::result::Result<T, FastErr>;

#[derive(Fail, Debug)]
pub enum FastErr {
    #[fail(display = "IO Err: {}", _0)]
    IoError(#[cause] std::io::Error),
    // #[fail(display = "datafusion Err: {}", error)]
    #[fail(display = "Convert Err: {}", _0)]
    ConvertErr(#[cause] core::convert::Infallible),
    #[fail(display = "Arrow Err: {}", _0)]
    ArrowErr(#[cause] ArrowError),
    #[fail(display = "DataFusionErr: {}", _0)]
    DataFusionErr(#[cause] DataFusionError),
    #[fail(display = "Unimplement: {}", _0)]
    UnimplementErr(String),
}

impl From<core::convert::Infallible> for FastErr {
    fn from(err: core::convert::Infallible) -> FastErr {
        FastErr::ConvertErr(err)
    }
}

impl  From<ArrowError> for FastErr {
   fn from(value: ArrowError) -> Self {
       FastErr::ArrowErr(value)
   } 
}

impl From<DataFusionError> for FastErr {
    fn from(value: DataFusionError) -> Self {
        FastErr::DataFusionErr(value)
    }
}