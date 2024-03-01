// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::ConnectorError;

pub mod avro;
pub mod protobuf;
pub mod schema_registry;

const MESSAGE_NAME_KEY: &str = "message";
const KEY_MESSAGE_NAME_KEY: &str = "key.message";
const SCHEMA_LOCATION_KEY: &str = "schema.location";
const SCHEMA_REGISTRY_KEY: &str = "schema.registry";
const NAME_STRATEGY_KEY: &str = "schema.registry.name.strategy";

#[derive(Debug, thiserror::Error, thiserror_ext::Macro)]
#[error("Invalid option: {message}")]
pub struct InvalidOptionError {
    message: String,
    // #[backtrace]
    // source: Option<risingwave_common::error::BoxedError>,
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaFetchError {
    #[error(transparent)]
    InvalidOption(#[from] InvalidOptionError),
    #[error(transparent)]
    Request(#[from] schema_registry::ConcurrentRequestError),
    #[error("schema compilation error: {0}")]
    SchemaCompile(
        #[source]
        #[backtrace]
        risingwave_common::error::BoxedError,
    ),
    #[error("{0}")] // source+{0} is effectively transparent but allows backtrace
    YetToMigrate(
        #[source]
        #[backtrace]
        ConnectorError,
    ),
}
