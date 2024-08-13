use std::num::NonZeroU64;

use thiserror::Error;

use crate::{LicenseKeyError, LicenseManager};

/// The error type...
#[derive(Debug, Clone, Error)]
#[error("invalid license key")]
pub enum CpuCoreLimitExceeded {
    #[error("cannot check CPU core limit due to license key error")]
    LicenseKeyError(#[from] LicenseKeyError),

    #[error(
        "CPU core limit exceeded as per the license key, \
        requesting {actual} while the maximum allowed is {limit}"
    )]
    Exceeded { limit: NonZeroU64, actual: u64 },
}

impl LicenseManager {
    pub fn check_cpu_core_limit(&self, cpu_core_count: u64) -> Result<(), CpuCoreLimitExceeded> {
        let license = self.license()?;

        match license.cpu_core_limit {
            Some(limit) if cpu_core_count > limit.get() => Err(CpuCoreLimitExceeded::Exceeded {
                limit,
                actual: cpu_core_count,
            }),
            _ => Ok(()),
        }
    }
}
