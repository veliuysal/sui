// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    base_types::{ObjectID, VersionNumber},
    committee::EpochId,
    storage::error::Result,
};
use std::sync::Arc;

pub trait ConfigStore {
    fn get_current_epoch_stable_sequence_number(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> Result<Option<VersionNumber>, crate::storage::error::Error>;
}

impl<T: ConfigStore + ?Sized> ConfigStore for &T {
    fn get_current_epoch_stable_sequence_number(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> Result<Option<VersionNumber>, crate::storage::error::Error> {
        (*self).get_current_epoch_stable_sequence_number(object_id, epoch_id)
    }
}

impl<T: ConfigStore + ?Sized> ConfigStore for Box<T> {
    fn get_current_epoch_stable_sequence_number(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> Result<Option<VersionNumber>, crate::storage::error::Error> {
        (**self).get_current_epoch_stable_sequence_number(object_id, epoch_id)
    }
}

impl<T: ConfigStore + ?Sized> ConfigStore for Arc<T> {
    fn get_current_epoch_stable_sequence_number(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> Result<Option<VersionNumber>, crate::storage::error::Error> {
        (**self).get_current_epoch_stable_sequence_number(object_id, epoch_id)
    }
}
