// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::stream::FuturesUnordered;
use sui_types::committee::EpochId;
use sui_types::signature::GenericSignature;
use sui_types::zk_login_authenticator::ZkLoginAuthenticator;
use std::collections::BTreeMap;
use std::sync::Arc;
use sui_types::base_types::{ObjectRef, SuiAddress};
use sui_types::crypto::{get_account_key_pair, Ed25519SuiSignature, Signature, Signer};
use sui_types::object::Object;

use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PrivateKey, Ed25519PublicKey, Ed25519Signature};
use fastcrypto_zkp::bn254::zk_login::ZkLoginInputs;

enum MockKeyPair {
    Ed25519(Ed25519KeyPair),
    ZkLogin(ZkLoginEphKeyPair),
}

impl Signer<GenericSignature> for MockKeyPair {
    fn sign(&self, message: &[u8]) -> GenericSignature {
        match self {
            MockKeyPair::Ed25519(keypair) => 
                GenericSignature::from(
                    <Ed25519KeyPair as sui_types::crypto::Signer<Signature>>::sign(keypair, message)
                ),
            // MockKeyPair::Ed25519(keypair) => 
                // GenericSignature::from(<keypair as .sign(message)),
            MockKeyPair::ZkLogin(keypair) => 
                GenericSignature::from(keypair.sign(message)),
        }
    }
}

#[derive(Clone)]
pub struct Account {
    pub sender: SuiAddress,
    pub keypair: Arc<MockKeyPair>,
    pub gas_objects: Arc<Vec<ObjectRef>>,
}

/// Generate \num_accounts accounts and for each account generate \gas_object_num_per_account gas objects.
/// Return all accounts along with a flattened list of all gas objects as genesis objects.
pub async fn batch_create_account_and_gas(
    num_accounts: u64,
    gas_object_num_per_account: u64,
) -> (BTreeMap<SuiAddress, Account>, Vec<Object>) {
    let tasks: FuturesUnordered<_> = (0..num_accounts)
        .map(|_| {
            tokio::spawn(async move {
                let (sender, keypair) = get_account_key_pair();
                let objects = (0..gas_object_num_per_account)
                    .map(|_| Object::with_owner_for_testing(sender))
                    .collect::<Vec<_>>();
                (sender, keypair, objects)
            })
        })
        .collect();
    let mut accounts = BTreeMap::new();
    let mut genesis_gas_objects = vec![];
    for task in tasks {
        let (sender, keypair, gas_objects) = task.await.unwrap();
        let gas_object_refs: Vec<_> = gas_objects
            .iter()
            .map(|o| o.compute_object_reference())
            .collect();
        accounts.insert(
            sender,
            Account {
                sender,
                keypair: Arc::new(MockKeyPair::Ed25519(keypair)),
                gas_objects: Arc::new(gas_object_refs),
            },
        );
        genesis_gas_objects.extend(gas_objects);
    }
    (accounts, genesis_gas_objects)
}

#[derive(Debug)]
pub struct ZkLoginEphKeyPair {
    private: Ed25519KeyPair,
    public: ZkLoginAuxInputs
}

#[derive(Debug)]
pub struct ZkLoginAuxInputs { // ZK proof and all(?) public inputs
    zkp_details: ZkLoginInputs,
    eph_pk: Ed25519PublicKey,
    max_epoch: EpochId,
}

pub type ZkLoginSignature = ZkLoginAuthenticator;

impl Signer<ZkLoginSignature> for ZkLoginEphKeyPair {
    fn sign(&self, message: &[u8]) -> ZkLoginSignature {
        let sig = self.private.sign(message);
        ZkLoginSignature::new(
            self.public.zkp_details.clone(),
            self.public.max_epoch,
            sig,
        )
    }
}