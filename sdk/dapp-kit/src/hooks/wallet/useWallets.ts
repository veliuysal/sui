// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { getWallets } from '@mysten/wallet-standard';

import { useWalletStore } from './useWalletStore.js';

/**
 * Retrieves a list of registered wallets available to the dApp sorted by preference.
 */
export function useWallets() {
	return useWalletStore(getWallets);
}
