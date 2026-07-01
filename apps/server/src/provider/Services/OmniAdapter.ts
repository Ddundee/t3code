/**
 * OmniAdapter — shape type for the Omni (Omnigent) provider adapter.
 *
 * @module OmniAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

export interface OmniAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
