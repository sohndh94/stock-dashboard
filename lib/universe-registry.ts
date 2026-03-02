import universe from "@/config/universe.json";

export interface RegistryGroup {
  group_key: string;
  name: string;
  purpose?: string;
  is_active: boolean;
}

export interface RegistryInstrument {
  symbol: string;
  name: string;
  name_ko?: string;
  category?: string;
  asset_type: string;
  market: string;
  currency: string;
  provider: string;
  provider_symbol: string;
  display_order: number;
  is_compare_default: boolean;
  is_active: boolean;
  groups: string[];
}

interface RegistryPayload {
  version: number;
  as_of: string;
  groups: RegistryGroup[];
  instruments: RegistryInstrument[];
}

const payload = universe as RegistryPayload;

export function getRegistryPayload(): RegistryPayload {
  return payload;
}

export function getRegistryCompareInstruments(): RegistryInstrument[] {
  return payload.instruments
    .filter((item) => item.is_active && item.groups.includes("compare"))
    .sort((a, b) => a.display_order - b.display_order);
}

export function getRegistryDefaultCompareSymbols(): string[] {
  const defaults = getRegistryCompareInstruments()
    .filter((item) => item.is_compare_default)
    .map((item) => item.symbol);

  if (defaults.length > 0) {
    return defaults;
  }

  return getRegistryCompareInstruments().slice(0, 6).map((item) => item.symbol);
}
