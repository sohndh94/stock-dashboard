import { SectionKey } from "@/lib/types";
import { getRegistryDefaultCompareSymbols } from "@/lib/universe-registry";

export const SECTION_ORDER: SectionKey[] = ["kospi", "bio", "samsung_bio"];

export const DEFAULT_CHART_FROM = "2025-07-01";

export const DEFAULT_COMPARE_SYMBOLS = getRegistryDefaultCompareSymbols();

export const DISCLAIMER_LINES = [
  "본 서비스는 공개 무료 데이터 소스를 기반으로 하며 투자 자문이 아닙니다.",
  "데이터 지연/정정 가능성이 있습니다. 원거래소 공시를 우선 확인하세요."
];
