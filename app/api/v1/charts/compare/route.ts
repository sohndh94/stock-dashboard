import { NextRequest, NextResponse } from "next/server";

import { getCompareChart } from "@/lib/server/data";

function parseSymbols(raw: string | null): string[] | undefined {
  if (!raw) {
    return undefined;
  }

  const parsed = raw
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  return parsed.length > 0 ? parsed : undefined;
}

export async function GET(request: NextRequest) {
  const symbols = parseSymbols(request.nextUrl.searchParams.get("symbols"));
  const from = request.nextUrl.searchParams.get("from") ?? undefined;
  const to = request.nextUrl.searchParams.get("to") ?? undefined;

  try {
    const chart = await getCompareChart(symbols, from, to);
    return NextResponse.json(chart);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
