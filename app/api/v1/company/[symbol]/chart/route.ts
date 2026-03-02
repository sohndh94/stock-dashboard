import { NextRequest, NextResponse } from "next/server";

import { getCompanyDetailChart } from "@/lib/server/data";

export async function GET(
  request: NextRequest,
  { params }: { params: { symbol: string } }
) {
  const symbol = decodeURIComponent(params.symbol);
  const from = request.nextUrl.searchParams.get("from") ?? undefined;
  const to = request.nextUrl.searchParams.get("to") ?? undefined;
  const modeRaw = (request.nextUrl.searchParams.get("mode") ?? "PRICE").toUpperCase();

  if (!["PRICE", "BASE100"].includes(modeRaw)) {
    return NextResponse.json(
      { error: "Invalid mode. Use PRICE or BASE100." },
      { status: 400 }
    );
  }

  try {
    const payload = await getCompanyDetailChart(
      symbol,
      from,
      to,
      modeRaw as "PRICE" | "BASE100"
    );
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
