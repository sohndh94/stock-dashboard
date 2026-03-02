import { NextRequest, NextResponse } from "next/server";

import { getCompanySnapshot } from "@/lib/server/data";

export async function GET(
  request: NextRequest,
  { params }: { params: { symbol: string } }
) {
  const date = request.nextUrl.searchParams.get("date") ?? undefined;
  const symbol = decodeURIComponent(params.symbol);

  try {
    const snapshot = await getCompanySnapshot(symbol, date);
    if (!snapshot) {
      return NextResponse.json({ error: "Company not found" }, { status: 404 });
    }
    return NextResponse.json(snapshot);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
