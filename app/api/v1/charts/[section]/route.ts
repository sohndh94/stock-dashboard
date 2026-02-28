import { NextRequest, NextResponse } from "next/server";

import { getChartBySection } from "@/lib/server/data";
import { SectionKey } from "@/lib/types";

const ALLOWED_SECTIONS: SectionKey[] = ["kospi", "bio", "samsung_bio"];

export async function GET(
  request: NextRequest,
  { params }: { params: { section: string } }
) {
  const section = params.section as SectionKey;

  if (!ALLOWED_SECTIONS.includes(section)) {
    return NextResponse.json(
      { error: "Invalid section. Use kospi, bio, or samsung_bio." },
      { status: 400 }
    );
  }

  const from = request.nextUrl.searchParams.get("from") ?? undefined;
  const to = request.nextUrl.searchParams.get("to") ?? undefined;

  try {
    const chart = await getChartBySection(section, from, to);
    return NextResponse.json(chart);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
