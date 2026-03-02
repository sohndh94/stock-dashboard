import { NextRequest, NextResponse } from "next/server";

import { getReportEvidence } from "@/lib/server/data";
import { SectionKey } from "@/lib/types";

const ALLOWED_SECTIONS: SectionKey[] = ["kospi", "bio", "samsung_bio"];

export async function GET(request: NextRequest) {
  const date = request.nextUrl.searchParams.get("date");
  const section = request.nextUrl.searchParams.get("section") as SectionKey | null;

  if (!date) {
    return NextResponse.json({ error: "date query is required" }, { status: 400 });
  }

  if (!section || !ALLOWED_SECTIONS.includes(section)) {
    return NextResponse.json(
      { error: "Invalid section. Use kospi, bio, or samsung_bio." },
      { status: 400 }
    );
  }

  try {
    const payload = await getReportEvidence(date, section);
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
