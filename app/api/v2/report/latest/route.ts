import { NextRequest, NextResponse } from "next/server";

import { getLatestReportV2 } from "@/lib/server/data";

export async function GET(request: NextRequest) {
  const lang = request.nextUrl.searchParams.get("lang");

  if (lang && !["ko", "en"].includes(lang)) {
    return NextResponse.json(
      { error: "Invalid lang. Use ko or en." },
      { status: 400 }
    );
  }

  try {
    const report = await getLatestReportV2();
    if (!report) {
      return NextResponse.json({ error: "No report found" }, { status: 404 });
    }

    return NextResponse.json(report);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
