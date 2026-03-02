import { NextRequest, NextResponse } from "next/server";

import { getCompareUniverse } from "@/lib/server/data";

export async function GET(request: NextRequest) {
  const group = request.nextUrl.searchParams.get("group");
  const active = request.nextUrl.searchParams.get("active");

  if (group && group !== "compare") {
    return NextResponse.json(
      { error: "Invalid group. Only compare is supported." },
      { status: 400 }
    );
  }

  if (active && !["true", "false"].includes(active)) {
    return NextResponse.json(
      { error: "Invalid active flag. Use true or false." },
      { status: 400 }
    );
  }

  try {
    const items = await getCompareUniverse();
    const filtered =
      active === "false" ? items : items.filter((item) => item.is_active);
    return NextResponse.json({ items: filtered });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
