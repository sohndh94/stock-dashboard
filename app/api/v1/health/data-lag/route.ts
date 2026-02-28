import { NextResponse } from "next/server";

import { getDataLag } from "@/lib/server/data";

export async function GET() {
  try {
    const payload = await getDataLag();
    return NextResponse.json(payload);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
