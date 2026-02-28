import type { Metadata } from "next";
import { Noto_Sans_KR, Space_Grotesk } from "next/font/google";

import "./globals.css";

const headingFont = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-heading",
  weight: ["500", "600", "700"]
});

const bodyFont = Noto_Sans_KR({
  subsets: ["latin"],
  variable: "--font-body",
  weight: ["400", "500", "700"]
});

export const metadata: Metadata = {
  title: "Daily Bio Market Dashboard",
  description: "KOSPI + Bio industry daily report dashboard"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko" className={`${headingFont.variable} ${bodyFont.variable}`}>
      <body style={{ fontFamily: "var(--font-body)" }}>{children}</body>
    </html>
  );
}
