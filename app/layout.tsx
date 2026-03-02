import type { Metadata } from "next";

import "./globals.css";

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
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
