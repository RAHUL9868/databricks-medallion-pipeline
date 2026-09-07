#!/usr/bin/env python3
"""Generate the dashboard deliverable PDF (stdlib only)."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = REPO_ROOT / "src" / "dashboard" / "E-Commerce-Gold-Analytics-Dashboard.pdf"

DASHBOARD_URL = (
    "https://dbc-6a498a86-07f2.cloud.databricks.com/dashboardsv3/"
    "01f1aab7db5713f8a43e09512edcaa70/published?o=7474655649363572"
)

LINES = [
    "E-Commerce Gold Analytics Dashboard",
    "",
    "Project: E-commerce Medallion Pipeline",
    "Schema: ecommerce",
    "Deliverable date: 2026-09-07",
    "Status: Published",
    "",
    "Published dashboard URL:",
    DASHBOARD_URL,
    "",
    "Workspace: dbc-6a498a86-07f2.cloud.databricks.com",
    "Dashboard ID: 01f1aab7db5713f8a43e09512edcaa70",
    "",
    "Required visualizations:",
    "1. Top 10 Products by Revenue (Bar) - gold_sales_by_product",
    "2. Customer Revenue Distribution (Histogram) - gold_revenue_by_customer",
    "3. Customer Segmentation by Behavior (Pie) - gold_customer_segmentation",
    "",
    "Gold data rules:",
    "- Completed orders only for revenue (design D1)",
    "- Only dq_is_valid = true Silver rows feed Gold (design D2)",
    "- Dashboard reads Gold tables only",
    "",
    "Repository files:",
    "- src/dashboard/dashboard_queries.sql",
    "- src/dashboard/DASHBOARD_GUIDE.md",
    "- src/dashboard/DASHBOARD_DELIVERABLE.md",
    "- notebooks/databricks_dashboard_preview.ipynb",
    "",
    "Refresh: re-run the pipeline notebook, then refresh the published dashboard.",
]


def _escape_pdf_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf(lines: list[str]) -> bytes:
    y_start = 750
    line_height = 14
    content_lines = ["BT", "/F1 11 Tf"]
    y = y_start
    for line in lines:
        content_lines.append(f"1 0 0 1 50 {y} Tm ({_escape_pdf_text(line)}) Tj")
        y -= line_height
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects: list[bytes] = []
    objects.append(b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n")
    objects.append(b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n")
    objects.append(
        b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj\n"
    )
    objects.append(
        f"4 0 obj<< /Length {len(stream)} >>stream\n".encode("ascii")
        + stream
        + b"\nendstream\nendobj\n"
    )
    objects.append(b"5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n")

    header = b"%PDF-1.4\n"
    body = b""
    offsets = [0]
    for obj in objects:
        offsets.append(len(header) + len(body))
        body += obj

    xref_start = len(header) + len(body)
    xref = [b"xref\n", f"0 {len(offsets)}\n".encode("ascii"), b"0000000000 65535 f \n"]
    for offset in offsets[1:]:
        xref.append(f"{offset:010d} 00000 n \n".encode("ascii"))
    trailer = (
        b"trailer<< /Size "
        + str(len(offsets)).encode("ascii")
        + b" /Root 1 0 R >>\nstartxref\n"
        + str(xref_start).encode("ascii")
        + b"\n%%EOF\n"
    )
    return header + body + b"".join(xref) + trailer


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_bytes(build_pdf(LINES))
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
