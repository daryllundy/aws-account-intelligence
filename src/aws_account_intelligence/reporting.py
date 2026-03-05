from __future__ import annotations

import csv
import json
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from aws_account_intelligence.analysis.impact import ImpactAnalyzer
from aws_account_intelligence.models import GraphExportResponse, ScanDeltaReport
from aws_account_intelligence.pipeline import ScanPipeline
from aws_account_intelligence.storage import Database


class ReportExporter:
    def __init__(self, database: Database, pipeline: ScanPipeline, output_dir: Path):
        self.database = database
        self.pipeline = pipeline
        self.output_dir = output_dir

    def export(self, scan_run_id: str, format_name: str, destination: Path | None = None) -> Path:
        payload = self._report_payload(scan_run_id)
        format_normalized = format_name.lower()
        target = destination or self._default_path(scan_run_id, format_normalized)
        target.parent.mkdir(parents=True, exist_ok=True)

        if format_normalized == "json":
            target.write_text(json.dumps(payload, indent=2, default=str))
        elif format_normalized == "csv":
            self._write_csv(target, payload)
        elif format_normalized == "pdf":
            self._write_pdf(target, payload)
        elif format_normalized == "slack":
            target.write_text(self._slack_digest(payload))
        elif format_normalized == "email":
            target.write_text(self._email_digest(payload))
        else:
            raise ValueError(f"Unsupported report format: {format_name}")
        return target

    def _report_payload(self, scan_run_id: str) -> dict:
        scan = self.database.get_scan_run(scan_run_id)
        if scan is None:
            raise ValueError(f"Unknown scan run: {scan_run_id}")
        inventory = self.pipeline.inventory(scan_run_id)
        costs = self.pipeline.costs(scan_run_id)
        edges = self.database.list_dependency_edges(scan_run_id)
        graph = GraphExportResponse(scan=scan, adjacency=self._adjacency(edges)).model_dump(mode="json")
        delta = self.pipeline.delta(scan_run_id).model_dump(mode="json")
        high_impact = self._top_impact_reports(scan_run_id)
        return {
            "scan": scan.model_dump(mode="json"),
            "inventory": inventory.model_dump(mode="json"),
            "costs": costs.model_dump(mode="json"),
            "graph": graph,
            "delta": delta,
            "top_impact_reports": high_impact,
        }

    def _top_impact_reports(self, scan_run_id: str) -> list[dict]:
        services = self.database.list_service_records(scan_run_id)
        costs = {item.resource_id: item.projected_monthly_cost_usd for item in self.database.list_cost_attributions(scan_run_id)}
        edges = self.database.list_dependency_edges(scan_run_id)
        analyzer = ImpactAnalyzer()
        scored = [
            analyzer.analyze(scan_run_id, service.resource_id, services, costs, edges)
            for service in services[: min(len(services), 20)]
        ]
        scored.sort(
            key=lambda item: (
                item.risk_score.value,
                item.estimated_monthly_savings_usd,
                len(item.direct_dependents),
                len(item.transitive_dependents),
            ),
            reverse=True,
        )
        return [report.model_dump(mode="json") for report in scored[:5]]

    def _adjacency(self, edges) -> dict[str, list]:
        adjacency: dict[str, list] = {}
        for edge in edges:
            adjacency.setdefault(edge.from_resource_id, []).append(edge.model_dump(mode="json"))
        return adjacency

    def _default_path(self, scan_run_id: str, format_name: str) -> Path:
        extensions = {
            "json": "json",
            "csv": "csv",
            "pdf": "pdf",
            "slack": "txt",
            "email": "txt",
        }
        stem = f"scan-report-{scan_run_id[:8]}-{format_name}"
        return self.output_dir / "reports" / f"{stem}.{extensions[format_name]}"

    def _write_csv(self, target: Path, payload: dict) -> None:
        services = {item["resource_id"]: item for item in payload["inventory"]["services"]}
        costs = {item["resource_id"]: item for item in payload["costs"]["costs"]}
        rows = []
        for resource_id, service in services.items():
            cost = costs.get(resource_id, {})
            rows.append(
                {
                    "resource_id": resource_id,
                    "service_name": service["service_name"],
                    "region": service["region"],
                    "status": service["status"],
                    "projected_monthly_cost_usd": cost.get("projected_monthly_cost_usd", 0.0),
                    "mtd_cost_usd": cost.get("mtd_cost_usd", 0.0),
                    "risk_score": self._impact_score_for(resource_id, payload["top_impact_reports"]),
                }
            )
        with target.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["resource_id"])
            writer.writeheader()
            writer.writerows(rows or [{"resource_id": ""}])

    def _write_pdf(self, target: Path, payload: dict) -> None:
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(name="BodySmall", parent=styles["BodyText"], fontSize=9, leading=12))
        doc = SimpleDocTemplate(str(target), pagesize=letter, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
        story = [
            Paragraph("AWS Account Intelligence Report", styles["Title"]),
            Spacer(1, 8),
            Paragraph(f"Scan ID: {payload['scan']['scan_run_id']}", styles["BodySmall"]),
            Paragraph(
                f"Resources: {payload['scan']['resource_count']} | Edges: {payload['scan']['edge_count']} | Warnings: {payload['scan']['summary']['warning_count']}",
                styles["BodySmall"],
            ),
            Spacer(1, 16),
            Paragraph("Cost Summary", styles["Heading2"]),
            Paragraph(
                f"Projected monthly cost: ${payload['costs']['total_projected_monthly_cost_usd']:.2f} | Unattributed: ${payload['costs']['unattributed_cost_usd']:.2f}",
                styles["BodyText"],
            ),
            Spacer(1, 16),
            Paragraph("Delta Summary", styles["Heading2"]),
            Paragraph(self._delta_summary(payload["delta"]), styles["BodyText"]),
            Spacer(1, 16),
            Paragraph("Top Shutdown Risks", styles["Heading2"]),
        ]
        top_rows = [["Resource", "Risk", "Savings", "Dependents"]]
        for report in payload["top_impact_reports"]:
            top_rows.append(
                [
                    _compact_id(report["target_resource_id"]),
                    report["risk_score"],
                    f"${report['estimated_monthly_savings_usd']:.2f}",
                    str(len(report["direct_dependents"]) + len(report["transitive_dependents"])),
                ]
            )
        table = Table(top_rows, repeatRows=1, colWidths=[240, 70, 80, 90])
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d7c66")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d7c7b3")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5efe4")]),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("LEADING", (0, 0), (-1, -1), 11),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(table)
        doc.build(story)

    def _slack_digest(self, payload: dict) -> str:
        lines = [
            f"*AWS Account Intelligence*: `{payload['scan']['scan_run_id'][:8]}`",
            f"- Resources: {payload['scan']['resource_count']}",
            f"- Projected monthly cost: ${payload['costs']['total_projected_monthly_cost_usd']:.2f}",
            f"- Warnings: {payload['scan']['summary']['warning_count']}",
            f"- Delta: {self._delta_summary(payload['delta'])}",
            "- Top shutdown risks:",
        ]
        for report in payload["top_impact_reports"]:
            lines.append(
                f"  - `{_compact_id(report['target_resource_id'])}` | {report['risk_score']} | ${report['estimated_monthly_savings_usd']:.2f}"
            )
        return "\n".join(lines) + "\n"

    def _email_digest(self, payload: dict) -> str:
        lines = [
            "Subject: AWS Account Intelligence Snapshot",
            "",
            f"Scan ID: {payload['scan']['scan_run_id']}",
            f"Completed at: {payload['scan']['completed_at']}",
            f"Resource count: {payload['scan']['resource_count']}",
            f"Projected monthly cost: ${payload['costs']['total_projected_monthly_cost_usd']:.2f}",
            f"Unattributed cost: ${payload['costs']['unattributed_cost_usd']:.2f}",
            f"Warning count: {payload['scan']['summary']['warning_count']}",
            f"Delta summary: {self._delta_summary(payload['delta'])}",
            "",
            "Top shutdown risks:",
        ]
        for report in payload["top_impact_reports"]:
            lines.extend(
                [
                    f"- Resource: {report['target_resource_id']}",
                    f"  Risk: {report['risk_score']}",
                    f"  Savings: ${report['estimated_monthly_savings_usd']:.2f}",
                    f"  Rationale: {report['rationale']}",
                ]
            )
        return "\n".join(lines) + "\n"

    def _delta_summary(self, delta_payload: dict) -> str:
        return (
            f"{len(delta_payload['added_resources'])} added, "
            f"{len(delta_payload['removed_resources'])} removed, "
            f"{len(delta_payload['cost_changes'])} cost changes"
        )

    def _impact_score_for(self, resource_id: str, reports: list[dict]) -> str:
        for report in reports:
            if report["target_resource_id"] == resource_id:
                return report["risk_score"]
        return "LOW"


def _compact_id(resource_id: str) -> str:
    if len(resource_id) <= 48:
        return resource_id
    return f"{resource_id[:22]}...{resource_id[-18:]}"
