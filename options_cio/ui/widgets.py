"""
Reusable Textual widgets for the Options CIO dashboard.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import DataTable, Label, Static
from textual.reactive import reactive
from rich.text import Text
from rich.panel import Panel


class PortfolioStatusBar(Static):
    """Shows deployment % and status for a single portfolio."""

    def __init__(self, portfolio_id: str, state: dict, config: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.portfolio_id = portfolio_id
        self.state = state
        self.config = config

    def render(self) -> Panel:
        dep = self.state.get("deployment_pct", 0)
        band = self.config.get("deployment_band", [0, 1])
        target = self.config.get("target_zone", band)
        name = self.config.get("name", self.portfolio_id)

        if dep < band[0]:
            color = "yellow"
            status = "UNDER"
        elif dep > band[1]:
            color = "red"
            status = "OVER"
        elif target[0] <= dep <= target[1]:
            color = "green"
            status = "TARGET"
        else:
            color = "cyan"
            status = "IN BAND"

        bar_len = 20
        filled = int(dep * bar_len)
        bar = f"[{color}]{'█' * filled}{'░' * (bar_len - filled)}[/{color}]"
        text = (
            f"{self.portfolio_id} | {name[:20]}\n"
            f"{bar} {dep:.1%} [{status}]\n"
            f"Band: {band[0]:.0%}–{band[1]:.0%}  "
            f"Target: {target[0]:.0%}–{target[1]:.0%}  "
            f"${self.state.get('deployed_capital', 0):,.0f} deployed"
        )
        return Panel(text, title=f"[bold]{self.portfolio_id}[/bold]", expand=True)


class GreeksTable(Widget):
    """Displays Greeks summary across all portfolios."""

    def __init__(self, greeks_summaries: list[dict], **kwargs) -> None:
        super().__init__(**kwargs)
        self.greeks_summaries = greeks_summaries

    def compose(self) -> ComposeResult:
        table = DataTable()
        table.add_columns("Portfolio", "Delta", "Gamma", "Theta", "Vega", "Positions")
        for g in self.greeks_summaries:
            delta_color = "red" if abs(g.get("delta", 0)) > 50 else "white"
            table.add_row(
                g.get("portfolio", ""),
                Text(f"{g.get('delta', 0):+.1f}", style=delta_color),
                f"{g.get('gamma', 0):+.4f}",
                f"{g.get('theta', 0):+.1f}",
                f"{g.get('vega', 0):+.1f}",
                str(g.get("position_count", 0)),
            )
        yield table


class AlertsPanel(Static):
    """Renders active rule alerts with severity coloring."""

    SEVERITY_COLORS = {"CRITICAL": "red", "WARN": "yellow", "INFO": "cyan"}

    def __init__(self, alerts: list[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self.alerts = alerts

    def render(self) -> Panel:
        if not self.alerts:
            content = "[green]No active alerts[/green]"
        else:
            lines = []
            for alert in self.alerts:
                color = "white"
                for sev, col in self.SEVERITY_COLORS.items():
                    if sev in alert:
                        color = col
                        break
                lines.append(f"[{color}]{alert}[/{color}]")
            content = "\n".join(lines)
        return Panel(content, title="[bold red]Active Alerts[/bold red]", expand=True)


class AIReviewPanel(Static):
    """Displays the latest AI CIO review output."""

    text: reactive[str] = reactive("Loading AI review...", layout=True)

    def render(self) -> Panel:
        return Panel(
            self.text,
            title="[bold cyan]CIO AI Analysis[/bold cyan]",
            expand=True,
        )
