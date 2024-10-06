from elementary.monitor.alerts.grouped_alerts.grouped_alert import GroupedAlert


class AllInOneAlert(GroupedAlert):
    @property
    def summary(self) -> str:
        return f"{len(self.alerts)} issues detected"
