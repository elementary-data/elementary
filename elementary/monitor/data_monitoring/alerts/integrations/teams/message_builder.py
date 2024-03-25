from pymsteams import cardsection, potentialaction  # type: ignore

from elementary.clients.teams.client import TeamsClient


class TeamsAlertMessageBuilder:
    def __init__(self, client: TeamsClient) -> None:
        self.client = client

    def title(self, title: str):
        self.client.title(title)

    def text(self, text: str):
        self.client.text(text)

    def addSection(self, section: cardsection):
        self.client.addSection(section)

    def addPotentialAction(self, action: potentialaction):
        self.client.addPotentialAction(action)
