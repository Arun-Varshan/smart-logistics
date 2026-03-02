class ScadaIngestionClient:
    def __init__(self, endpoint=None):
        self.endpoint = endpoint

    def fetch_snapshot(self):
        return {"status": "ok", "data": {}}

