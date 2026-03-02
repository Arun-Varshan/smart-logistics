class WarehouseIoTClient:
    def __init__(self, broker_url=None):
        self.broker_url = broker_url

    def read_sensors(self):
        return {"sensors": []}

