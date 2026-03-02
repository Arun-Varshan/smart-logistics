class RealRobotAPIClient:
    def __init__(self, base_url=None, api_key=None):
        self.base_url = base_url
        self.api_key = api_key

    def send_robot_command(self, robot_id, payload):
        return {"robot_id": robot_id, "accepted": True, "payload": payload}

