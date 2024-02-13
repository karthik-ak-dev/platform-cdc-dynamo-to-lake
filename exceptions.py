class InvalidInputException(Exception):
    def __init__(self, msg: str):
        self.status_code = 422
        self.generic_msg = "Invalid input"
        self.msg = msg
