
import time, requests

class Poller:
    def __init__(self, attempts: int, interval_s: int, per_attempt_timeout_s: int):
        self.attempts = attempts
        self.interval_s = interval_s
        self.per_attempt_timeout_s = per_attempt_timeout_s

    def poll(self, fetch_fn):
        for _ in range(self.attempts):
            try:
                result = fetch_fn()
                if result:
                    return result
            except requests.Timeout:
                pass
            time.sleep(self.interval_s)
        raise TimeoutError(f"Polling exceeded {self.attempts} attempts")
