import time
from locust import HttpUser, task, between

class AlbumsUser(HttpUser):
    wait_time = between(0.0, 0.0)

    @task(3)
    def get_albums(self):
        with self.client.get("/albums", name="GET /albums", catch_response=True) as r:
            if r.status_code != 200:
                r.failure(f"GET /albums failed: {r.status_code}")

    @task(1)
    def post_album(self):
        payload = {
            "id": str(time.time_ns()),
            "title": "Load Test Album",
            "artist": "Locust",
            "price": 10.00
        }
        with self.client.post("/albums", json=payload, name="POST /albums", catch_response=True) as r:
            # server returns 201 Created for POST /albums
            if r.status_code != 201:
                r.failure(f"POST /albums failed: {r.status_code}")
