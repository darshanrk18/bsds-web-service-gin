import time
from locust import task, between
from locust.contrib.fasthttp import FastHttpUser

class AlbumsUser(FastHttpUser):
    wait_time = between(0.0, 0.0)

    @task(3)
    def get_albums(self):
        self.client.get("/albums", name="GET /albums")

    @task(1)
    def post_album(self):
        payload = {
            "id": str(time.time_ns()),
            "title": "Load Test Album",
            "artist": "Locust",
            "price": 10.00
        }
        self.client.post("/albums", json=payload, name="POST /albums")