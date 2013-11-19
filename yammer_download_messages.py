"""
http://developer.yammer.com/introduction/#gs-authentication
"""
import datetime
import redis
import httplib2
import json
import time
from config import Config


class YammerUpdate:
    def __init__(self):
        self.config = Config()
        self.token = self.config.get("access-token")
        if self.token is None:
            raise AttributeError("No access token specified")
        self.headers = {"Authorization": "Bearer %s" % self.token}
        self.post_queue = []
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.redis = redis.Redis(host=self.config.get("redis-hostname"), port=self.config.get("redis-port"), db=self.config.get("redis-db"))
        self.people = None

    def get_people(self):
        p_k = "yammer-tmp-people2"
        if self.people:
            return self.people
        if self.redis.exists(p_k):
            data = self.redis.get(p_k)
            loaded = json.loads(self.redis.get(p_k))
            if isinstance(loaded, dict):
                self.people = loaded
                return loaded
        userdata = {}
        for page in range(1, 20):
            (_, cont) = self.h.request("https://www.yammer.com/api/v1/users.json?page=%s" % page, headers=self.headers)
            users = json.loads(cont)
            if len(users) == 0:
                break
            for user in users:
                address = None
                for email in user.get("contact", {}).get("email_addresses", []):
                    if email.get("type", "") == "primary":
                        address = email["address"]
                if address is None:
                    continue
                userdata[user["id"]] = address 
            time.sleep(2)
        self.redis.setex(p_k, json.dumps(userdata), 86400) # one day
        self.people = userdata
        return userdata

    def get_messages(self, newer_than=None):
        p_k = "yammer-tmp-messages-newer_than-%s" % newer_than
        if self.redis.exists(p_k):
            return json.loads(self.redis.get(p_k))
        url = "https://www.yammer.com/api/v1/messages.json?"
        if newer_than:
            url += "&newer_than=%s" % newer_than
        (_, content) = self.h.request(url, "GET", headers=self.headers)
        messages = json.loads(content)
        self.redis.setex(p_k, json.dumps(messages), 30)
        return messages

    def load_newest(self):
        return self.redis.get("yammer-newest-id")

    def save_newest(self, newest):
        self.redis.set("yammer-newest-id", newest)

    def run(self):
        if self.people is None:
            self.get_people()
        newest_id = self.process(self.load_newest())
        while True:
            newest_id = self.process(newest_id)
            time.sleep(2)
            if newest_id is None:
                return

    def process(self, newer_than = None):
        if self.people is None:
            self.get_people()
        messages = self.get_messages(newer_than)
        if newer_than is not None:
            largest = newer_than
        else:
            largest = 0
        for message in messages.get("messages", []):
            username = self.people.get(str(message.get("sender_id")))
            if username is None:
                continue
            created_at = message.get("created_at").split(" ")
            created_at = (created_at[0] + " " + created_at[1]).replace("/", "-")
            ts = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
            data = {"system": "yammer_message", "username": username, "timestamp": str(ts), "data": message.get("id"), "is_utc": True}
            id_tmp = int(message.get("id", 0))
            if id_tmp > largest:
                self.save_newest(id_tmp)
                largest = id_tmp
            self.post(data)
        self.post()
        if largest == newer_than:
            return None
        return largest

    def post(self, data = None):
        if data:
            try:
                json.dumps(data)
            except:
                return
            self.post_queue.append(data)
        if len(self.post_queue) > 250 or (data is None and len(self.post_queue) > 0):
            self.h.request(self.config.get("server-url"), "POST", body=json.dumps(self.post_queue))
            self.post_queue = []

def main():
    yammer = YammerUpdate()
    yammer.run()

if __name__ == '__main__':
    main()
