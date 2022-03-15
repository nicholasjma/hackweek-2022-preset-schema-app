import io
from unittest import TestCase

import pandas as pd
import requests


class PresetSchemaTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.auth = ("iterable", "cinnamondreams29")

    def test_auth(self):
        r = requests.get("http://127.0.0.1:5000/test_auth", auth=self.auth)
        self.assertEquals(r.status_code, 200)

    def test_get_schema(self):
        r = requests.get("http://127.0.0.1:5000/get_schema", auth=self.auth)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(
            r.json(),
            {
                "email": "string",
                "firstName": "string",
                "lastName": "string",
                "signupDate": "timestamp",
            },
        )

    def test_get_data(self):
        r = requests.get("http://127.0.0.1:5000/get_data", auth=self.auth)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns), ["email", "firstName", "lastName", "signupDate"]
        )

    def test_reset(self):
        r = requests.post("http://127.0.0.1:5000/reset", auth=self.auth)
        self.assertEqual(r.status_code, 200)

    def test_upload_process(self):
        r = requests.post("http://127.0.0.1:5000/reset", auth=self.auth)
        df = pd.DataFrame(
            {
                "email": ["bob@acme.com"],
                "firstName": ["Bob"],
                "lastName": ["Jones"],
                "signup_date": ["2020-12-05"],
                "favoriteColor": ["purple"],
                "bogus_data": [325822],
            }
        )
        r = requests.post(
            "http://127.0.0.1:5000/upload_csv",
            files={"file": io.StringIO(df.to_csv(index=False))},
            auth=self.auth,
        )
        response = r.json()
        self.assertEqual(
            set(response["suggestions"].keys()),
            {"favoriteColor", "signup_date", "bogus_data"},
        )
        r = requests.post(
            "http://127.0.0.1:5000/complete_upload",
            json={
                "favoriteColor": {
                    "action": "add",
                    "new_name": "favoriteColor",
                    "dtype": "string",
                },
                "signup_date": {"action": "map", "map_to_name": "signupDate"},
                "bogus_data": {"action": "drop"},
            },
            auth=self.auth,
        )
        print(r.text)
        r = requests.get("http://127.0.0.1:5000/get_data", auth=self.auth)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns),
            ["email", "firstName", "lastName", "signupDate", "favoriteColor"],
        )
        self.assertEqual(
            df.iloc[-1].tolist(),
            ["bob@acme.com", "Bob", "Jones", "2020-12-05", "purple"],
        )
        r = requests.get("http://127.0.0.1:5000/get_schema", auth=self.auth)
        self.assertEqual(
            r.json(),
            {
                "email": "string",
                "favoriteColor": "string",
                "firstName": "string",
                "lastName": "string",
                "signupDate": "timestamp",
            },
        )
