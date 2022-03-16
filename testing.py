import io
from unittest import TestCase

import pandas as pd
import requests


class PresetSchemaTest(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.auth = ("iterable", "cinnamondreams29")
        r = requests.post("http://127.0.0.1:5000/reset", auth=cls.auth)

    def test_auth(self):
        r = requests.get("http://127.0.0.1:5000/test_auth", auth=self.auth)
        self.assertEqual(r.status_code, 200)

    def test_get_schema(self):
        r = requests.get("http://127.0.0.1:5000/get_schema", auth=self.auth)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(
            r.json(),
            {
                "schema": {
                    "email": "string",
                    "firstName": "string",
                    "lastName": "string",
                    "signupDate": "timestamp",
                },
                "schema_alternatives": {
                    "email": [],
                    "firstName": [],
                    "lastName": [],
                    "signupDate": [],
                },
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
                "favorite_color": ["purple"],
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
            {"favorite_color", "signup_date", "bogus_data"},
        )
        r = requests.post(
            "http://127.0.0.1:5000/complete_upload",
            json={
                "favorite_color": {
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
            r.json()["schema"],
            {
                "email": "string",
                "favoriteColor": "string",
                "firstName": "string",
                "lastName": "string",
                "signupDate": "timestamp",
            },
        )

    def test_update_schema(self):
        r = requests.post("http://127.0.0.1:5000/reset", auth=self.auth)
        r = requests.post(
            "http://127.0.0.1:5000/update_schema",
            json={
                "email": {"action": "drop"},
                "widgetsOwned": {"action": "add", "dtype": "int"},
                "signupDate": {
                    "action": "alter",
                    "new_name": "signup_date",
                    "dtype": "string",
                },
            },
            auth=self.auth,
        )
        r = requests.get("http://127.0.0.1:5000/get_schema", auth=self.auth)
        print(r.json())
        self.assertEqual(
            r.json(),
            {
                "schema": {
                    "firstName": "string",
                    "lastName": "string",
                    "signup_date": "string",
                    "widgetsOwned": "int",
                },
                "schema_alternatives": {
                    "firstName": [],
                    "lastName": [],
                    "signupDate": [],
                    "widgetsOwned": [],
                },
            },
        )
        r = requests.get("http://127.0.0.1:5000/get_data", auth=self.auth)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns), ["firstName", "lastName", "signup_date", "widgetsOwned"]
        )
