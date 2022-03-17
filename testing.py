import io
from unittest import TestCase

import pandas as pd
import requests


class PresetSchemaTest(TestCase):
    @classmethod
    def endpoint(cls, endpoint: str):
        return f"{cls.url}{endpoint}"

    @classmethod
    def setUpClass(cls):
        cls.auth = ("iterable", "cinnamondreams29")
        # cls.url = "http://127.0.0.1:5000/"
        cls.url = "https://hackweek-2022-schema-preset.herokuapp.com/"

    def test_auth(self):
        r = requests.get(self.endpoint("test_auth"), auth=self.auth)
        self.assertEqual(r.status_code, 200)


    def test_get_schema(self):
        r = requests.post(self.endpoint("reset"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        r = requests.get(self.endpoint("get_schema"), auth=self.auth)
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
        r = requests.post(self.endpoint("reset"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        r = requests.get(self.endpoint("get_data"), auth=self.auth)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns), ["email", "firstName", "lastName", "signupDate"]
        )

    def test_get_data_json(self):
        r = requests.post(self.endpoint("reset"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        r = requests.get(self.endpoint("get_data_json"), auth=self.auth)
        self.assertEqual(
            # fmt: off
            r.json(),
            {
                "columns": ["email", "firstName", "lastName", "signupDate"],
                "data": [  #
                    ["nick.ma@iterable.com", "Nick", "Ma", 1643673600000],
                    ["brett.eckrich@iterable.com", "Brett", "Eckrich", 1643760000000],
                    ["chris@iterable.com", "Chris", "Wheeler", 1643846400000],
                    ["keegan@iterable.com", "Keegan", "Hinson", 1643932800000],
                    ["kyle.moulder@iterable.com", "Kyle", "Moulder", 1644019200000],
                    ["michelle.chuang@iterable.com", "Michelle", "Chuang", 1644105600000],
                    ["mona.bazzaz@iterable.com", "Mona", "Bazzaz", 1644192000000],
                    ["steven.milov@iterable.com", "Steven", "Milov", 1644278400000],
                    ["tracy.schaffer@iterable.com", "Tracy", "Schaffer", 1644364800000],
                ],
            },
            # fmt: on
        )

    def test_reset(self):
        r = requests.post(self.endpoint("reset"), auth=self.auth)
        self.assertEqual(r.status_code, 200)

    def test_upload_process(self):
        r = requests.post(self.endpoint("reset"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        r = requests.get(self.endpoint("get_data"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns),
            ["email", "firstName", "lastName", "signupDate"],
        )
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
            self.endpoint("upload_csv"),
            files={"file": io.StringIO(df.to_csv(index=False))},
            auth=self.auth,
        )
        self.assertEqual(r.status_code, 200)
        response = r.json()
        self.assertEqual(
            response,
            {
                "suggestions": {
                    "bogus_data": {},
                    "favorite_color": {},
                    "signup_date": {"signupDate": 100},
                }
            },
        )
        r = requests.post(
            self.endpoint("complete_upload"),
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
        self.assertEqual(r.status_code, 200)
        r = requests.get(self.endpoint("get_data"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns),
            ["email", "firstName", "lastName", "signupDate", "favoriteColor"],
        )
        self.assertEqual(
            df.iloc[-1].tolist(),
            ["bob@acme.com", "Bob", "Jones", "2020-12-05", "purple"],
        )
        r = requests.get(self.endpoint("get_schema"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
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
        r = requests.post(self.endpoint("reset"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        r = requests.post(
            self.endpoint("update_schema"),
            json={
                "email": {"action": "drop"},
                "widgetsOwned": {
                    "action": "add",
                    "dtype": "int",
                    "alternatives": ["widgets_owned", "Widgets Owned"],
                },
                "signupDate": {
                    "action": "alter",
                    "new_name": "signup_date",
                    "dtype": "string",
                },
            },
            auth=self.auth,
        )
        r = requests.get(self.endpoint("get_schema"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
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
                    "widgetsOwned": ["widgets_owned", "Widgets Owned"],
                },
            },
        )
        r = requests.get(self.endpoint("get_data"), auth=self.auth)
        self.assertEqual(r.status_code, 200)
        df = pd.read_csv(io.StringIO(r.text))
        self.assertEqual(
            list(df.columns), ["firstName", "lastName", "signup_date", "widgetsOwned"]
        )
