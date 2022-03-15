from datetime import date, datetime
from enum import Enum, IntEnum
from typing import List

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request
from flask_classful import FlaskView, route

app = Flask(__name__)


class Responses(Enum):
    notfound = 404
    invalid = 403
    ok = 200


class Dtypes(Enum):
    string = lambda col: col.astype(str)
    long = lambda col: col.astype(int)
    int = lambda col: col.astype(int)
    timestamp = lambda col: pd.to_datetime(col)


class Actions(IntEnum):
    drop = 1
    add = 2
    map = 3


def get_matches(self, name: str) -> List[str]:
    raise NotImplementedError


class SchemaApp(FlaskView):
    def __init__(self):
        self.schema = {
            "email": Dtypes.string,
            "firstName": Dtypes.string,
            "lastName": Dtypes.string,
            "signupDate": Dtypes.timestamp,
        }

        data = pd.DataFrame(
            {
                "email": [
                    "nick.ma@iterable.com",
                    "brett.eckrich@iterable.com",
                    "chris@iterable.com",
                    "keegan@iterable.com",
                    "kyle.moulder@iterable.com",
                    "michelle.chuang@iterable.com",
                    "mona.bazzaz@iterable.com",
                    "steven.milov@iterable.com",
                    "tracy.schaffer@iterable.com",
                ],
            }
        )
        names = [
            "Nick Ma",
            "Brett Eckrich",
            "Chris Wheeler",
            "Keegan Hinson",
            "Kyle Moulder",
            "Michelle Chuang",
            "Mona Bazzaz",
            "Steven Milov",
            "Tracy Schaffer",
        ]
        data["firstName"] = [name.split()[0] for name in names]
        data["lastName"] = [name.split()[1] for name in names]
        data["signupDate"] = [
            date(year=2022, month=2, day=x + 1) for x in range(len(names))
        ]
        self.data = data
        self.pending_df = None

    @route("/")
    def index(self):
        return "See API documentation"

    @route("/upload_csv", methods=["GET", "POST"])
    def upload_csv(self):
        if request.method == "POST":
            self.pending_df = pd.read_csv(request.files["file"])
            response = {"suggestions": {}}
            for column in self.pending_df.columns:
                if column not in self.schema:
                    response["suggestions"][column] = get_matches(column)
            return response, Responses.ok

    @route("/get_schema", methods=["GET"])
    def get_schema(self):
        return self.schema, Responses.ok

    @route("/complete_upload", methods=["GET", "POST"])
    def complete_upload(self):
        if self.pending_df is None:
            return "Use upload_csv first", Responses.invalid
        actions_configs = request.get_json()
        if not isinstance(actions_configs, dict):
            return "Need json actions map, see documentation", Responses.invalid
        # should look like
        # {
        #     "newcol1": {"action": "drop"},
        #     "newcol2": {"action": "add", "new_name": "favoriteColor", "dtype": "string"},
        #     "newcol3": {"action": "map", "map_to_name": "signupDate"}
        # }
        new_columns = [x for x in self.pending_df.columns if x not in self.schema]
        rename_map = {}
        drop_cols = []
        for column in new_columns:
            action_config = actions_configs.get(column)
            if action_config is None:
                return f"Action not specified for column {column}"
            else:
                action = Actions[action_config]
            if action is Actions.drop:
                drop_cols.append(column)
            elif action is Actions.add:
                dtype = action_config.get("dtype")
                new_name = action_config.get("new_name")
                if dtype is None:
                    return "Missing dtype field for action add", Responses.invalid
                if new_name is None:
                    return "Missing new_name field for action add", Responses.invalid
                if new_name in self.schema:
                    return (
                        f"New column name {new_name} already in schema",
                        Responses.invalid,
                    )
                if dtype not in Dtypes:
                    return (
                        f"Invalid dtype {actions_configs[column]['dtype']}",
                        Responses.invalid,
                    )
                self.schema[new_name] = Dtypes[dtype]
                self.data[new_name] = pd.NA
            elif action is Actions.map:
                map_to_name = actions_configs[column].get("map_to_name")
                if map_to_name is None:
                    return "Missing map_to_name field for action map", Responses.invalid
                elif map_to_name not in self.schema:
                    return (
                        f"map_to_name {map_to_name} not in current schema",
                        Responses.invalid,
                    )
                else:
                    rename_map[column] = map_to_name
        cleaned_new_data = self.pending_df.drop(columns=drop_cols).rename(
            columns=rename_map
        )
        self.data = pd.concat([self.data, cleaned_new_data])
        for col, dtype in self.schema.items():
            self.data[col] = dtype.value(self.data[col])
        assert set(self.data.columns) == set(self.schema.keys())
        return "", Responses.ok


SchemaApp.register(app, route_base="/")
