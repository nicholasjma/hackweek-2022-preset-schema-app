import hashlib
from datetime import date, datetime
from enum import Enum, IntEnum
from typing import Dict, List

import numpy as np
import pandas as pd
from flask import Flask, jsonify, make_response, request
from flask_classful import FlaskView, route

app = Flask(__name__)


class Responses:
    notfound = 404
    invalid = 400
    unauthorized = 403
    ok = 200


class Dtypes(Enum):
    string = "string"
    long = "long"
    int = "int"
    timestamp = "timestamp"

    @property
    def converter(self):
        if self is Dtypes.string:
            return lambda col: col.astype(str)
        elif self is Dtypes.long or self is Dtypes.int:
            return lambda col: col.astype(int)
        elif self is Dtypes.timestamp:
            return lambda col: pd.to_datetime(col)


class Actions(IntEnum):
    drop = 1
    add = 2
    map = 3


def get_matches(name: str) -> List[str]:
    return []


valid_users = {
    "iterable": "1116977ba16abc1fd84fec9cd1494bc18faa596307737d7f5e2e1ef5aa230874",
}


def authorize(authorization: Dict[str, str]):
    try:
        if authorization["username"] in valid_users:
            pass_hash = hashlib.sha256(authorization["password"].encode()).hexdigest()
            if pass_hash == valid_users[authorization["username"]]:
                return True
    except KeyError:
        pass
    return False


class State:
    def __init__(self):
        self.schema = {
            "email": Dtypes.string.name,
            "firstName": Dtypes.string.name,
            "lastName": Dtypes.string.name,
            "signupDate": Dtypes.timestamp.name,
        }
        self.schema_alternatives = {k: [] for k in self.schema.keys()}
        self.update_alternatives_lookup()

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

    def update_alternatives_lookup(self):
        self.alternative_lookup_map = {}
        for column, alternates in self.schema_alternatives.items():
            for alternate in alternates:
                self.alternative_lookup_map[alternate] = column


state = State()


class SchemaApp(FlaskView):
    def __init__(self):
        pass

    @route("/")
    def index(self):
        return "See API documentation"

    @route("/test_auth")
    def test_auth(self):
        if authorize(request.authorization):
            return make_response("Authorized", Responses.ok)
        else:
            return make_response("Invalid Authorization", Responses.unauthorized)

    @route("/upload_csv", methods=["GET", "POST"])
    def upload_csv(self):
        if request.method == "GET":
            return 400
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        if request.method == "POST":
            self.state.pending_df = pd.read_csv(request.files["file"])
            response = {"suggestions": {}}
            for column in self.state.pending_df.columns:
                if (
                    column not in self.state.schema
                    and column not in self.state.alternative_lookup_map
                ):
                    response["suggestions"][column] = get_matches(column)
            return make_response(response, Responses.ok)

    @route("/cancel_upload", methods=["GET", "POST"])
    def cancel_upload(self):
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        if request.method == "GET":
            return 400
        self.state.pending_df = None
        return make_response("Upload cancelled", Responses.ok)

    @route("/get_schema", methods=["GET"])
    def get_schema(self):
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        return make_response(self.state.schema, Responses.ok)

    @route("/get_data", methods=["GET"])
    def get_data(self):
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        return make_response(self.state.data.to_csv(index=False), Responses.ok)

    @route("/get_pending", methods=["GET"])
    def get_pending(self):
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        return make_response(self.state.pending_df.to_csv(index=False), Responses.ok)

    @route("/reset", methods=["GET", "POST"])
    def reset(self):
        global state
        if request.method == "GET":
            return 400
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        state = State()
        return make_response("Reset complete", Responses.ok)

    @route("/complete_upload", methods=["GET", "POST"])
    def complete_upload(self):
        if not authorize(request.authorization):
            return make_response("Invalid Authorization", Responses.unauthorized)
        if request.method == "GET":
            return 400
        if self.state.pending_df is None:
            return make_response("Use upload_csv first", Responses.invalid)
        actions_configs = request.get_json()
        if not isinstance(actions_configs, dict):
            return make_response(
                "Need json actions map, see documentation", Responses.invalid
            )
        # should look like
        # {
        #     "newcol1": {"action": "drop"},
        #     "newcol2": {"action": "add", "new_name": "favoriteColor", "dtype": "string"},
        #     "newcol3": {"action": "map", "map_to_name": "signupDate"}
        # }
        new_columns = [
            x for x in self.state.pending_df.columns if x not in self.state.schema
        ]
        rename_map = {}
        drop_cols = []
        for column in new_columns:
            action_config = actions_configs.get(column)
            if action_config is None:
                return f"Action not specified for column {column}"
            else:
                action = Actions[action_config["action"]]
            if action is Actions.drop:
                drop_cols.append(column)
            elif action is Actions.add:
                dtype = action_config.get("dtype")
                new_name = action_config.get("new_name")
                if dtype is None:
                    return make_response(
                        "Missing dtype field for action add", Responses.invalid
                    )
                if new_name is None:
                    return make_response(
                        "Missing new_name field for action add", Responses.invalid
                    )
                if new_name in self.state.schema:
                    return (
                        f"New column name {new_name} already in schema",
                        Responses.invalid,
                    )
                if dtype not in Dtypes.__members__:
                    return (
                        f"Invalid dtype {dtype}",
                        Responses.invalid,
                    )
                self.state.schema[new_name] = Dtypes[dtype].name
                self.state.data[new_name] = pd.NA
            elif action is Actions.map:
                map_to_name = actions_configs[column].get("map_to_name")
                if map_to_name is None:
                    return make_response(
                        "Missing map_to_name field for action map", Responses.invalid
                    )
                elif map_to_name not in self.state.schema:
                    return (
                        f"map_to_name {map_to_name} not in current schema",
                        Responses.invalid,
                    )
                else:
                    rename_map[column] = map_to_name
        cleaned_new_data = self.state.pending_df.drop(columns=drop_cols).rename(
            columns=rename_map
        )
        self.state.data = pd.concat([self.state.data, cleaned_new_data]).reset_index(
            drop=True
        )
        for col, dtype in self.state.schema.items():
            self.state.data[col] = Dtypes[dtype].converter(self.state.data[col])
        assert set(self.state.data.columns) == set(self.state.schema.keys())
        return make_response("", Responses.ok)

    @property
    def state(self):
        global state
        return state


SchemaApp.register(app, route_base="/")
