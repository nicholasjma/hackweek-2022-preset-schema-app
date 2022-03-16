from __future__ import annotations

import hashlib
import pickle
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from enum import Enum, IntEnum
from typing import Any, Dict, List

import pandas as pd
from flask import Flask, Response, make_response, request
from flask_classful import FlaskView, route

from word_ranker import similarity_ranking


@dataclass
class ResponseCode:
    code: int

    def __call__(self, body: Any) -> Response:
        return make_response(body, self.code)


class Responses(Enum):
    notfound = ResponseCode(404)
    invalid = ResponseCode(400)
    unauthorized = ResponseCode(403)
    ok = ResponseCode(200)
    unimplemented = ResponseCode(501)

    def __call__(self, body: Any) -> Response:
        return self.value(body)


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
            return lambda col: col.astype("Int64")
        elif self is Dtypes.timestamp:
            return lambda col: pd.to_datetime(col, errors="coerce")


class Actions(IntEnum):
    drop = 1
    add = 2
    map = 3
    alter = 4


def authorize(authorization: Dict[str, str]) -> bool:
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
        self.alternative_lookup_map = None
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

    def get_matches(self, name) -> Dict[str, int]:
        """Return ordered list of matches here"""
        if name in self.schema or name in self.alternative_lookup_map:
            return {name: 100}
        ranking = similarity_ranking(name, self.get_corpus())
        return {
            column: similarity
            for column, similarity in ranking.items()
            if similarity >= 75
        }

    def get_corpus(self):
        return set(self.schema.keys()) | set(self.alternative_lookup_map.keys())


class SchemaApp(FlaskView):
    def __init__(self):
        pass

    @route("/")
    def index(self):
        return "See API documentation"

    @route("/test_auth")
    def test_auth(self) -> Response:
        """See if username/password work, note all endpounits require username/password auth"""
        if authorize(request.authorization):
            return Responses.ok("Authorized")
        else:
            return Responses.unauthorized("Invalid Authorization")

    @route("/upload_csv", methods=["GET", "POST"])
    def upload_csv(self) -> Response:
        """
        Begin the csv upload process, should upload a file called `file`

        The response will look something like
        {"suggestions": {"first_name": ["firstName"]}}
        """
        if request.method == "GET":
            return 400
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        if request.method == "POST":
            self.load_state()
            self.state.pending_df = pd.read_csv(request.files["file"])
            response = {"suggestions": {}}
            for column in self.state.pending_df.columns:
                if (
                    column not in self.state.schema
                    and column not in self.state.alternative_lookup_map
                ):
                    response["suggestions"][column] = state.get_matches(column)
            self.save_state()
            return Responses.ok(response)

    @route("/cancel_upload", methods=["GET", "POST"])
    def cancel_upload(self) -> Response:
        """Cancel upload process"""
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        if request.method == "GET":
            return Responses.invalid("GET not supported for cancel_upload")
        self.load_state()
        self.state.pending_df = None
        self.save_state()
        return Responses.ok("Upload cancelled")

    @route("/get_schema", methods=["GET"])
    def get_schema(self) -> Response:
        """Get the schema, response will be a json with the schema"""
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        self.load_state()
        return Responses.ok(
            {
                "schema": self.state.schema,
                "schema_alternatives": self.state.schema_alternatives,
            },
        )

    @route("/get_data", methods=["GET", "POST"])
    def get_data(self) -> Response:
        """Get the data, response will be the data in csv format as the response body"""
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        self.load_state()
        return Responses.ok(self.state.data.to_csv(index=False))

    @route("/get_data_json", methods=["GET", "POST"])
    def get_data_json(self) -> Response:
        """Get the data as json"""
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        self.load_state()
        return Responses.ok(self.state.data.to_json(index=False))

    @route("/get_pending", methods=["GET"])
    def get_pending(self) -> Response:
        """Get the pending data, for debug purposes"""
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        self.load_state()
        return Responses.ok(self.state.pending_df.to_csv(index=False))

    @route("/reset", methods=["GET", "POST"])
    def reset(self) -> Response:
        """Reset data and schema to defaults"""
        global state
        if request.method == "GET":
            return 400
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        del state
        state = State()
        self.save_state()
        return Responses.ok("Reset complete")

    @route("/complete_upload", methods=["GET", "POST"])
    def complete_upload(self) -> Response:
        """
        Complete csv upload, to be called after `upload_csv`

        The request json should be formatted as below, with one key per column specified in the `upload_csv` response
        {
            "newcol1": {"action": "drop"},
            "newcol2": {"action": "add", "new_name": "favoriteColor", "dtype": "string"},
            "newcol3": {"action": "map", "map_to_name": "signupDate"}
        }
        """
        global state
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        if request.method == "GET":
            return 400
        self.load_state()
        new_state = deepcopy(state)
        if new_state.pending_df is None:
            return Responses.invalid("Use upload_csv first")
        actions_configs = request.get_json()
        if not isinstance(actions_configs, dict):
            return Responses.invalid("Need json actions map, see documentation")
        new_columns = [
            x for x in new_state.pending_df.columns if x not in new_state.schema
        ]
        rename_map = {}
        drop_cols = []
        for column in new_columns:
            action_config = actions_configs.get(column)
            if column in self.state.alternative_lookup_map:
                rename_map[column] = self.state.alternative_lookup_map[column]
            elif action_config is None:
                Responses.invalid(f"Action not specified for column {column}")
            else:
                action = Actions[action_config["action"]]
            if action is Actions.drop:
                drop_cols.append(column)
            elif action is Actions.add:
                dtype = action_config.get("dtype")
                new_name = action_config.get("new_name")
                if dtype is None:
                    return Responses.invalid("Missing dtype field for action add")
                if new_name is None:
                    return Responses.invalid("Missing new_name field for action add")
                elif new_name in new_state.schema:
                    return Responses.invalid(
                        f"New column name {new_name} already in schema"
                    )
                else:
                    rename_map[column] = new_name
                if dtype not in Dtypes.__members__:
                    return Responses.invalid(f"Invalid dtype {dtype}")
                new_state.schema[new_name] = Dtypes[dtype].name
                new_state.schema_alternatives[new_name] = []
                new_state.data[new_name] = pd.NA
            elif action is Actions.map:
                map_to_name = actions_configs[column].get("map_to_name")
                if map_to_name is None:
                    return Responses.invalid("Missing map_to_name field for action map")
                elif map_to_name not in new_state.schema:
                    return Responses.invalid(
                        f"map_to_name {map_to_name} not in current schema"
                    )
                else:
                    rename_map[column] = map_to_name
        new_state.update_alternatives_lookup()
        cleaned_new_data = new_state.pending_df.drop(columns=drop_cols).rename(
            columns=rename_map
        )
        new_state.data = pd.concat([new_state.data, cleaned_new_data]).reset_index(
            drop=True
        )
        for col, dtype in new_state.schema.items():
            new_state.data[col] = Dtypes[dtype].converter(new_state.data[col])
        assert set(new_state.data.columns) == set(new_state.schema.keys())
        state = new_state
        self.save_state()
        return Responses.ok("Upload complete")

    @route("/update_schema", methods=["GET", "POST"])
    def update_schema(self) -> Response:
        """
        Update the schema

        JSON should look like this
        {
            "existing_col": {
                "action": "alter",
                "new_name": "some_other_col",
                "dtype": "string",
                "alternatives": ["col1", "col2"],
            },
            "new_col": {
                "action": "add",
                "dtype": "string",
                "alternatives": ["new_col_alias_1", "new_col_alias_2"],
            },
            "existing_col_to_delete": {
                "action": "drop",
            }
        }
        """
        global state
        if not authorize(request.authorization):
            return Responses.unauthorized("Invalid Authorization")
        if request.method == "GET":
            return 400
        self.load_state()
        new_state = deepcopy(self.state)
        for column, action_dict in request.get_json().items():
            action = Actions[action_dict.get("action")]
            if action is Actions.drop:
                if column in new_state.schema:
                    del new_state.schema[column]
                    del new_state.schema_alternatives[column]
                    del new_state.data[column]
                else:
                    return Responses.invalid(f"Invalid column {column}")
            elif action is Actions.add:
                if column in new_state.schema:
                    return Responses.invalid(f"Column {column} already exists")
                else:
                    dtype = action_dict.get("dtype")
                    alternatives = action_dict.get("alternatives")
                    if alternatives and set(alternatives) & (
                        set(new_state.schema) | set(new_state.alternative_lookup_map)
                    ):
                        return Responses.invalid(
                            f"Alternatives {alternatives} must not already exist as columns or mappings"
                        )
                    if dtype is None:
                        return Responses.invalid(
                            f"Invalid dtype {dtype} for column {column}"
                        )
                    new_state.schema[column] = Dtypes[dtype].name
                    new_state.data[column] = pd.NA
                    print(new_state.data)
                    new_state.schema_alternatives[column] = alternatives or []
            elif action is Actions.alter:
                if column in new_state.schema:
                    dtype = action_dict.get("dtype")
                    new_name = action_dict.get("new_name")
                    alternatives = action_dict.get("alternatives")
                    if new_name:
                        if new_name in new_state.schema:
                            return Responses.invalid(
                                f"New name {new_name} for column {column} already exists"
                            )
                        new_state.schema[new_name] = new_state.schema[column]
                        del new_state.schema[column]
                        new_state.data = new_state.data.rename(
                            columns={column: new_name}
                        )
                    else:
                        new_name = column
                    if dtype:
                        if dtype in Dtypes.__members__:
                            new_state.schema[new_name] = dtype
                            new_state.data[new_name] = Dtypes[dtype].converter(
                                new_state.data[new_name]
                            )
                        else:
                            return Responses.invalid(
                                f"Invalid dtype {dtype} for column {new_name}"
                            )
                    if alternatives:
                        if set(alternatives) & (
                            set(new_state.schema)
                            | set(new_state.alternative_lookup_map)
                        ):
                            return Responses.invalid(
                                f"Alternatives {alternatives} must not already exist as columns or mappings"
                            )
                        new_state.schema_alternatives[new_name] = alternatives
                else:
                    return Responses.invalid(
                        f"Cannot alter non-existent column {column}"
                    )
            else:
                return Responses.invalid(f"Invalid action {action} for column {column}")
            new_state.update_alternatives_lookup()
        state = new_state
        self.save_state()
        return Responses.ok("Schema update complete")

    @property
    def state(self) -> State:
        """helper function to get the state from the global state variable, workaround for flask_classful limitation"""
        global state
        return state

    def load_state(self):
        global state
        with open("state.pkl", "rb") as f:
            state = pickle.load(f)

    def save_state(self):
        global state
        with open("state.pkl", "wb") as f:
            pickle.dump(state, f)


valid_users: Dict[str, str] = {
    "iterable": "1116977ba16abc1fd84fec9cd1494bc18faa596307737d7f5e2e1ef5aa230874",
}

app = Flask(__name__)
state = State()
SchemaApp.register(app, route_base="/")
