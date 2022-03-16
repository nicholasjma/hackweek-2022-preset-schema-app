import json
import re
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Dict, List

import requests

base_url = "https://api.iterable.com/"
get_profile_fields_url = "api/users/getFields"

with open("topwords.txt") as word_file:
    cleaned_words = {word.strip().lower() for word in word_file}


def extract_words(s: str):
    res = [x for x in cleaned_word_dict if x in s]
    return sorted(res, key=len, reverse=True)


def similarity(a: str, b: str):
    a, b = map(normalize, (a, b))
    return int(SequenceMatcher(None, a, b).ratio() * 100)


def normalize(field_name: str):
    expr = re.compile(r"[^A-Za-z0-9]+")
    digits_expr = re.compile(r"[0-9]")
    output_name = re.sub(expr, "", field_name)
    output_name = re.sub(digits_expr, "#", output_name)
    return output_name.lower()


def similarity_ranking(word, corpus):
    match_dict = {
        potential_match: similarity(word, potential_match) for potential_match in corpus
    }
    match_dict = dict(
        sorted(match_dict.items(), key=lambda item: item[1], reverse=True)
    )
    return match_dict


if __name__ == "__main__":
    print(similarity_ranking("foo", ["fo_o", "Foo", "bar", "faz"]))
