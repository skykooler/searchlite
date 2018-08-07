#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Mini searching db."""

import pprint
import uuid

MAX_BUCKET_SIZE = 20
IDS = {}


class DB(object):
    """Mini elasticsearch-like database."""

    def __init__(self):
        """Create new empty db object."""
        self.prefixes = {}

    def add(self, item):
        """Item is an arbitrary dictionary to be added to the database.

        Keys are indexed
        """
        itemid = uuid.uuid4()
        IDS[itemid] = item
        for key in item.keys():
            prefixes = self.prefixes
            for i, char in enumerate(key):
                if char in prefixes:
                    if i == len(key) - 1:
                        if item[key] in prefixes[char][0]:
                            prefixes[char][0][item[key]].append(itemid)
                        else:
                            prefixes[char][0][item[key]] = [itemid]
                    else:
                        prefixes = prefixes[char][1]
                    # if len(prefixes[char]) > MAX_BUCKET_SIZE:

                else:
                    if i == len(key) - 1:
                        prefixes[char] = [{item[key]:[itemid]}]
                    else:
                        prefixes[char] = [{}, {}]
                        prefixes = prefixes[char][1]

    def get(self, field, val):
        """Get an item stored in the db.

        Raises KeyError if no such item exists.
        """
        prefixes = self.prefixes
        for char in field[:-1]:
            prefixes = prefixes[char][1]
        return [IDS[i] for i in prefixes[field[-1]][0][val]]

    def pprint(self):
        """Pretty print internals of object."""
        pprint.pprint(IDS)
        pprint.pprint(self.prefixes)
