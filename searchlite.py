#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Mini searching db."""

import pprint
import uuid
import tempfile
import os
import json

MAX_BUCKET_SIZE = 20


class DB(object):
    """Mini elasticsearch-like database."""

    def __init__(self, filename=None):
        """Create new db object.

        If filename is passed, load db from disk.
        Otherwise, create an empty db.
        """
        self.ids = {}
        self.prefixes = {}
        if filename is not None:
            self.load(filename)

    def add(self, item):
        """Item is an arbitrary dictionary to be added to the database.

        Keys are indexed
        """
        itemid = str(uuid.uuid4())
        self.ids[itemid] = item
        for key in item.keys():
            prefixes = self.prefixes
            for i, char in enumerate(key):
                if char in prefixes:
                    if i == len(key) - 1:
                        if str(item[key]) in prefixes[char][0]:
                            prefixes[char][0][str(item[key])].append(itemid)
                        else:
                            prefixes[char][0][str(item[key])] = [itemid]
                    else:
                        prefixes = prefixes[char][1]
                    # if len(prefixes[char]) > MAX_BUCKET_SIZE:

                else:
                    if i == len(key) - 1:
                        prefixes[char] = [{str(item[key]):[itemid]}]
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
        return [self.ids[i] for i in prefixes[field[-1]][0][str(val)]]

    def pprint(self):
        """Pretty print internals of object."""
        pprint.pprint(self.ids)
        pprint.pprint(self.prefixes)

    def save(self, filename):
        """Save data to a file atomically."""
        with tempfile.NamedTemporaryFile(
            'w',
            dir=os.path.dirname(filename),
            delete=False
        ) as tmpfile:
            json.dump(
                [self.ids, self.prefixes],
                tmpfile,
                separators=(',', ':'))
            os.rename(tmpfile.name, filename)

    def load(self, filename):
        """Load data from a file. Overwrites current database contents."""
        self.ids, self.prefixes = json.load(open(filename))
