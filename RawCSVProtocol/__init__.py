# -*- coding: utf-8 -*-
__author__ = 'Sam Zhang'
"""
Parses object as comma-separated values, with no quote escaping.
"""


class RawCSVProtocol(object):
    def read(self, line):
        """
        :param line:
        :return:
        """
        parts = line.split(',', 1)
        if len(parts) == 1:
            parts.append(None)

        return tuple(parts)

    def write(self, key, value):
        """
        Value is expected to be a string already.
        :param key:
        :param value:
        """
        vals = ','.join((key, value))
        return vals