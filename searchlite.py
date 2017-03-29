#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def search(string, field, node):
	if string[0] in node:
		return search(string[1:], field, node)
	elif node['_IS_LEAF']:
		return [i for i in node if string in i]
	else:
		return False
