"""
Helpers module for the OIC DevOps package.

This module provides helper functions used throughout the package.
"""

import datetime
import os
import random
import re
import string
import uuid
from typing import Any, Dict, Optional


def format_date(
	date_obj: datetime.datetime, format_str: str = '%Y-%m-%dT%H:%M:%S.%fZ'
) -> str:
	"""
	Format a datetime object as a string.

	Args:
	    date_obj: The datetime object to format.
	    format_str: The format string to use.

	Returns:
	    str: The formatted date string.

	"""
	return date_obj.strftime(format_str)


def parse_date(
	date_str: str, format_str: str = '%Y-%m-%dT%H:%M:%S.%fZ'
) -> datetime.datetime:
	"""
	Parse a date string into a datetime object.

	Args:
	    date_str: The date string to parse.
	    format_str: The format string to use.

	Returns:
	    datetime.datetime: The parsed datetime object.

	Raises:
	    ValueError: If the date string cannot be parsed.

	"""
	return datetime.datetime.strptime(date_str, format_str)


def generate_identifier(
	name: str,
	prefix: Optional[str] = None,
	suffix: Optional[str] = None,
	max_length: int = 50,
) -> str:
	"""
	Generate a valid OIC identifier from a name.

	Args:
	    name: The name to generate an identifier from.
	    prefix: Optional prefix to add to the identifier.
	    suffix: Optional suffix to add to the identifier.
	    max_length: Maximum length of the identifier.

	Returns:
	    str: The generated identifier.

	"""
	# Remove non-alphanumeric characters and replace spaces with underscores
	identifier = re.sub(r'[^a-zA-Z0-9_]', '', name.replace(' ', '_'))

	# Ensure the identifier starts with a letter
	if not identifier or not identifier[0].isalpha():
		identifier = 'id_' + identifier

	# Add prefix if provided
	if prefix:
		prefix = re.sub(r'[^a-zA-Z0-9_]', '', prefix)
		if not prefix[0].isalpha():
			prefix = 'p_' + prefix
		identifier = prefix + '_' + identifier

	# Add suffix if provided
	if suffix:
		suffix = re.sub(r'[^a-zA-Z0-9_]', '', suffix)
		if not suffix[0].isalpha():
			suffix = 's_' + suffix
		identifier = identifier + '_' + suffix

	# Truncate the identifier if it's too long
	if len(identifier) > max_length:
		identifier = identifier[:max_length]

	return identifier


def generate_random_string(length: int = 8) -> str:
	"""
	Generate a random string of the specified length.

	Args:
	    length: The length of the string to generate.

	Returns:
	    str: The generated random string.

	"""
	chars = string.ascii_letters + string.digits
	return ''.join(random.choices(chars, k=length))


def generate_uuid() -> str:
	"""
	Generate a random UUID.

	Returns:
	    str: The generated UUID.

	"""
	return str(uuid.uuid4())


def get_file_extension(file_path: str) -> str:
	"""
	Get the extension of a file.

	Args:
	    file_path: The path to the file.

	Returns:
	    str: The file extension (without the dot).

	"""
	_, ext = os.path.splitext(file_path)
	return ext.removeprefix('.')


def ensure_directory_exists(directory_path: str) -> str:
	"""
	Ensure that a directory exists, creating it if necessary.

	Args:
	    directory_path: The path to the directory.

	Returns:
	    str: The path to the directory.

	"""
	if not os.path.exists(directory_path):
		os.makedirs(directory_path)
	return directory_path


def flatten_dict(
	d: Dict[str, Any], parent_key: str = '', separator: str = '.'
) -> Dict[str, Any]:
	"""
	Flatten a nested dictionary.

	Args:
	    d: The dictionary to flatten.
	    parent_key: The parent key (used for recursion).
	    separator: The separator to use between nested keys.

	Returns:
	    Dict: The flattened dictionary.

	"""
	items = []
	for k, v in d.items():
		new_key = f'{parent_key}{separator}{k}' if parent_key else k
		if isinstance(v, dict):
			items.extend(flatten_dict(v, new_key, separator).items())
		else:
			items.append((new_key, v))
	return dict(items)


def unflatten_dict(d: Dict[str, Any], separator: str = '.') -> Dict[str, Any]:
	"""
	Unflatten a dictionary that was flattened using flatten_dict.

	Args:
	    d: The flattened dictionary.
	    separator: The separator used between nested keys.

	Returns:
	    Dict: The unflattened dictionary.

	"""
	result = {}
	for key, value in d.items():
		parts = key.split(separator)
		temp = result
		for part in parts[:-1]:
			if part not in temp:
				temp[part] = {}
			temp = temp[part]
		temp[parts[-1]] = value
	return result


def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Merge two dictionaries, with dict2 values taking precedence.

	Args:
	    dict1: The first dictionary.
	    dict2: The second dictionary.

	Returns:
	    Dict: The merged dictionary.

	"""
	result = dict1.copy()
	for key, value in dict2.items():
		if key in result and isinstance(result[key], dict) and isinstance(value, dict):
			result[key] = merge_dicts(result[key], value)
		else:
			result[key] = value
	return result


def remove_none_values(d: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Remove None values from a dictionary.

	Args:
	    d: The dictionary to process.

	Returns:
	    Dict: The dictionary with None values removed.

	"""
	return {k: v for k, v in d.items() if v is not None}


def chunk_list(lst: list, chunk_size: int) -> list:
	"""
	Split a list into chunks of the specified size.

	Args:
	    lst: The list to split.
	    chunk_size: The size of each chunk.

	Returns:
	    List: The list of chunks.

	"""
	return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
