from datetime import date
from pprint import pprint

options = dict(
	day1=date(1970, 1, 1),         # reqires a date (or None). Here, a default value is set.
	day2=date,                     # requires a date, will be None if not assigned
	something=None,                # accepts anything (that works in json)
	text='',                       # requires a string (or None).  Here, the empty string is default.
	number=int,                    # requires an int (or None)
	seq=[],                        # reqires a list
	cplx=dict(str=dict(str=str)),  # reqires a dict of string to dict of string to string
)


def synthesis():
	pprint(options)
