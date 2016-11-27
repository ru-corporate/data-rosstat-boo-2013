# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 13:37:16 2016

@author: Евгений
"""
from inspect_columns import Columns

EMPTY = int('0')
QUOTE_CHAR = '"'


# row transformation

def get_adjust_func(unit):
    """Select function to adjust string values to '000 rub"""
    if unit == '384':
        # no adjustment
        return lambda x: int(x)
    elif unit == '383':
        # adjust rub to thousand rub
        return lambda x: int(round(0.001*float(x)))
    elif unit == '385':
        # adjust mln rub to thousand rub
        return lambda x: 1000*int(x)
    else:
        raise ValueError("Unit not supported: " + unit)


def parse_row(rowd):
    """Return modified *rowd* dictionary."""

    # assemble new text cols
    ok1, ok2, ok3 = okved3(rowd['okved'])
    org, title = dequote(rowd['name'])
    region = rowd['inn'][0:2]
    # warning: 'date' may not be in rowd.keys() in some datasets
    date_reviewed = rowd['date']
    text = [rowd['year'], date_reviewed, ok1, ok2, ok3,
            org, title, region, rowd['inn'],
            rowd['okpo'], rowd['okopf'], rowd['okfs']]

    # assemble new data cols
    func = get_adjust_func(rowd['unit'])
    data = [func(rowd[k]) for k in Columns.DATACOLS]

    return text+data


def get_parsed_colnames():
    """Return colnames corresponding to parse_row(). """
    return ['year', 'date', 'ok1', 'ok2', 'ok3',
            'org', 'title', 'region', 'inn',
            'okpo', 'okopf', 'okfs'] + Columns.RENAMED_DATACOLS


def get_colname_dtypes():
    """Return types correspoding to get_colnames().
       Used to speed up CSV import in custom_df_reader(). """
    dtype_dict = {k: int for k in get_parsed_colnames()}
    string_cols = ['date', 'org', 'title', 'region', 'inn',
                   'okpo', 'okopf', 'okfs']
    dtype_dict.update({k: str for k in string_cols})
    return dtype_dict

# stateless transformations


def okved3(code_string):
    """Get 3 levels of OKVED codes from *code_string*."""
    codes = [int(x) for x in code_string.split(".")]
    return codes + [EMPTY] * (3-len(codes))


def dequote(name):
    """Split company *name* to organisation and title."""
    # Warning: will not work well on company names with more than 4 quotechars
    parts = name.split(QUOTE_CHAR)
    org = parts[0].strip()
    cnt = name.count(QUOTE_CHAR)
    if cnt == 2:
        title = parts[1].strip()
    elif cnt > 2:
        title = QUOTE_CHAR.join(parts[1:])
    else:
        title = name
    return org, title.strip()
