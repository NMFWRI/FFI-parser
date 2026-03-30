from re import findall, sub
from datetime import date
from dateutil import parser
from pandas import isna


def create_url(**kwargs):
    """
    create a SQLAlchemy URL out of a config file parameters
    The config.ini file is excluded from the git repository for security purposes; you'll have to create your own for
    your own server

    """
    driver = kwargs['driver']
    user = kwargs['user']
    pwd = kwargs['password']
    server = kwargs['server']
    database = kwargs['database']
    if 'postgresql' in kwargs['type'].lower():
        conn_str = f"{driver}://{user}:{pwd}@{server}/{database}"
    elif 'sqlserver' in kwargs['type'].lower():
        conn_str = f"{driver}://{user}:{pwd}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        return ""
    return conn_str


def parse_camelcase(txt: str):
    """
    convert CamelCase to snake_case

    :param txt: the string in CamelCase to be converted
    :return: the string returned as snake_case
    """
    segments = []
    cur_word = ''
    prev = ''
    for idx, char in enumerate(txt):
        try:
            next_char = txt[idx+1]
        except IndexError:
            next_char = ''

        # find where the word changes from lower case to uppercase or vice-versa
        if (prev.isupper() and char.isupper() and next_char.islower()) or \
                (prev.islower() and char.isupper()):
            segments.append(cur_word)
            cur_word = ''
            cur_word += char
        else:
            cur_word += char
        prev = char
    segments.append(cur_word)

    new_string = '_'.join(word.lower() for word in segments)
    return new_string


def normalize_string(string: str):
    """
    turns strings into the formatting that is standard for postgres

    :param string: string to be formatted
    :return: the formatted string
    """
    temp1 = string.replace(' ', '').replace('.', '').replace('-', '')
    temp2 = sub(r'\(\w+\)', '', temp1)
    snake_case = parse_camelcase(temp2)
    return snake_case


def convert_datetime(datetime):
    """
    SQL server has an issue parsing datetimes with the timezone information explicitly appended to the end, so we need
    to convert to local time and strip off the timezone difference from UTC
    """
    if not isna(datetime):
        tz_date = parser.parse(datetime).astimezone().isoformat()
        date_notz = sub(r'-0\d:00', '', tz_date)
        trim_date = sub(r'([1-9]{2,})0+$', r'\1', date_notz)
        sql_date = sub(r'.(\d{3})\d+$', r'\1', trim_date)
        if findall(r':\d{5}$', sql_date):
            s_len = len(sql_date)
            sql_date = f'{sql_date[:s_len-3]}.{sql_date[s_len-3:]}'

        return sql_date
    else:
        return datetime


def to_datenum(datetime):
    """
    convert a date to a datetime value (number of seconds since Jan 1, 1900, I think) in the format that SQLServer uses.
    This is different than how other programs do it, but I wanted it to align with MSSQL, since that's what FFI does.

    :param datetime: datetime value to be converted to big int
    :return: the big int of the datetime value
    """

    date_parts = findall(r'(\d{4})-(\d{2})-(\d{2})', datetime)[0]  # regex to parse parts of datetime
    date_key = {'year': int(date_parts[0]), 'month': int(date_parts[1]), 'day': int(date_parts[2])}
    offset = 693595  # datetime int value of 1/1/1900

    this_date = date(date_key['year'], date_key['month'], date_key['day'])
    date_ord = this_date.toordinal()
    date_num = str(date_ord - offset)

    return date_num


def strip_namespace(string):
    """
    strips the namespace off a tag element of an XML file

    :param string: the string from which to remove the namespace string
    :return: another string. but with the namespace removed
    """

    new_string = sub(r'\{http://\w+\.\w{3}[\w/.\d]+\}', '', string, count=1)
    return new_string


def quoted_list(this_list: [str]):
    quoted_items = [f"'{thing}'" for thing in this_list]
    list_str = ','.join(quoted_items)

    return list_str
