import re
from typing import List, Optional


def parse_string_into_groups(string: str, pattern: str, groups: Optional[List[str]]):
    """
    Example:
        parse_string_into_groups(
            string=file_name,
            pattern="(.*)\_(.*)\_(.*)\_(.*)\_(.*)\.csv",
            groups=['client_name', 'report_type', 'app_type', 'start_date', 'end_date']
        )
    """
    m = re.compile(pattern)
    g = m.search(string)
    if g:
        class ParsedString(object):
            pass
        parsed_string = ParsedString
        for i, group in enumerate(groups, start=1):
            setattr(parsed_string, group, g.group(i))
        return parsed_string()
    return None
