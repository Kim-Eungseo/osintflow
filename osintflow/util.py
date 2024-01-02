import hashlib
import re

IP_PATTERN = re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b")
DOMAIN_PATTERN = re.compile(r"\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]")


def parse_template(template, kwargs, globals, locals):
    for match in re.findall(r'<.*?>', template):
        statement = match[1:-1]
        globals.update(kwargs)
        repl = eval(statement, globals, locals)
        template = template.replace(match, str(repl))
    return template


def remove_duplicates_by_key(list_of_dicts, key):
    seen = set()
    return [x for x in list_of_dicts if
            tuple(x[k] for k in key) not in seen and not seen.add(tuple(x[key] for key in key))]


def calculate_md5(input_string):
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()
