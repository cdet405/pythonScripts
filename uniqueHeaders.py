# Try to return all headers with hierarchy to rebuild schema  
import json
import csv
file_path = "tickets1685558410.json"


def find_headers(obj, hierarchy=[]):
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_hierarchy = hierarchy + [key]
            if isinstance(value, (dict, list)):
                yield from find_headers(value, new_hierarchy)
            else:
                yield '.'.join(new_hierarchy)
    elif isinstance(obj, list):
        for item in obj:
            yield from find_headers(item, hierarchy)


with open(file_path, "r") as source_file:
    json_data = json.load(source_file)
    headers = set(find_headers(json_data))

    
with open('headers.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Headers'])
    for header in headers:
        writer.writerow([header])
