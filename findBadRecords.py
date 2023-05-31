# Locate Schema Breaking Records
import json

with open('nld-tickets1685558410.ndjson', "r") as source_file:
    line_number = 1
    for line in source_file:
        json_line = json.loads(line)
        if ("customer" in json_line and json_line["customer"] is not None
                and "meta" in json_line["customer"] and json_line["customer"]["meta"] is not None
                and "profile_picture_url" in json_line["customer"]["meta"]):
            print(f"Found 'customer.meta.profile_picture_url' in line {line_number}:\
             [id:{json_line['id']}][ts:{json_line['created_datetime']}")
        line_number += 1
