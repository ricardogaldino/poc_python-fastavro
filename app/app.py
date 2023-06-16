from fastavro import writer, reader, parse_schema

schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id",  "type": ["int", "null"]},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["string", "null"]}
    ]
}

parsed_schema = parse_schema(schema)

records = [
    {"id": 1, "name": "Galdino", "email": "ricardo@email.com"},
    {"id": 2, "name": "Paulo"},
    {"id": 3, "name": "Elias", "email": "elias@email.com"}
]

# Writing
with open('user.avro', 'wb') as out:
    writer(out, parsed_schema, records)

# Reading
with open('user.avro', 'rb') as fo:
    for record in reader(fo):
        print(record)
