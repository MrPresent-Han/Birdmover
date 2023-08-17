import fire
from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
from constants import FIELDS, NAME, DEFAULT_ITERATION_RATE
import os


def bulk(name):
    return 'Hello {name}!'.format(name=name)


def export(ip, port, collection_name, local_dir):
    connections.connect("default", host=ip, port=port)
    # 0. check dir and set up connection
    if not os.path.exists(local_dir):
        return f"provided local_dir{local_dir} is not existed"
    if not utility.has_collection(collection_name):
        print(f"{collection_name} not exist, cannot export")

    # 1. set up collection, schema, and fields
    schema = utility.get_collection_schema(collection_name)
    output_fields, output_files = set_up_output_fields_files(schema, local_dir)
    collection = Collection(collection_name)

    # 2. set up iterator
    query_iterator = collection.query_iterator(output_fields=output_fields,
                                               iteration_extension_reduce_rate=DEFAULT_ITERATION_RATE)
    page_idx = 0
    while True:
        res = query_iterator.next()
        if len(res) == 0:
            query_iterator.close()
            break
        for i in range(len(res)):
            write_data_to_file(output_files, res[i])
        page_idx += 1
    print(f"export {page_idx} pages in all")

    for file in output_files.values():
        file.close()
    return 'finish exporting collection, {name}!'.format(name=collection_name)


def set_up_output_fields_files(schema: CollectionSchema, output_dir: str):
    fields = schema.fields
    output_fields = []
    output_files = {}
    for field in fields:
        output_fields.append(field.name)
        output_file_path = output_dir + "/" + field.name
        output_file = open(output_file_path, 'w')
        output_files[field.name] = output_file

    return output_fields, output_files

def write_data_to_file(output_files, row):
    for key in output_files:
        output_files[key].write(str(row[key]) + "\n")


if __name__ == '__main__':
    fire.Fire({
        'bulk': bulk,
        'export': export
    })
