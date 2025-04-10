from fastavro import json_writer, parse_schema, writer
import requests
import json
import os



def save_sales_from_apifile(stg_dir: str, raw_dir: str) -> None:
    schema = {
  "type": "record",
  "name": "PurchaseRecord",
  "namespace": "com.example",
  "fields": [
    {
      "name": "client",
      "type": "string"
    },
    {
      "name": "purchase_date",
      "type": "string"
    },
    {
      "name": "product",
      "type": "string"
    },
    {
      "name": "price",
      "type": "int"
    }
  ]
}

    schemaafterparse = parse_schema(schema)

    files_raw_dir = os.listdir(raw_dir)  # List of file names

    for filename in files_raw_dir:
        file_raw_dir_path = os.path.join(raw_dir, filename)


    with open(r'' + file_raw_dir_path) as js:
        x = json.load(js)




    stg_dir_name = filename.split('.')[0] + '.avro'
    stg_dir_path = os.path.join(stg_dir, stg_dir_name)



    if os.path.exists(stg_dir_path):
        os.remove(stg_dir_path)

    if os.path.exists(stg_dir):
        os.rmdir(stg_dir)


    if not os.path.exists(stg_dir):
        os.mkdir(stg_dir)




    with open(stg_dir_path, 'wb') as out:
        writer(out, schemaafterparse,x)

