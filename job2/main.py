import os
from flask import Flask, request
from flask import typing as flask_typing
from Lecture_2.ht_template.job2.JSON_to_AVRO import save_sales_from_apifile


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    input_data: dict = request.json[0]
    # TODO: implement me
    stg_dir = input_data.get('stg_dir')
    raw_dir = input_data.get('raw_dir')

    save_sales_from_apifile(stg_dir, raw_dir)



    if save_sales_from_apifile(stg_dir, raw_dir) is False:
        return {
            "message": "No data found for this date"
        }, 400
    else:
        return {
        "message": "Data retrieved successfully in AVRO from raw_dir",
    }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
