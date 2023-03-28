from prefect.blocks.system import JSON

json_block = JSON.load("excel-sheet-schema")

print(json_block.value["sheet_0_columns"])