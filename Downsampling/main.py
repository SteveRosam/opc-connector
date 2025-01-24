import os
from quixstreams import Application

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="raw-to-table-v5", auto_offset_reset="earliest", use_changelog_topics=True)

input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

sdf.print()

# sdf = sdf.apply(lambda row: row, expand=True)

def expand_values_to_columns(value: float):
    new_row = {}
    
    new_row["value"] = value
    
    return new_row

sdf = sdf.apply(expand_values_to_columns)
sdf.print()

sdf.apply(lambda row: print(row['value']))

sdf = sdf.hopping_window(2000, 250).reduce(lambda state, row: { **state, **row}, lambda row: row).final()

sdf = sdf.apply(lambda row:{
    "timestamp": row["start"],
    **row["value"]
})

sdf = sdf.update(lambda row: print(row))
sdf.print()

# sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)