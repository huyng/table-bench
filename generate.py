import datapad as dp
import random

random.seed(0)

headers = ["name", "city", "age", "is_member", "last_arrival_day"]

def make_row():

    name = random.choice(["dana", "jose", "simao", "xiao", "jason", "yong", "huy", "thanh", "darius", "tuan", "jasmine", "vijay", "seung", "jin", "anav", "preeti", "penny","dustin" ]),
    city = random.choice(["shanghai", "new york", "ho chi minh city", "taipei", "paris", "new orleans", "san francisco", "los angeles", "nghe an", "mexico city", "madrid", "lisbon", "singapore"]),
    age = random.choice(list(range(16, 80))),
    is_member = random.choice([0, 1]),
    last_arrival_day = [random.choice(range(1990, 2024)), random.choice(range(1,13)), random.choice(range(1,32))]
    last_arrival_day = "-".join([str(v) for v in last_arrival_day])

    return [
        name[0],
        city[0],
        age[0],
        is_member[0],
        last_arrival_day
    ]



print("Generating data")
rows = dp.Sequence(range(800_000))\
        .map(lambda v: make_row())\
        .collect()

json_sink = dp.io.JsonSink("table.jsonl")
text_sink = dp.io.TextSink("table.csv")

print("Writing table.jsonl")
seq = dp.Sequence(rows)\
        .map(lambda v: dict(zip(headers, v)))\
        .progress()\
        .dump(json_sink)


print("Writing table.csv")
seq = dp.Sequence([headers])\
        .concat(dp.Sequence(rows))\
        .map(lambda row: ",".join([str(c) for c in row]))\
        .progress()\
        .dump(text_sink)
