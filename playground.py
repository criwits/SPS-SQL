db_names = [
    "battle_death",
    "car_1",
    "concert_singer",
    "course_teach",
    "cre_Doc_Template_Mgt",
    "dog_kennels",
    "employee_hire_evaluation",
    "flight_2",
    "museum_visit",
    "network_1",
    "orchestra",
    "pets_1",
    "poker_player",
    "real_estate_properties",
    "singer",
    "student_transcripts_tracking",
    "tvshow",
    "voter_1",
    "world_1",
    "wta_1"
]

import json

from schema import build_db_from_spider

sql_count = dict()

with open("result/generated-spider-dev-256.json") as f:
    data = json.load(f)

for d in data:
    db_id = d["db_id"]
    if db_id not in sql_count:
        sql_count[db_id] = 0
    sql_count[db_id] += 1

with open("data/spider/tables.json") as f:
    tables = json.load(f)

dbs = dict()
for d in tables:
    db = build_db_from_spider(d)
    dbs[db.name] = db

for i in range(10):
    j = i + 10

    name1 = db_names[i]
    
    name2 = db_names[j]

    display_name1 = name1.replace("_", "\\_")
    display_name2 = name2.replace("_", "\\_")


    print(f"{display_name1} & {len(dbs[name1].tables)} & {sql_count[name1]} & {display_name2} & {len(dbs[name2].tables)} & {sql_count[name2]} \\\\")
    
