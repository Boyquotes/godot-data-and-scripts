from neo4j.v1 import GraphDatabase, basic_auth
import os
import csv


driver = GraphDatabase.driver(
    "bolt://localhost", auth=basic_auth("neo4j", os.environ["NEO4J_PASSWORD"]))
session = driver.session()


# add Egyptian month names
input_file = open('indiction_cycles.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (t:Timeline)
    MERGE (t)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type: 'Cycles'})
    MERGE (yrs)-[:hasYearReferenceSystem]->(yrs2:YearReferenceSystem {type: 'Indiction Cycle'})
    MERGE (yrs2)-[:hasCalendarPartial]-(cp_cycle:CalendarPartial {type: 'cycle', value: '%s'})
    MERGE (cp_cycle)-[:hasCalendarPartial]-(cp_year:CalendarPartial {type: 'year', value: '%s'})
    MERGE (cp_year)-[:hasGodotUri]-(g:GODOT {uri: '%s', type:'standard', bot_before:'%s', not_after:'%s'})
    """ % (row[0], row[1], row[4], row[2], row[3])
    print(query)
    session.run(query)

session.close()
