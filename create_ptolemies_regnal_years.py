from neo4j.v1 import GraphDatabase, basic_auth
import os
import csv

driver = GraphDatabase.driver(
    "bolt://localhost", auth=basic_auth("neo4j", os.environ["NEO4J_PASSWORD"]))
session = driver.session()

#
# create data scaffold for regnal years of Ptolemies
#
input_file = open('scaffold_ptolemies_regnal_years.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (root:Timeline)
    MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Regnal Years'})
    MERGE (yrs)-[:hasYearReferenceSystem]->(yrs2:YearReferenceSystem {type:'Ptolemies'})
    MERGE (yrs2)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s', uri:'%s'})
    MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'year', value:'%s'})
    MERGE (cp2)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before:'%s', not_after:'%s'})
    """ % (row[0], row[4], row[1], row[5], row[2], row[3])
    session.run(query)
session.close()
