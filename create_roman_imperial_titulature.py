from neo4j.v1 import GraphDatabase, basic_auth
import os
import csv


driver = GraphDatabase.driver(
    "bolt://localhost", auth=basic_auth("neo4j", os.environ["NEO4J_PASSWORD"]))
session = driver.session()





#
# create data scaffold for Trib Pot of Roman emperors
#
input_file = open('scaffold_imperial_tribunicia_potestas.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (root:Timeline)
    MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Titulature of Roman Emperors'})
    MERGE (yrs)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s', uri:'%s'})
    MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'Tribunicia Potestas'})
    MERGE (cp2)-[:hasCalendarPartial]->(cp3:CalendarPartial {type:'number', value:'%s'})
    MERGE (cp3)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before:'%s', not_after:'%s'})
    """ % (row[0], row[1], row[2], row[5], row[3], row[4])
    session.run(query)
# add trib pot attestations from ocre (coin types)
input_file = open('attestations_trib_pot_ocre.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (g:GODOT)
    WHERE g.uri='%s'
    MERGE (g)-[:hasAttestation]->(:Attestation {title:'%s', uri:'%s', date_string:'%s'})
    """ % (row[8], row[0], row[1], row[4] + " // " + row[5])
    session.run(query)
# add edh attestations (Latin inscriptions)
input_file = open('attestations_trib_pot_edh.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (g:GODOT)
    WHERE g.uri='%s'
    MERGE (g)-[:hasAttestation]->(:Attestation {title:'%s', uri:'%s', date_string:"%s"})
    """ % (row[11], row[0], 'https://edh-www.adw.uni-heidelberg.de/edh/inschrift/'+row[0], row[9])
    session.run(query)


#
# add Imperial Victory Titles
#
input_file = open('scaffold_imperial_victory_titles.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (root:Timeline)
    MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Titulature of Roman Emperors'})
    MERGE (yrs)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s', uri:'%s'})
    MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'Imperial Victory Titles'})
    MERGE (cp2)-[:hasCalendarPartial]->(cp3:CalendarPartial {type:'title', value:'%s'})
    MERGE (cp3)-[:hasCalendarPartial]->(cp4:CalendarPartial {type:'number', value:'%s'})
    MERGE (cp4)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before:'%s', not_after:'%s', comment:'%s', date_is_uncertain:'%s'})
    """ % (row[0], row[1], row[2], row[3], row[9], row[5], row[6], row[8], row[7])
    session.run(query)


#
# add Imperial Acclamations
#
input_file = open('scaffold_imperial_acclamations.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (root:Timeline)
    MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Titulature of Roman Emperors'})
    MERGE (yrs)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s', uri:'%s'})
    MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'Imperial Acclamations'})
    MERGE (cp2)-[:hasCalendarPartial]->(cp3:CalendarPartial {type:'number', value:'%s'})
    MERGE (cp3)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before:'%s', not_after:'%s', comment:'%s', date_is_uncertain:'%s'})
    """ % (row[0], row[1], row[2], row[7], row[3], row[4], row[6], row[5])
    session.run(query)


#
# add Imperial Consulates
#
input_file = open('scaffold_imperial_consulates.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    # create array for cypher from parts_of_consulates column
    consul_list = []
    for consulship in row[10].split(";"):
        consul_list.append(consulship.strip())
    consul_list_str = str(consul_list)
    query = """
    MATCH(root: Timeline)
    MERGE(root) - [: hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Titulature of Roman Emperors'})
    MERGE(yrs) - [: hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s', uri:'%s'})
    MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'Imperial Consulates'})
    MERGE (cp2)-[:hasCalendarPartial]->(cp3:CalendarPartial {type:'type', value:'%s'})
    MERGE (cp3)-[:hasCalendarPartial]->(cp4:CalendarPartial {type:'number', value:'%s'})
    MERGE (cp4)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before:'%s', not_after:'%s', comment:'%s', date_is_uncertain:'%s', part_of_consulate:%s})

    """ % (row[0], row[1], row[2], row[3], row[9], row[5], row[6], row[8], row[7], consul_list_str)
    session.run(query)

#
# add emperor "unknown"
#
query = """
MATCH(root: Timeline)
MERGE(root) - [: hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Titulature of Roman Emperors'})
MERGE(yrs) - [: hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'unknown'})
"""
session.run(query)


session.close()
