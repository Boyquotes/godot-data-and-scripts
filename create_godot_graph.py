from neo4j.v1 import GraphDatabase, basic_auth
import os
import csv


driver = GraphDatabase.driver(
    "bolt://localhost", auth=basic_auth("neo4j", os.environ["NEO4J_PASSWORD"]))
session = driver.session()

# delete all nodes and relations in database
query = """
MATCH (n)
DETACH DELETE n
"""
session.run(query)


# create Timeline root node
query = """
create (t:Timeline) return t
"""
session.run(query)

#
# create data scaffold for Trib Pot of Roman emperors
#
input_file = open('scaffold_tribunicia_potestas_roman_emperors.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (root:Timeline)
    MERGE (root)-[:hasYearReckoningSystem]->(yrs:YearReckoningSystem {type:'Titulature of Roman Emperors: Tribunicia Potestas'})
    MERGE (yrs)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'reign', value:'%s', uri:'%s'})
    MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'number', value:'%s'})
    MERGE (cp2)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before:'%s', not_after:'%s'})
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
# create data scaffold for Apollo priest Cyrenaica
#
input_file = open('scaffold_eponymous_apollo_priests_cyrenaica.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    if row[1] != "":
        not_before = ", not_before:'" + row[1] + "'"
    if row[2] != "":
        not_after = ", not_after:'" + row[2] + "'"

    query = """
    MATCH (root:Timeline)
    MERGE (root)-[:hasYearReckoningSystem]->(yrs:YearReckoningSystem {type:'Eponymous officials: Apollo Priest (Cyrenaica)'})
    MERGE (yrs)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s'})
    MERGE (cp1)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard'
    """ % (row[0], row[5])
    query += not_before + not_after
    query += "})"
    session.run(query)

#
# add data scaffold for Roman consulates
#
input_file = open('scaffold_roman_consulates.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
        MATCH (root:Timeline)
        MERGE (root)-[:hasYearReckoningSystem]->(yrs:YearReckoningSystem {type:'Eponymous officials: Roman Consulships'})
        MERGE (yrs)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'names', value:"%s"})
        MERGE (cp1)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before: '%s', not_after: '%s'})
        """ % (row[9], row[8], row[0], row[0])
    session.run(query)
# add edh attestations (Latin inscriptions)
input_file = open('attestations_edh_consulibus.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
        MATCH (g:GODOT)
        WHERE g.uri='%s'
        MERGE (g)-[:hasAttestation]->(:Attestation {title:'%s', uri:'%s', date_string:"%s"})
        """ % (row[3], row[0], 'https://edh-www.adw.uni-heidelberg.de/edh/inschrift/' + row[0], row[2])
    session.run(query)

#
# create data scaffold for regnal years Roman emperors
#


#
# create data scaffold for regnal years of Ptolemies (Egypt)
#


session.close()
