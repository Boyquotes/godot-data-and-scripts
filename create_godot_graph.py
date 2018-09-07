from neo4j.v1 import GraphDatabase, basic_auth
import os
import csv


driver = GraphDatabase.driver(
    "bolt://localhost", auth=basic_auth("neo4j", os.environ["NEO4J_PASSWORD"]))
session = driver.session()

# delete all nodes and relations in database
# query = """
# MATCH (n)
# DETACH DELETE n
# """
# session.run(query)


# create Timeline root node
# query = """
# create (t:Timeline) return t
# """
# session.run(query)

#
# create "None" YRS
# dates with month/day only
#
query = """
MATCH (t:Timeline) 
MERGE (t)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type: 'None'})
MERGE (yrs)-[:hasCalendarType]->(ct:CalendarType {type: 'Egyptian Calendar'})
"""
session.run(query)
# add Egyptian month names
input_file = open('scaffold_none_yrs_egyptian_calendar.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (Timeline)--(YearReferenceSystem {type: 'None'})--(ct:CalendarType {type: 'Egyptian Calendar'})
    MERGE (ct)-[:hasCalendarPartial]-(cp:CalendarPartial {type: 'month', value: '%s'})
    MERGE (cp)-[:hasGodotUri]-(g:GODOT {uri: '%s', type:'standard'})
    """ % (row[0], row[2])
    session.run(query)


#
# create "Unknown" YRS
# year number given, but unknown if regnal or (Actian) era is meant
#
query = """
MATCH (t:Timeline) 
MERGE (t)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type: 'Unknown'})
"""
session.run(query)



#
# create Actian Era YRS
#
input_file = open('scaffold_era_actian.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
    MATCH (t:Timeline) 
    MERGE (t)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type: 'Era'})
    MERGE (yrs)-[:hasYearReferenceSystem]->(yrs_sub:YearReferenceSystem {type:'Actian'})
    MERGE (yrs_sub)-[:hasCalendarPartial]-(cp1:CalendarPartial {type: 'year', value: '%s'})
    MERGE (cp1)-[:hasGodotUri]-(g:GODOT {uri: '%s', type:'standard'})
    """ % (row[0], row[3])
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
    MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Titulature of Roman Emperors'})
    MERGE (yrs)-[:hasYearReferenceSystem]->(yrs_sub:YearReferenceSystem {type:'Tribunicia Potestas'})
    MERGE (yrs_sub)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s', uri:'%s'})
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
    MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Eponymous officials'})
    MERGE (yrs)-[:hasYearReferenceSystem]->(yrs_sub:YearReferenceSystem {type:'Apollo Priest (Cyrenaica)'})
    MERGE (yrs_sub)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:'%s'})
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
        MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Eponymous officials'})
        MERGE (yrs)-[:hasYearReferenceSystem]->(yrs_sub:YearReferenceSystem {type:'Roman Consulships'})
        MERGE (yrs_sub)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:"%s"})
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
input_file = open('scaffold_regnal_years_of_roman_emperors_short.tsv', "r")
reader = csv.reader(input_file, delimiter='\t')
next(reader)  # skip first line
for row in reader:
    query = """
        MATCH (root:Timeline)
        MERGE (root)-[:hasYearReferenceSystem]->(yrs:YearReferenceSystem {type:'Regnal Years'})
        MERGE (yrs)-[:hasYearReferenceSystem]->(yrs_sub:YearReferenceSystem {type:'Roman Emperors'})
        MERGE (yrs_sub)-[:hasCalendarPartial]->(cp1:CalendarPartial {type:'name', value:"%s"})
        MERGE (cp1)-[:hasCalendarPartial]->(cp2:CalendarPartial {type:'year', value:"%s"})
        MERGE (cp2)-[:hasGodotUri]->(g:GODOT {uri:'%s', type:'standard', not_before: '%s', not_after: '%s'})
        """ % (row[0], row[2], row[5], row[3], row[4])
    session.run(query)


#
# create data scaffold for regnal years of Ptolemies (Egypt)
#



#
# add indexes
#
query = "CREATE INDEX ON :YearReferenceSystem(type)"
session.run(query)
query = "CREATE INDEX ON :CalendarType(type)"
session.run(query)

#
# create unique constraint for GODOT URIs
#
query = "CREATE CONSTRAINT ON (g:GODOT) ASSERT g.uri IS UNIQUE"
session.run(query)


session.close()
