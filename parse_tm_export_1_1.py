import os
import csv
import unicodedata
import shortuuid
import re
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver(
    "bolt://localhost", auth=basic_auth("neo4j", os.environ["NEO4J_PASSWORD"]))
session = driver.session()

months_egyptian = (
    'Thoth',
    'Phaophi',
    'Hathyr',
    'Choiak',
    'Tybi',
    'Mecheir',
    'Phamenoth',
    'Pharmouthi',
    'Pachons',
    'Pauni',
    'Epeiph',
    'Mesore',
    'Epagomenai',
    'month epagomenai',
    'Sebastos',
    'Germanikeios',
    'Neos Sebastos',
    'Neroneios',
    'Hadrianos',
    'Gaieios',
    'Drousilleios',
    'Drousieus',
    'Kaisareios',
    'Sotereios',
)

months_macedonian = (
    'Dios',
    'Apellaios',
    'Audnaios',
    'Peritios',
    'Dystros',
    'Xandikos',
    'Artemisios',
    'Daisios',
    'Panemos',
    'Loios',
    'Gorpiaios',
    'Hyperberetaios',
)

months_roman = (
    'Ianuarius',
    'Februarius',
    'Martius',
    'Aprilis',
    'Maius',
    'Iunius',
    'Iulius',
    'Augustus',
    'September',
    'October',
    'November',
    'December',
    'Quintilis',
    'Sextilis',
)

# translation table Roman Emperors
# Trismegistos : GODOT
roman_emperors_translation_list = {
    "Augustus" : "Augustus",
    "Tiberius" : "Tiberius",
    "Caligula" : "Caligula",
    "Claudius" : "Claudius",
    "Nero" : "Nero",
    "Galba" : "Galba",
    "Otho" : "Otho",
    "Vitellius" : "Vitellius",
    "Vespasianus" : "Vespasian",
    "Titus" : "Titus",
    "Domitianus" : "Domitian",
    "Nerva" : "Nerva",
    "Traianus" : "Trajan",
    "Hadrianus" : "Hadrian",
    "Antoninus Pius" : "Antoninus Pius",
    "Marcus Aurelius" : "Marc Aurel",
    "Commodus" : "Commodus",
    "Pertinax" : "Pertinax",
    "Didius Iulianus" : "Didius Iulianus",
    "Pescennius Niger" : "Pescennius Niger",
    "Septimius Severus" : "Septimius Severus",
    "Caracalla" : "Caracalla",
    "Septimius Geta" : "Geta",
    "Macrinus" : "Macrinus",
    "Elagabalus" : "Elagabal",
    "Severus Alexander" : "Severus Alexander",
    "Maximinus Thrax" : "Maximinus Thrax",
    "Gordianus I" : "Gordian I",
    "Gordianus II" : "Gordian II",
    "Balbinus" : "Balbinus",
    "Gordianus III" : "Gordian III",
    "Pupienus" : "Pupienus",
    "Philippus Arabs" : "Philippus Arabs",
    "Decius" : "Decius",
    "Hostilianus" : "Hostilianus",
    "Volusianus" : "Volusianus",
    "Trebonianus Gallus" : "Trebonianus Gallus",
    "Aemilius Aemilianus" : "Aemilius Aemilianus",
    "Valerianus" : "Valerian",
    "Gallienus" : "Gallienus",
    "Macrianus" : "Macrianus",
    "Quietus" : "Quietus",
    "Claudius II Gothicus" : "Claudius II Gothicus",
    "Quintillus" : "Quintillus",
    "Aurelianus" : "Aurelianus",
    "Vallabathus" : "",
    "Tacitus" : "Tacitus",
    "Probus" : "Probus",
    "Carus" : "Carus",
    "Carinus" : "Carinus",
    "Numerianus" : "Numerianus",
    "Diocletianus" : "Diocletianus",
    "Maximianus" : "Maximian",
    "Galerius Maximianus" : "",
    "Constantius I" : "Constantius I",
    "Domitius Domitianus" : "",
    "Maximinus Daia" : "",
    "Severus" : "",
    "Constantinus I" : "Constantin I",
    "Licinius" : "Licinius",
    "Constantinus II" : "",
    "Crispus" : "",
    "Licinius II" : "",
    "Constantius II" : "Constantius II",
    "Constans" : "",
    "Dalmatius" : "",
    "Gallus" : "",
    "Iulianus" : "",
    "Iovianus" : "",
    "Valens" : "Valens",
    "Valentinianus I" : "Valentinian I",
    "Gratianus" : "Gratian",
    "Valentinianus II" : "",
    "Theodosius I" : "",
    "Arcadius" : "",
    "Honorius" : "",
    "Theodosius II" : "",
    "Constantius III" : "",
    "Valentinianus III" : "",
    "Marcianus" : "",
    "Leo I" : "",
    "Leo II" : "",
    "Zeno" : "",
    "Anastasius" : "",
    "Iustinus I" : "",
    "Iustinianus" : "",
    "Iustinus II" : "",
    "Tiberius II" : "",
    "Mauricius" : "",
    "Phocas" : "",
    "Heraclius" : "",
    "Heraclius Novus Constantinus" : "",
}

consul_per_cluster_id = {
    "2708" : "https://godot.date/id/2bfYfJSjGbTtrXfNnLQjFL",
    "2459" : "https://godot.date/id/3fn9GXkJF5wtb5eLhFSvfX",
    "2455" : "https://godot.date/id/66yLVjHU4G2Y793cvbVyFS",
    "2453" : "https://godot.date/id/7qgApuHjWVv6FYbsfvKhQN",
    "3409" : "https://godot.date/id/9Xj9CPyQxVQSnf5EkHMhu5",
    "1227" : "https://godot.date/id/aPSyqJuVdjRiLrCxKojqdk",
    "2468" : "https://godot.date/id/c4GqWMZXuH9HwgRpg92Nje",
    "2475" : "https://godot.date/id/c4GqWMZXuH9HwgRpg92Nje",
    "2786" : "https://godot.date/id/CeAK2K53qqLh9tZrua5TpT",
    "2469" : "https://godot.date/id/CoR3obRex6NCfqg7ccgPjK",
    "2457" : "https://godot.date/id/CRMY5s3ba5xhyr4V8Mwowd",
    "3214" : "https://godot.date/id/cRYyky2mvYdmzfKGSJxJq3",
    "2564" : "https://godot.date/id/drcVYZCLcGB5yyB9Z2UgE5",
    "2485" : "https://godot.date/id/dUjHxPPbLcJWeJen3GaQrT",
    "2568" : "https://godot.date/id/dUjHxPPbLcJWeJen3GaQrT",
    "2758" : "https://godot.date/id/dUjHxPPbLcJWeJen3GaQrT",
    "3420" : "https://godot.date/id/dUjHxPPbLcJWeJen3GaQrT",
    "2484" : "https://godot.date/id/dwuekVzSoeMRpcri9rVD6J",
    "3411" : "https://godot.date/id/EC6W6eKSWkJXBEej2mJLyT",
    "1949" : "https://godot.date/id/Fqhsg82iBLh4NoiVveZw2L",
    "2464" : "https://godot.date/id/GGfPAqB4zsJLUtEXmy2fk9",
    "2714" : "https://godot.date/id/GhxwBqiCkZF7LeZVB9aupN",
    "3388" : "https://godot.date/id/GhxwBqiCkZF7LeZVB9aupN",
    "3390" : "https://godot.date/id/GhxwBqiCkZF7LeZVB9aupN",
    "2467" : "https://godot.date/id/gNsV3WnDFEnmrRYJTJHtrH",
    "3245" : "https://godot.date/id/h5SdUFWBXEGoVMJaDkQoDT",
    "3408" : "https://godot.date/id/h5SdUFWBXEGoVMJaDkQoDT",
    "2795" : "https://godot.date/id/hkiHnTsykSB8RoHVWtLdRX",
    "2763" : "https://godot.date/id/HZ6FZMQkkhnJj3Rrk3EToi",
    "2471" : "https://godot.date/id/iv64PLCH2mBdLYBCT4NjrC",
    "2472" : "https://godot.date/id/iv64PLCH2mBdLYBCT4NjrC",
    "3295" : "https://godot.date/id/jjsHCYDgFRAAGEnh2oBxSN",
    "2454" : "https://godot.date/id/JKwQMJkL7S8mqRV5afJ2YA",
    "3272" : "https://godot.date/id/MavimxMsD4Xqb8KTbfjbcV",
    "3273" : "https://godot.date/id/MavimxMsD4Xqb8KTbfjbcV",
    "3278" : "https://godot.date/id/MavimxMsD4Xqb8KTbfjbcV",
    "1938" : "https://godot.date/id/McuvVgBBxANSeDcK3K7WAf",
    "2474" : "https://godot.date/id/mdfBNiezFtNgftbYnMVAgN",
    "1128" : "https://godot.date/id/mMd9Uq3E2kiaBctpNZERkF",
    "1224" : "https://godot.date/id/mMd9Uq3E2kiaBctpNZERkF",
    "1225" : "https://godot.date/id/mMd9Uq3E2kiaBctpNZERkF",
    "2473" : "https://godot.date/id/mWEPHF52r3AejWqKs5cN8n",
    "3407" : "https://godot.date/id/mWEPHF52r3AejWqKs5cN8n",
    "2704" : "https://godot.date/id/ne3MLuw93k9bAXeVFXaEU7",
    "2479" : "https://godot.date/id/NkpmXQN8C5g5Bs7EBXsWCL",
    "2478" : "https://godot.date/id/Np2BmHPtTTnmYKwVKuciP2",
    "2043" : "https://godot.date/id/q3LSZdPKtKNgmYeK6FuMGc",
    "1223" : "https://godot.date/id/qHcoQNo4tB8ENcTy3LHLBc",
    "1553" : "https://godot.date/id/qHcoQNo4tB8ENcTy3LHLBc",
    "2429" : "https://godot.date/id/qHcoQNo4tB8ENcTy3LHLBc",
    "2773" : "https://godot.date/id/qn6kJhmpQjRpBqCWwS3AjG",
    "2712" : "https://godot.date/id/qV6mAcEfkuCu8CGGTCwvS6",
    "1497" : "https://godot.date/id/qyMUmtzTYeSWyr66hWwYdb",
    "2713" : "https://godot.date/id/spuk2pGLY4YD4xJXjk9Ca7",
    "2461" : "https://godot.date/id/St6FKWhNwqmBYhC7PBigfk",
    "2477" : "https://godot.date/id/sTL3PzgXgKkhmyDpjg4Hvn",
    "1345" : "https://godot.date/id/T2KdW4bFdeWrDcoLnA5c2Y",
    "2709" : "https://godot.date/id/t96eKkSqCLubqg39KFUkxg",
    "2114" : "https://godot.date/id/T97xgn7w8wBghg75oaFn2B",
    "2456" : "https://godot.date/id/tHgkwbwUo2nbUrdBJZWpcj",
    "1587" : "https://godot.date/id/tMEWuCmn7TqfFFjNtLVWxX",
    "1092" : "https://godot.date/id/tQCMtSXMBDVwWyem5i8oma",
    "3257" : "https://godot.date/id/TQf7vsmj5MEienztyoWGo2",
    "3415" : "https://godot.date/id/TQf7vsmj5MEienztyoWGo2",
    "2482" : "https://godot.date/id/UCjnWvgfc862mik5qCcgNH",
    "3277" : "https://godot.date/id/uYwLjEFh3QNQ4wx9wtnBVP",
    "2462" : "https://godot.date/id/vdPYxPyHVSxG8vVEvw6RQV",
    "2690" : "https://godot.date/id/w5fxMfwAo5wSJHAAr8ZtUe",
    "2692" : "https://godot.date/id/w5fxMfwAo5wSJHAAr8ZtUe",
    "2480" : "https://godot.date/id/wM3oBQnEJWFReAMUgwQLRb",
    "2465" : "https://godot.date/id/wnVyygCTEkToMP4VZSBQcU",
    "2470" : "https://godot.date/id/WSyX976K3bAdfCdtAjUqaM",
    "3422" : "https://godot.date/id/WSyX976K3bAdfCdtAjUqaM",
    "2717" : "https://godot.date/id/WU29FX7g3WUjwCx5dToxgH",
    "2720" : "https://godot.date/id/WyxhseyTgcbQxhj2F3wtC3",
    "3223" : "https://godot.date/id/xAocAK7wBaNzntm2TyNbJ7",
    "1106" : "https://godot.date/id/XqvtQAV8mYBkwUW4PZLKcX",
    "2481" : "https://godot.date/id/ybff2338HGrmaZAhWBekJ3",
    "3416" : "https://godot.date/id/ybff2338HGrmaZAhWBekJ3",
    "2483" : "https://godot.date/id/ygB7PsqqwNk7JwNWL9uaT9",
    "2521" : "https://godot.date/id/ygB7PsqqwNk7JwNWL9uaT9",
    "2523" : "https://godot.date/id/ygB7PsqqwNk7JwNWL9uaT9",
    "1625" : "https://godot.date/id/z7eZ6RyvRKATA8c4inL76D",
    "2781" : "https://godot.date/id/zZggdqBRkqbb5DbSnGgSHd",
}


def get_iso_like_dates_from_string(date_string):
    """
    converts date string into list of (up to 2) ISO like dates
    :param date_string:
    :return: list of ISO like dates
    """
    # first split at hyphen
    result_dates_list = []
    dates_list = date_string.split("-")
    for d in dates_list:
        date_elements = d.split()
        bc_date = 1
        for elem in date_elements:
            if elem == "BC":
                bc_date = -1


def create_line_span_string(lines):
    """
    converts cluster line numbers into line string like e.g. "3-6"
    instead of having 3 3 3 4 4 5 5 5 6 6 6
    :param lines:
    :return: string with line information
    """
    line_number_list = lines.split()
    first_line_number = line_number_list[0]
    last_line_number = line_number_list[-1]
    if "-" in first_line_number:
        first_line_number = first_line_number.split("-")[0]
    if "-" in last_line_number:
        last_line_number = last_line_number.split("-")[-1]
    line_number_string = ""
    if first_line_number == last_line_number:
        line_number_string = first_line_number
    else:
        line_number_string = first_line_number + "-" + last_line_number
    return line_number_string


def get_data_from_cluster_data_by_tm_id(cluster_data, tm_id):
    data_list = []
    for cluster_id in cluster_data:
        if cluster_data[cluster_id][0] == tm_id:
            data_list.append(cluster_data[cluster_id])
    return data_list


def is_simple_date(data_dict):
    # test if no key has double/triple values
    is_simple_date = True
    for key in data_dict:
        if key != "consul" and len(data_dict[key]) > 1:
            is_simple_date = False
    return is_simple_date


def get_number_of_yrs(data_dict):
    number_of_yrs = 0
    if "consul" in data_dict:
        number_of_yrs += 1
    if "king" in data_dict:
        number_of_yrs += 1
    if "indictio" in data_dict:
        number_of_yrs += 1
    return number_of_yrs


def get_calendar_type_by_month_name(month_name):
    """
    returns calendar type based on month name
    :param month_name: string of month name
    :return: string [Egyptian|Macedonian|Roman|Unknown]
    """
    if month_name in months_egyptian:
        return "Egyptian Calendar"
    elif month_name in months_macedonian:
        return "Macedonian Calendar"
    elif month_name in months_roman:
        return "Roman Calendar"
    else:
        return "Unknown"


def _get_attestation_title(cluster_id, title, cluster_data):
    title = "BGU " + title[4:-4]
    title = title.replace(".", " ")
    title = title.replace("_", " ")
    title += ", l. " + cluster_data[cluster_id][2]
    return title


def is_roman_emperor(king):
    # check im king was Roman emperor
    emperor_name = ""
    try:
        emperor_name = roman_emperors_translation_list[king]
        if emperor_name != "":
            return emperor_name
        else:
            print("#### no Roman Emperor: ", king)
            return False
    except:
        print("#### no Roman Emperor: ", king)
        return False


def create_cypher_for_regnal_years(data, cluster_data):
    query = ""
    cluster_id = data['cluster_id'][0]
    calendar_type = ""
    month = ""
    year = ""
    day = ""
    godot_uri = "https://godot.date/id/" + shortuuid.uuid()
    attestation_title = _get_attestation_title(cluster_id, cluster_data[cluster_id][1], cluster_data)
    date_string = cluster_data[cluster_id][5]
    date_string = re.sub(r'\|.*?\|', '', date_string)
    date_string = re.sub(r'\*', '', date_string)
    date_string = re.sub(r'\\', '', date_string)
    if "year" in data:
        year = data['year'][0]
    if "month" in data:
        month = data['month'][0]
    if month:
        calendar_type = get_calendar_type_by_month_name(month)
    else:
        calendar_type = "Unknown"
    if "day" in data:
        day = data['day'][0]

    # first identify Ptolemaic vs. Roman Emperor
    if "king" in data and (data['king'][0].startswith("Ptolem") or data['king'][0].startswith("Bereni") or data['king'][0].startswith("Cleopatra")):
        query = """
        match (yrs:YearReferenceSystem {type:'Regnal Years'})--(yrs2:YearReferenceSystem {type:'Ptolemies'})
        match (yrs2)--(cp:CalendarPartial {value:'%s'})
        """ % data['king'][0]
        last_node = "cp"
    elif "king" in data and is_roman_emperor(data['king'][0]):
        query = """
                match (yrs:YearReferenceSystem {type:'Regnal Years'})--(yrs2:YearReferenceSystem {type:'Roman Emperors'})
                match (yrs2)--(cp:CalendarPartial {value:'%s'})
                """ % roman_emperors_translation_list[data['king'][0]]
        last_node = "cp"

    if query != "":
        if year != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_year:CalendarPartial {type:'year', value:'%s'})
            """ % (last_node, year)
            last_node = "cp_year"
        if month != "" or day != "":
            # first add calendartype
            query += """merge (%s)-[:hasCalendarType]->(ct:CalendarType {type:'%s'})
            """ % (last_node, calendar_type)
            last_node = "ct"
            if month != "":
                query += """merge (%s)-[:hasCalendarPartial]->(cp_month:CalendarPartial {type:'month', value:'%s'})
                """ % (last_node, month)
                last_node = "cp_month"
            if day != "":
                query += """merge (%s)-[:hasCalendarPartial]->(cp_day:CalendarPartial {type:'day', value:'%s'})
                """ % (last_node, day)
                last_node = "cp_day"
        # add godot node
        query += """merge (%s)-[:hasGodotUri]->(g:GODOT)
            on create set g.uri = '%s'
            set g.type = 'standard'
        """ % (last_node, godot_uri)
        last_node = "g"

        # add attestation node
        query += """merge (%s)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
                        set a.last_update = date(),
                            a.username = '%s',
                            a.date_string = '%s'
                    """ % (
            last_node, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title,
            "Trismegistos (Depauw/Verreth)", date_string
            )
        query += " return g.uri as godot_uri"
    return query


def create_cypher_for_consul_dating(data, cluster_data):
    query = ""
    cluster_id = data['cluster_id'][0]
    calendar_type = ""
    month = ""
    year = ""
    day = ""
    godot_uri = "https://godot.date/id/" + shortuuid.uuid()
    attestation_title = _get_attestation_title(cluster_id, cluster_data[cluster_id][1], cluster_data)
    date_string = cluster_data[cluster_id][5]
    date_string = re.sub(r'\|.*?\|', '', date_string)
    date_string = re.sub(r'\*', '', date_string)
    date_string = re.sub(r'\\', '', date_string)
    if "month" in data:
        month = data['month'][0]
    if month:
        calendar_type = get_calendar_type_by_month_name(month)
    else:
        calendar_type = "Unknown"
    if "day" in data:
        day = data['day'][0]

    # get GODOT URI for consulate of this cluster id
    if cluster_id in consul_per_cluster_id:
        godot_uri_consulate = consul_per_cluster_id[cluster_id]
        query = """
        match (g:GODOT {uri:'%s'})
        """ % (godot_uri_consulate)
        last_node = "g"
        if month != "" or day != "":
            # vom übergeordneten cp Knoten ausgehen
            query += """match (cp_consulate:CalendarPartial)-->(%s)
            merge (cp_consulate)-[:hasCalendarType]->(ct:CalendarType {type:'%s'})
            """ % (last_node, calendar_type)
            last_node = "ct"
            if month != "":
                query += """
                merge (%s)-[:hasCalendarPartial]->(cp_month:CalendarPartial {type:'month', value:'%s'})
                """ % (last_node, month)
                last_node = "cp_month"
            if day != "":
                query += """merge (%s)-[:hasCalendarPartial]->(cp_day:CalendarPartial {type:'day', value:'%s'})
                """ % (last_node, day)
                last_node = "cp_day"
            # add godot node
            query += """merge (%s)-[:hasGodotUri]->(g2:GODOT)
                    on create set g2.uri = '%s'
                    set g2.type = 'standard'
                """ % (last_node, godot_uri)
            last_node = "g2"

            # add attestation node
            query += """merge (%s)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
                                    set a.last_update = date(),
                                        a.username = '%s',
                                        a.date_string = '%s'
                                """ % (
                last_node, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title,
                "Trismegistos (Depauw/Verreth)", date_string
            )


        else:
            # only consulate => attach attestation node to godot node
            query += """merge (%s)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
                                    set a.last_update = date(),
                                        a.username = '%s',
                                        a.date_string = '%s'
                                """ % (
                last_node, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title,
                "Trismegistos (Depauw/Verreth)", date_string
            )
        query += " return g.uri as godot_uri"
    return query


def create_cypher_for_indiction_year(data, cluster_data):
    query = ""
    cluster_id = data['cluster_id'][0]
    calendar_type = ""
    month = ""
    day = ""
    godot_uri = "https://godot.date/id/" + shortuuid.uuid()
    attestation_title = _get_attestation_title(cluster_id, cluster_data[cluster_id][1], cluster_data)
    date_string = cluster_data[cluster_id][5]
    date_string = re.sub(r'\*', '', date_string)
    date_string = re.sub(r'\\', '', date_string)
    if "month" in data:
        month = data['month'][0]
    if month:
        calendar_type = get_calendar_type_by_month_name(month)
    else:
        calendar_type = "Unknown"
    if "day" in data:
        day = data['day'][0]
    query = """
    match (yrs:YearReferenceSystem {type:'Cycles'})--(yrs2:YearReferenceSystem {type:'Indiction Cycle'})
    merge (yrs2)-[:hasCalendarPartial]->(cp:CalendarPartial {value:'%s'})
    """ % data['indictio'][0]
    last_node = "cp"

    # add month / day if neccessary
    if month != "" or day != "":
        # first add calendartype
        query += """merge (%s)-[:hasCalendarType]->(ct:CalendarType {type:'%s'})
        """ % (last_node, calendar_type)
        last_node = "ct"
        if month != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_month:CalendarPartial {type:'month', value:'%s'})
            """ % (last_node, month)
            last_node = "cp_month"
        if day != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_day:CalendarPartial {type:'day', value:'%s'})
            """ % (last_node, day)
            last_node = "cp_day"

    # add godot node
    query += """merge (%s)-[:hasGodotUri]->(g:GODOT)
        on create set g.uri = '%s'
        set g.type = 'standard'
    """ % (last_node, godot_uri)
    last_node = "g"

    # add attestation node
    query += """merge (%s)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
                            set a.last_update = date(),
                                a.username = '%s',
                                a.date_string = '%s'
                        """ % (
        last_node, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title,
        "Trismegistos (Depauw/Verreth)", date_string
    )

    query += " return g.uri as godot_uri"
    return query



def create_cypher_for_none_year(data, cluster_data):
    query = ""
    cluster_id = data['cluster_id'][0]
    calendar_type = ""
    year = ""
    month = ""
    day = ""

    godot_uri = "https://godot.date/id/" + shortuuid.uuid()
    attestation_title = _get_attestation_title(cluster_id, cluster_data[cluster_id][1], cluster_data)
    date_string = cluster_data[cluster_id][5]
    date_string = re.sub(r'\*', '', date_string)
    date_string = re.sub(r'\\', '', date_string)
    if "month" in data:
        month = data['month'][0]
    if month:
        calendar_type = get_calendar_type_by_month_name(month)
    else:
        calendar_type = "Unknown"
    if "day" in data:
        day = data['day'][0]
    if "year" in data:
        year = data['year'][0]
    # start creating query
    query = """
        match (yrs:YearReferenceSystem {type:'Unknown'})
        merge (yrs)-[:hasCalendarPartial]->(cp:CalendarPartial {type:'year', value:'%s'})
        """ % year
    last_node = "cp"

    # add month / day if neccessary
    if month != "" or day != "":
        # first add calendartype
        query += """merge (%s)-[:hasCalendarType]->(ct:CalendarType {type:'%s'})
            """ % (last_node, calendar_type)
        last_node = "ct"
        if month != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_month:CalendarPartial {type:'month', value:'%s'})
                """ % (last_node, month)
            last_node = "cp_month"
        if day != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_day:CalendarPartial {type:'day', value:'%s'})
                """ % (last_node, day)
            last_node = "cp_day"

    # add godot node
    query += """merge (%s)-[:hasGodotUri]->(g:GODOT)
            on create set g.uri = '%s'
            set g.type = 'standard'
        """ % (last_node, godot_uri)
    last_node = "g"

    # add attestation node
    query += """merge (%s)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
                            set a.last_update = date(),
                                a.username = '%s',
                                a.date_string = '%s'
                        """ % (
        last_node, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title,
        "Trismegistos (Depauw/Verreth)", date_string
    )

    query += " return g.uri as godot_uri"
    return query


def create_cypher_for_none_yrs(data, cluster_data):
    query = ""
    cluster_id = data['cluster_id'][0]
    calendar_type = ""
    year = ""
    month = ""
    day = ""

    godot_uri = "https://godot.date/id/" + shortuuid.uuid()
    attestation_title = _get_attestation_title(cluster_id, cluster_data[cluster_id][1], cluster_data)
    date_string = cluster_data[cluster_id][5]
    date_string = re.sub(r'\*', '', date_string)
    date_string = re.sub(r'\\', '', date_string)
    if "month" in data:
        month = data['month'][0]
    if month:
        calendar_type = get_calendar_type_by_month_name(month)
    else:
        calendar_type = "Unknown"
    if month == "month epagomenai":
        month = "Epagomenal Days"
    if month == "":
        month = "None"
    if "day" in data:
        day = data['day'][0]

    # start creating query
    query = "match (yrs:YearReferenceSystem {type:'None'})"
    last_node = "yrs"

    # add month / day if neccessary
    if month != "" or day != "":
        # first add calendartype
        query += """merge (%s)-[:hasCalendarType]->(ct:CalendarType {type:'%s'})
                """ % (last_node, calendar_type)
        last_node = "ct"
        if month != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_month:CalendarPartial {type:'month', value:'%s'})
                    """ % (last_node, month)
            last_node = "cp_month"
        if day != "":
            query += """merge (%s)-[:hasCalendarPartial]->(cp_day:CalendarPartial {type:'day', value:'%s'})
                    """ % (last_node, day)
            last_node = "cp_day"

    # add godot node
    query += """merge (%s)-[:hasGodotUri]->(g:GODOT)
                on create set g.uri = '%s'
                set g.type = 'standard'
            """ % (last_node, godot_uri)
    last_node = "g"

    # add attestation node
    query += """merge (%s)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
                            set a.last_update = date(),
                                a.username = '%s',
                                a.date_string = '%s'
                        """ % (
        last_node, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title,
        "Trismegistos (Depauw/Verreth)", date_string
    )
    query += " return g.uri as godot_uri"
    return query


def get_synchron_godot_uri(godot_uri_list, data, cluster_data):
    godot_uri = "https://godot.date/id/" + shortuuid.uuid()
    cluster_id = data['cluster_id'][0]
    date_string = cluster_data[cluster_id][5]
    date_string = re.sub(r'\*', '', date_string)
    date_string = re.sub(r'\\', '', date_string)
    attestation_title = _get_attestation_title(cluster_id, cluster_data[cluster_id][1], cluster_data)
    query = """
    match (g_1:GODOT {uri:'%s'}), (g_2:GODOT {uri:'%s'}) 
    merge (g_1)-[:hasGodotUri]->(g_super:GODOT {type:'synchron', uri:'%s'})
    merge (g_2)-[:hasGodotUri]->(g_super)
    merge (g_super)-[:hasAttestation]->(a:Attestation {uri:'%s', title: '%s'})
    set a.last_update = date(),
        a.username = '%s',
        a.date_string = '%s'
    return g_super.uri as godot_super_uri
    """ % (godot_uri_list[0], godot_uri_list[1], godot_uri, "https://www.trismegistos.org/text/" + cluster_data[cluster_id][0], attestation_title, "Trismegistos (Depauw/Verreth)", date_string)

    return query


def main():
    data_file = open('godot_tm_export_2018_11_19_2.tsv', "r")
    concordance_tm_cluster_id_godot_uri = open("concordance_tm_cluster_id_godot_uri.tsv", "w")
    todo_list = open("todo.tsv", "w")
    reader = csv.reader(data_file, delimiter='\t')
    #next(reader)  # skip first line
    # columns:
    # - cluster_id
    # - tm_id
    # - text_identifier (human-readable)
    # - lines
    # - chunked Greek orginal_source
    # - chunked cluster information interpreted (English)
    # - source text edition
    #
    #


    cluster_data = {} # all data for each cluster; cluster_id as keys
    tm_id_cluster_dict = {} # for each tm_id a list of all cluster_ids; tm_id as keys

    # load tsv data into cluster_data dictionary
    for row in reader:
        cluster_data[row[0]] = [row[1], row[2], create_line_span_string(row[3]), row[4], unicodedata.normalize("NFKD",row[5]), row[6]]

    parsed_data_list = []
    for cluster in cluster_data:
        interpreted_cluster_data_list = cluster_data[cluster][4].split("|")
        tmp_dict = {}
        tmp_dict['cluster_id'] = [cluster]
        for d in interpreted_cluster_data_list:
            # load this into dict of lists; split by whitespace/colon
            if " " in d.strip() and ":" in d.strip():
                (k, v) = (d.strip().split(":"))
                if k in tmp_dict:
                    # key already exists, add to list
                    tmp_dict[k].append(v.strip())
                else:
                    # new key, create
                    tmp_dict[k] = [v.strip()]
        parsed_data_list.append(tmp_dict)

    cnt = 1
    for godot_date in parsed_data_list:
        print("###",godot_date, get_number_of_yrs(godot_date))
        query = ""
        if is_simple_date(godot_date) and get_number_of_yrs(godot_date) == 1:
            if "year" in godot_date or "king" in godot_date:
                # regnal year
                print(cnt, "regnal")
                query = create_cypher_for_regnal_years(godot_date, cluster_data)
                if query == "":
                    print("no cypher error", cnt, godot_date)
                cnt += 1
            elif "indictio" in godot_date:
                print(cnt, "indictio")
                if "year" not in godot_date:
                    query = create_cypher_for_indiction_year(godot_date, cluster_data)
                    if query == "":
                        print("no cypher error", cnt, godot_date)
                cnt += 1
            elif "consul" in godot_date:
                print(cnt, "consul")
                if "year" not in godot_date:
                    query = create_cypher_for_consul_dating(godot_date, cluster_data)
                    if query == "":
                        print("no cypher error", cnt, godot_date)
                cnt += 1
            else:
                # no YRS identified
                print("&&&&&&&&", godot_date)
                pass
        elif is_simple_date(godot_date) and get_number_of_yrs(godot_date) == 0:
            # a simple date, but no or unknown YRS
            print("simple date, but no yrs")
            if "year" in godot_date:
                # simple date with yrs=regnal/Unknown?
                query = create_cypher_for_none_year(godot_date, cluster_data)
                if query == "":
                    print("no cypher error", cnt, godot_date)
                cnt += 1
            else:
                # simple date with yrs=None
                query = create_cypher_for_none_yrs(godot_date, cluster_data)
                if query == "":
                    print("no cypher error", cnt, godot_date)
                cnt += 1
        else:
            print("______complex date")

        if query != "":
            print(query)
            result = session.run(query)
            for res in result:
                print(res['godot_uri'])
                concordance_tm_cluster_id_godot_uri.write(godot_date['cluster_id'][0] + "\t" + res['godot_uri'] + "\n")
        else:
            todo_list.write(repr(godot_date) + "\n")
            if "month" in godot_date:
                if len(godot_date['month']) > 1:
                    if "king" in godot_date and len(godot_date['king']) <= 1 and "consul" not in godot_date and "indictio" not in godot_date:
                        # regnal year with multiple months
                        # make 2 distinct queries for the two distinct single paths
                        if "year" in godot_date:
                            years_list = godot_date['year']
                        if "month" in godot_date:
                            months_list = godot_date['month']
                        if "day" in godot_date:
                            days_list = godot_date['day']
                        is_epagomenai = False
                        if len(months_list) == 2:
                            if months_list[0] == "month epagomenai" or months_list[1] == "month epagomenai":
                                is_epagomenai = True

                        tmp_regnal_dict_1 = {}
                        tmp_regnal_dict_1['cluster_id'] = godot_date['cluster_id']
                        tmp_regnal_dict_1['king'] = godot_date['king']
                        if "year" in godot_date:
                            tmp_regnal_dict_1['year'] = [list(godot_date['year'])[0]]
                        if "month" in godot_date:
                            tmp_regnal_dict_1['month'] = [list(godot_date['month'])[0]]
                        if "day" in godot_date:
                            if not is_epagomenai:
                                tmp_regnal_dict_1['day'] = [list(godot_date['day'])[0]]
                            elif is_epagomenai and months_list[0] == "month epagomenai":
                                tmp_regnal_dict_1['day'] = [list(godot_date['day'])[0]]

                        tmp_regnal_dict_2 = {}
                        tmp_regnal_dict_2['cluster_id'] = godot_date['cluster_id']
                        tmp_regnal_dict_2['king'] = godot_date['king']
                        if "year" in godot_date:
                            if len(godot_date['year']) > 1:
                                tmp_regnal_dict_2['year'] = [list(godot_date['year'])[1]]
                            else:
                                tmp_regnal_dict_2['year'] = [list(godot_date['year'])[0]]
                        if "month" in godot_date:
                            if len(godot_date['month']) > 1:
                                tmp_regnal_dict_2['month'] = [list(godot_date['month'])[1]]
                            else:
                                tmp_regnal_dict_2['month'] = [list(godot_date['month'])[0]]
                        if "day" in godot_date:
                            if len(godot_date['day']) > 1:
                                if not is_epagomenai:
                                    tmp_regnal_dict_2['day'] = [list(godot_date['day'])[1]]
                                elif is_epagomenai and months_list[1] == "month epagomenai":
                                    tmp_regnal_dict_2['day'] = [list(godot_date['day'])[1]]
                            else:
                                if not is_epagomenai:
                                    tmp_regnal_dict_2['day'] = [list(godot_date['day'])[0]]
                                elif is_epagomenai and months_list[1] == "month epagomenai":
                                    tmp_regnal_dict_2['day'] = [list(godot_date['day'])[0]]

                        query_1 = create_cypher_for_regnal_years(tmp_regnal_dict_1, cluster_data)
                        query_2 = create_cypher_for_regnal_years(tmp_regnal_dict_2, cluster_data)
                        print("### multiple date start ###")
                        print(query_1)
                        print(query_2)

                        # 2 paths for months; if both from same calendar type and no epagmon. day
                        # => list of 2 distinct dates => no common super godot node
                        godot_uri_1 = ""
                        godot_uri_2 = ""
                        result_1 = session.run(query_1)
                        for res in result_1:
                           godot_uri_1 = res['godot_uri']
                        result_2 = session.run(query_2)
                        for res in result_2:
                            godot_uri_2 = res['godot_uri']
                        ct_1 = get_calendar_type_by_month_name(tmp_regnal_dict_1['month'][0])
                        ct_2 = get_calendar_type_by_month_name(tmp_regnal_dict_2['month'][0])
                        if ct_1 != ct_2 or (ct_1 == ct_2 and is_epagomenai):
                            # create super godot node
                            if godot_uri_1 != "" and godot_uri_2 != "":
                                print(get_synchron_godot_uri([godot_uri_1, godot_uri_2], godot_date, cluster_data))
                                godot_synchron_query = get_synchron_godot_uri([godot_uri_1, godot_uri_2], godot_date, cluster_data)
                                result_super_node = session.run(godot_synchron_query)
                                for res in result_super_node:
                                    print(res['godot_super_uri'])
                        print("### multiple date end ###")


if __name__ == "__main__":
    main()