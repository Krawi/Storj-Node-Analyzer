#!/usr/bin/env python

import argparse
import atexit
import datetime
import email.message
import io
import json
import locale
import pathlib
import platform
import signal
import smtplib
import socket
import sys
import threading
import time
import urllib.request
import urllib.error

# It's better to have log for errors defined
encoding_charset = "utf-8"
sys.stderr = io.open("analyzer.log", mode = "w", encoding = encoding_charset)

OS_Windows = "Windows"

# petabyte = 2**50
# terabyte = 2**40
# gigabyte = 2**30
# megabyte = 2**20
# kilobyte = 2**10
# byte     = 1
petabyte = 10**15
terabyte = 10**12
gigabyte = 10**9
megabyte = 10**6
kilobyte = 10**3
byte     = 1
petabyte_unit = "PB"
terabyte_unit = "TB"
gigabyte_unit = "GB"
megabyte_unit = "MB"
kilobyte_unit = "KB"
byte_unit     = "B"

# Make storage values + unit
def output_units(output):
    strio_output = io.StringIO()

    if abs(output) >= petabyte:
        print("%.3f %s" % (output / petabyte, petabyte_unit), file=strio_output)
    elif abs(output) >= terabyte:
        print("%.3f %s" % (output / terabyte, terabyte_unit), file=strio_output)
    elif abs(output) >= gigabyte:
        print("%.3f %s" % (output / gigabyte, gigabyte_unit), file=strio_output)
    elif abs(output) >= megabyte:
        print("%.3f %s" % (output / megabyte, megabyte_unit), file=strio_output)
    elif abs(output) >= kilobyte:
        print("%.3f %s" % (output / kilobyte, kilobyte_unit), file=strio_output)
    else:
        print("%d %s" % (int(output), byte_unit), file=strio_output)

    return(strio_output.getvalue().replace("\n", ""))
# end def

# Payout data
payed_storage_tb      =  1.50
payed_customer_egress = 20.00
payed_repair_egress   = 10.00

# How many days has this month?
def get_month_days(year, month):
    if month < 12:
        return((datetime.date(year, month + 1, 1) \
                - datetime.date(year, month, 1)).days)
    else:
        return(31)
# end def

# Current day as decimal number
def get_day_dec():
    global utc
    utc = datetime.datetime.now(datetime.timezone.utc)
    global year
    year    = int(utc.strftime("%Y"))
    global month
    month   = int(utc.strftime("%m"))
    global day
    day     = int(utc.strftime("%d"))
    global hours
    hours   = int(utc.strftime("%H"))
    global minutes
    minutes = int(utc.strftime("%M"))
    global seconds
    seconds = int(utc.strftime("%S"))

    global month_days
    month_days = get_month_days(year, month)

    # date as decimal
    return((float(day - 1)) + ((hours*3600) + (minutes*60) + seconds) / (24*3600))
# end def

day_dec = get_day_dec()

# Dictionary Keys
node_list                    = "nodes"
node_name                    = "name"
node_url                     = "url"
node_location                = "location"
node_storage_used            = "storage_used"
node_storage_used_month      = "storage_used_month"
node_payout                  = "payout"
node_payout_month            = "payout_month"
node_payout_tb               = "payout_tb"
node_payout_tb_month         = "payout_tb_month"
node_payout_average          = "payout_average"

satellite_list               = "satellites"
satellite_name               = "name"
satellite_storage_used       = "storage_used"
satellite_storage_used_month = "storage_used_month"
satellite_storage_summary    = "storage_summary"
satellite_egress_summary     = "egress_summary"
satellite_repair_summary     = "repair_summary"
satellite_payout             = "payout"
satellite_payout_month       = "payout_month"
satellite_payout_tb          = "payout_tb"
satellite_payout_tb_month    = "payout_tb_month"
satellite_payout_average     = "payout_average"

storage_node_total           = "node_total"
storage_first_day            = "first_day"
storage_first_month          = "first_month"
storage_used_first_day       = "used_first_day"
storage_used_current_day     = "used_current_day"
storage_used_last_day        = "used_last_day"
storage_used_month           = "used_month"
storage_used_growth          = "used_growth"
storage_used_average         = "used_average"

location_list                = "locations"
location_name                = "name"

smtp_dictionary              = "mail"
smtp_server                  = "server"
smtp_port                    = "port"
smtp_starttls                = "starttls"
smtp_ssl                     = "ssl"
smtp_timeout                 = "timeout"
smtp_username                = "username"
smtp_username_full           = "username_full"
smtp_userpassword            = "userpassword"
smtp_sendername_full         = "sendername_full"
smtp_subject_day             = "subject_day"
smtp_subject_midnight        = "subject_midnight"
smtp_msgtxt_day              = "msgtxt_day"
smtp_msgtxt_midnight         = "msgtxt_midnight"

export_config_dictionary     = "export"
export_path_unix             = "path_unix"
export_path_windows          = "path_windows"

excel_config_dictionary      = "excel"
excel_path_unix              = "path_unix"
excel_path_windows           = "path_windows"
excel_file                   = "file"
excel_interval               = "interval"

sheets_config_dictionary     = "google_sheets"
sheets_path_unix             = "path_unix"
sheets_path_windows          = "path_windows"
sheets_file                  = "file"
sheets_interval              = "interval"

locale_dictionary            = "locale"
locale_end_of_month          = "end_of_month"
locale_location              = "location"
locale_satellite             = "satellite"
locale_node                  = "node"
locale_is_running_since      = "is_running_since"
locale_first_data_since      = "first_data_since"
locale_days                  = "days"
locale_disk_used             = "disk_used"
locale_payout                = "payout"
locale_per                   = "per"
locale_current               = "current"
locale_estimated             = "estimated"
locale_time_format           = "time_format"

excel_set_names_header       = "header"
excel_set_names_month        = "month"
excel_set_names_locations    = "locations"

ext_csv  = ".csv"
ext_txt  = ".txt"
ext_json = ".json"

# Localized values avaiable?
def assign_localization():
    locale_dict = localization[locale_dictionary][locale_value]
    global end_of_month_str
    try:
        end_of_month_str = locale_dict[locale_end_of_month]
    except:
        end_of_month_str = "End of month (estimated)"

    global location_str
    try:
        location_str = locale_dict[locale_location]
    except:
        location_str = "Location (current)"

    global satellite_str
    try:
        satellite_str = locale_dict[locale_satellite]
    except:
        satellite_str = "Satellite"

    global node_str
    try:
        node_str = locale_dict[locale_node]
    except:
        node_str = "Node"

    global is_running_since_str
    try:
        is_running_since_str = locale_dict[locale_is_running_since]
    except:
        is_running_since_str = "is running since"

    global first_data_since_str
    try:
        first_data_since_str = locale_dict[locale_first_data_since]
    except:
        first_data_since_str = "first data since"

    global days_str
    try:
        days_str = locale_dict[locale_days]
    except:
        days_str = "days"

    global disk_used_str
    try:
        disk_used_str = locale_dict[locale_disk_used]
    except:
        disk_used_str = "disk used"

    global payout_str
    try:
        payout_str = locale_dict[locale_payout]
    except:
        payout_str = "payout"

    global per_str
    try:
        per_str = locale_dict[locale_per]
    except:
        per_str = "per"

    global current_str
    try:
        current_str = locale_dict[locale_current]
    except:
        current_str = "current"

    global estimated_str
    try:
        estimated_str = locale_dict[locale_estimated]
    except:
        estimated_str = "estimated"

    # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    global time_format_str
    try:
        time_format_str = locale_dict[locale_time_format]
    except:
        time_format_str = "%m/%d/%Y, %I:%M:%S %p"
# end def

# Define arguments
arg_description = "Querys data from Storj-Exporter instances in the network and analyze it.\nV1.0.0"
parser = argparse.ArgumentParser(formatter_class = argparse.RawDescriptionHelpFormatter,
                                 description = arg_description)
parser.add_argument("-a", "--add_date", action=argparse.BooleanOptionalAction, \
                    help = "adds '-YYYY-MM-DD' to output file names (UTC time)")
parser.add_argument("-t", "--text", action=argparse.BooleanOptionalAction, \
                    help = "creates a text file 'Storj.txt'")
parser.add_argument("-c", "--csv", action=argparse.BooleanOptionalAction, \
                    help = "creates 'Storj.csv' with delimiter ',' as default")
parser.add_argument("-d", "--delimiter", metavar = "delimiter", \
                    nargs = '?', type = str, default = ",", const = ",", \
                    help = "sets delimiter for csv file")
parser.add_argument("-j", "--json", action=argparse.BooleanOptionalAction, \
                    help = "creates JSON files 'Storj-Nodes.json' + 'Storj-Storage.json'")
parser.add_argument("-m", "--midnight", metavar = "minutes", \
                    nargs = '?', type = int, const = 5, \
                    help = "waits till x minutes before 24:00 (UTC) to proceed, default 5")
parser.add_argument("-s", "--send", action=argparse.BooleanOptionalAction, \
                    help = "sends the text file(s) as email (needs -t, -c or -j)")
parser.add_argument("-e", "--excel", action=argparse.BooleanOptionalAction, \
                    help = "activates the Excel mode")
parser.add_argument("-g", "--google", action=argparse.BooleanOptionalAction, \
                    help = "activates the Google Sheets mode")
parser.add_argument("-l", "--locale", metavar = "locale", \
                    nargs = '?', type = str, \
                    help = "override the locale value from OS")
parser.add_argument("-p", "--provider", metavar = "<name>", \
                    nargs = '?', type = str, default = "mail", const = "mail", \
                    help = "uses given JSON file for smtp settings, default 'mail'")
args = parser.parse_args()

# Load JSON data
def load_json(filename):
    global storage
    try:
        input_json = io.open(filename, mode = "r", encoding = encoding_charset)
        json_dictionary = json.load(input_json)
        # print(json_dictionary)
        input_json.close()
    except:
        json_dictionary = {}
    # end try

    return(json_dictionary)
# end def

# Save JSON data
def save_json(json_dictionary, filename):
    output_json = io.open(filename, mode = "w", encoding = encoding_charset)
    json.dump(json_dictionary, output_json, indent = 2, ensure_ascii = False)
    #json_str = json.dumps(storage, indent = 2)
    #output_json.write(json_str)
    output_json.close()
# end def

# Clean up at exit script
def exit_handler(arg1, arg2):
    # print(arg1, arg2)
    print("Exiting, saving data...")
    save_json(storage, config_storage_filename)
    if args.excel:
        save_json(storage_excel, config_excel_storage)
        # end if
    if args.google:
        save_json(storage_google, config_google_storage)
    # end if
    print("Bye")

    # Now it's finally over
    sys.exit(0)
# end def

# And register our exit handler
atexit.register(exit_handler)
signal.signal(signal.SIGTERM, exit_handler)
signal.signal(signal.SIGINT, exit_handler)

# Mail wanted?
if args.send:
    # Text, JSON or CSV file set?
    if not (args.text or args.csv or args.json):
        sys.stderr.write("Error: No email sending without file(s)!\n")
        sys.exit(1)
    # end if
# end if

# CSV wanted?
if args.csv and not (args.excel or  args.google):
    sys.stderr.write("Error: CSV without --excel or --google used!\n")
    sys.exit(1)
# end if

# Date for mail subject
def get_date_subject():
    return(utc.strftime("%Y") + "-" + utc.strftime("%m") + "-" + utc.strftime("%d"))
# end def


# Date for output files if --add_date is set or forced, otherwise empty string
def get_date_filename(force = False):
    if args.add_date or force:
        return(utc.strftime("-%Y") + "-" + utc.strftime("%m") + "-" + utc.strftime("%d"))
    else:
        return("")
    # end if
# end def

# Output file names
output_txt_name          = "Storj" + get_date_filename() + ext_txt
output_json_nodes_name   = "Storj-Nodes" + get_date_filename() + ext_json
output_json_storage_name = "Storj-Storage" + get_date_filename() + ext_json

# Config file names
config_storj_filename         = "storj.json"
config_storage_filename       = "storage.json"
config_localization_filename  = "localization.json"
config_configuration_filename = "configuration.json"
config_mail_filename          = "mail.json"
config_excel_storage          = "storage-excel.json"
config_excel_header           = "-header.csv"
config_excel_month            = "-month.csv"
config_excel_locations        = "-locations.csv"
config_google_storage         = "storage-google.json"
#config_nodes_filename         = "nodes.json"
#config_satellites_filename    = "satellites.json"
#config_export_filename        = "export.json"
#config_excel_filename         = "excel.json"
#config_google_sheets_filename = "google-sheets.json"
config_locations_filename     = "locations.json"

# Waiting for midnight UTC time
def sleep_until(minutes):
    now = datetime.datetime.today()
    hours_now = int(now.strftime("%H"))
    difference = hours_now - hours
    start  = datetime.datetime(now.year, now.month, now.day, 23, 60 - minutes)
    start += datetime.timedelta(hours = difference)
    print("Wait until:", start)
    wait_sec = int((start - now).total_seconds())
    for wait in range(wait_sec):
        time.sleep(1)

    # Update needed
    day_dec = round((float(day - 1)) + ((23*3600) + ((60 - minutes)*60), 2))
# end def

if args.midnight:
    sleep_until(args.midnight)
# end if

# Gives the full Excel filenames including path
def get_excel_names_full():
    excel = config[excel_config_dictionary]

    if args.csv and args.excel:
        if platform.system() == OS_Windows:
            path = export[export_path_windows]
        else:
            path = export[export_path_unix]
        # end if
    else:
        if platform.system() == OS_Windows:
            path = excel[excel_path_windows]
        else:
            path = excel[excel_path_unix]
        # end if
    # end if
    filename = excel[excel_file]

    return_value = {}
    return_value[excel_set_names_header]    = pathlib.Path(path) \
                        / (filename + get_date_filename() + config_excel_header)
    return_value[excel_set_names_month]     = pathlib.Path(path) \
                        / (filename + get_date_filename() + config_excel_month)
    return_value[excel_set_names_locations] = pathlib.Path(path) \
                        / (filename + get_date_filename() + config_excel_locations)
    return(return_value)
# end if

# Gives the full Google Sheets filename including path
def get_google_name_full():
    sheets = config[sheets_config_dictionary]

    if args.csv and args.google:
        if platform.system() == OS_Windows:
            path = export[export_path_windows]
        else:
            path = export[export_path_unix]
        # end if
    else:
        if platform.system() == OS_Windows:
            path = sheets[sheets_path_windows]
        else:
            path = sheets[sheets_path_unix]
        # end if
    # end if
    filename = sheets[sheets_file] + get_date_filename() + ext_csv

    return(pathlib.Path(path) / filename)
# end if

# Write simple text file
def text_write():
    parting_line = "-------------------------------------------------------------------------------\n"

    if platform.system() == OS_Windows:
        path = export[export_path_windows]
    else:
        path = export[export_path_unix]
    # end if
    output_txt_full = pathlib.Path(path) / output_txt_name
    output_txt = io.open(output_txt_full, mode = "w", encoding = encoding_charset)

    for node in nodes:
        nodename = node[node_name]
        # Standard sizes for values stored: used = TB, growth = GB, average = MB
        used       = output_units(node[node_storage_used] * terabyte)
        growth     = output_units(storage[nodename][storage_node_total][storage_used_growth]  * gigabyte)
        average    = output_units(storage[nodename][storage_node_total][storage_used_average] * megabyte)
        used_month = output_units(storage[nodename][storage_node_total][storage_used_month]   * terabyte)
        print("%s: %s %s %.4f %s\n" \
              " %s: %s, +: %s, Ø: %s, %s: ~%s\n" \
              " %s %s: $%7.4f, Ø: $%7.4f, %s: ~$%7.4f\n" \
              " %s TB %s: $%7.4f, %s: ~$%7.4f\n" \
              % (node_str, nodename, is_running_since_str, day_dec, days_str, \
                 disk_used_str, used, growth, average, estimated_str, used_month, \
                 payout_str, current_str, node[node_payout], \
                 node[node_payout_average], \
                 estimated_str, node[node_payout_month], \
                 per_str, current_str, node[node_payout_tb], \
                 estimated_str, node[node_payout_tb_month]), \
              file = output_txt)

        # Satellites list
        for satellite in satellites:
            sat = satellite[satellite_name]
            # Standard sizes for values stored: used = TB, growth = GB, average = MB
            used       = output_units(storage[nodename][sat][storage_used_current_day] * terabyte)
            growth     = output_units(storage[nodename][sat][storage_used_growth]      * gigabyte)
            average    = output_units(storage[nodename][sat][storage_used_average]     * megabyte)
            used_month = output_units(storage[nodename][sat][storage_used_month]       * terabyte)

            print("%s: %s %s %.4f\n" \
                  " %s: %s, +: %s, Ø: %s, %s: ~%s\n" \
                  " %s %s: $%7.4f, Ø: $%7.4f, %s: ~$%7.4f\n" \
                  " %s TB %s: $%7.4f, %s: ~$%7.4f\n" \
                  % (satellite_str, sat, \
                     first_data_since_str, storage[nodename][sat][storage_first_day], \
                     disk_used_str, used, growth, average, estimated_str, used_month,
                     payout_str, current_str, node[sat][satellite_payout], \
                     node[sat][satellite_payout_average], \
                     estimated_str, node[sat][satellite_payout_month], \
                     per_str, current_str, node[sat][satellite_payout_tb], \
                     estimated_str, node[sat][satellite_payout_tb_month]), \
                  file = output_txt)
        # end for
        output_txt.write(parting_line)
    # end for

    output_txt.close()
# end def

# Write header line month estimated
def csv_write_header_month(csv_output, delimiter):
    now = datetime.datetime.now()
    row = end_of_month_str + " (" + str(month_days) + ") " \
          + now.strftime(time_format_str) + delimiter + delimiter + delimiter
    for satellite in satellites:
        row += delimiter + delimiter + delimiter
    # end for
    row += "\n"
    csv_output.write(row)
# end def

# Gives the hostname part of satellites name and shortens long names
def get_sat_hostname(fqdn):
    words = fqdn.split(".")
    sat = words[0]
    if sat == "europe-north-1":
        sat = "eun"
    elif sat == "saltlake":
        sat = "slk"
    # end if

    return(sat)
# end def

# Write cell header line month estimated
def csv_write_cell_header_month(csv_output, delimiter):
    row = node_str
    row += delimiter + "N:D"
    row += delimiter + "N:$"
    row += delimiter + "N:$T"
    for satellite in satellites:
        sat = get_sat_hostname(satellite[satellite_name])
        row += delimiter + sat + ":D"
        row += delimiter + sat + ":$"
        row += delimiter + sat + ":$T"
    # end for
    row += "\n"
    csv_output.write(row)
# end def

# Write cell lines month estimated
def csv_write_cells_month(nodes, storage, csv_output, delimiter):
    for node in nodes:
        nodename = node[node_name]
        row = nodename
        if node_storage_used in node:
            row += delimiter \
                   + str(storage[nodename][storage_node_total][storage_used_month])
        else:
            row += delimiter
        # end if
        if node_payout_month in node:
            row += delimiter + str(node[node_payout_month])
        else:
            row += delimiter
        # end if
        if node_payout_tb_month in node:
            row += delimiter + str(node[node_payout_tb_month])
        else:
            row += delimiter
        # end if

        for satellite in satellites:
            sat = satellite[satellite_name]
            try:
                row += delimiter \
                       + str(storage[nodename][sat][storage_used_month])
            except:
                row += delimiter
            try:
                row += delimiter \
                       + str(node[sat][satellite_payout_month])
            except:
                row += delimiter
            try:
                row += delimiter \
                       + str(node[sat][satellite_payout_tb_month])
            except:
                row += delimiter
        # end for
        row += "\n"
        if delimiter == ";":
            row = row.replace( ".", ",")
        # end if
        csv_output.write(row)
    # end for
# end def

# Write header line location
def csv_write_header_location(csv_output, location, delimiter):
    row = location_str + ": " + location[location_name] \
          + delimiter + delimiter+ delimiter
    for satellite in satellites:
        row += delimiter + delimiter + delimiter
    # end for
    row += "\n"
    csv_output.write(row)
# end def

# Write cell header line locations
def csv_write_cell_header_location(csv_output, delimiter):
    row = node_str
    row += delimiter + "N:D+(G)"
    row += delimiter + "N:DØ(M)"
    row += delimiter + "N:PØ"
    for satellite in satellites:
        sat = get_sat_hostname(satellite[satellite_name])
        row += delimiter + sat + ":D+"
        row += delimiter + sat + ":DØ"
        row += delimiter + sat + ":PØ"
    # end for
    row += "\n"
    csv_output.write(row)
# end def

# Write location cells for a node
def csv_write_cells_location(csv_output, node, storage, delimiter):
    nodename = node[node_name]
    row = nodename
    if storage_used_growth in storage[nodename][storage_node_total]:
        row += delimiter \
               + str(storage[nodename][storage_node_total][storage_used_growth])
    else:
        row += delimiter
    # end if
    if storage_used_average in storage[nodename][storage_node_total]:
        row += delimiter \
               + str(storage[nodename][storage_node_total][storage_used_average])
    else:
        row += delimiter
    # end if
    if storage_used_average in storage[nodename][storage_node_total]:
        row += delimiter \
               + str(node[node_payout_average])
    else:
        row += delimiter
    # end if

    for satellite in satellites:
        sat = satellite[satellite_name]
        try:
            row += delimiter \
                   + str(storage[nodename][sat][storage_used_growth])
        except:
            row += delimiter
        try:
            row += delimiter \
                   + str(storage[nodename][sat][storage_used_average])
        except:
            row += delimiter
        try:
            row += delimiter \
                   + str(node[sat][satellite_payout_average])
        except:
            row += delimiter
    # end for
    row += "\n"
    if delimiter == ";":
        row = row.replace( ".", ",")
    # end if
    csv_output.write(row)
# end def

# Write parsed data to a set of CSV files using delimiter, files used for Excel mode
def csv_write_excel(nodes, storage, delimiter):
    names = get_excel_names_full()

    # Write file with header lines
    csv_output = io.open(names[excel_set_names_header], mode = "w", encoding = encoding_charset)
    csv_write_header_month(csv_output, delimiter)
    csv_write_cell_header_month(csv_output, delimiter)
    for location in locations:
        csv_write_header_location(csv_output, location, delimiter)
    csv_write_cell_header_location(csv_output, delimiter)
    csv_output.close()

    # Write file with month estimated
    csv_output = io.open(names[excel_set_names_month], mode = "w", encoding = encoding_charset)
    csv_write_cell_header_month(csv_output, delimiter)
    csv_write_cells_month(nodes, storage, csv_output, delimiter)
    csv_output.close()

    # Write locations file
    csv_output = io.open(names[excel_set_names_locations], mode = "w", encoding = encoding_charset)
    csv_write_cell_header_location(csv_output, delimiter)
    for node in nodes:
        csv_write_cells_location(csv_output, node, storage, delimiter)
    # end for
    csv_output.close()
# end def

# Write parsed data to a CSV file using delimiter
def csv_write_google(nodes, storage, delimiter):
    csv_output = io.open(get_google_name_full(), mode = "w", encoding = encoding_charset)

    # Write month estimated
    csv_write_header_month(csv_output, delimiter)
    csv_write_cell_header_month(csv_output, delimiter)
    csv_write_cells_month(nodes, storage, csv_output, delimiter)

    csv_output.write("\n")

    # Write locations
    for location in locations:
        csv_write_header_location(csv_output, location, delimiter)
        csv_write_cell_header_location(csv_output, delimiter)
        for node in nodes:
            if node[node_location] == location[location_name]:
                csv_write_cells_location(csv_output, node, storage, delimiter)
        # end for
        csv_output.write("\n")
    # end for

    csv_output.close()
# end def

# Save the filled dictionarys to a file
def write_json():
    if platform.system() == OS_Windows:
        path = export[export_path_windows]
    else:
        path = export[export_path_unix]
    # end if

    save_json(nodes, pathlib.Path(path) / output_json_nodes_name)
    save_json(storage, pathlib.Path(path) / output_json_storage_name)
# end def

# Add file to message as plaintext
def add_file_to_message(filename_full, message):
    filename = filename_full.name
    input = io.open(filename_full, mode = "rb")
    filecontent = input.read()
    input.close()
    message.add_attachment(filecontent, filename = filename, \
                           maintype = "text", subtype = "plain")
# end def

# Send file(s) as email attachment(s)
def send_email():
    mail = load_json(config_mail_filename)
    smtp = mail[args.provider]

    date = datetime.datetime.now().strftime(time_format_str)

    if platform.system() == OS_Windows:
        path = export[export_path_windows]
    else:
        path = export[export_path_unix]
    # end if

    output_json_nodes_full = pathlib.Path(path) / output_json_nodes_name
    hostname = socket.getfqdn()
    # hostname = "<name>"
    message = email.message.EmailMessage()
    message["From"] = smtp[smtp_sendername_full] + " <" + smtp[smtp_username] + ">"
    message["To"]   = smtp[smtp_username_full] + " <" + smtp[smtp_username] + ">"

    if args.midnight:
        message["Subject"] = smtp[smtp_subject_midnight] + date + ", Host: " + hostname
        message.set_content(smtp[smtp_msgtxt_midnight])
    else:
        message["Subject"] = smtp[smtp_subject_day] + date + ", Host: " + hostname
        message.set_content(smtp[smtp_msgtxt_day])
    # end if

    if args.text:
        output_txt_full = pathlib.Path(path) / output_txt_name
        add_file_to_message(output_txt_full, message)
        print("Added: " + output_txt_name)
    # end if

    if args.csv:
        if args.excel:
            names = get_excel_names_full()
            csv_output_full = names[excel_set_names_header]
            add_file_to_message(csv_output_full, message)
            print("Added: " + csv_output_full.name)
            csv_output_full = names[excel_set_names_month]
            add_file_to_message(csv_output_full, message)
            print("Added: " + csv_output_full.name)
            csv_output_full = names[excel_set_names_locations]
            add_file_to_message(csv_output_full, message)
            print("Added: " + csv_output_full.name)
        else:
            csv_output_full = get_google_name_full()
            add_file_to_message(csv_output_full, message)
            print("Added: " + csv_output_full.name)
        #end if
    # end if

    if args.json:
        output_json_nodes_full = pathlib.Path(path) / output_json_nodes_name
        add_file_to_message(output_json_nodes_full, message)
        print("Added: " + output_json_nodes_name)
        output_json_storage_full = pathlib.Path(path) / output_json_storage_name
        add_file_to_message(output_json_storage_full, message)
        print("Added: " + output_json_storage_name)
    # end if

    print("Sending email using provider: '" + args.provider + "'")
    if smtp[smtp_starttls] == "true" and smtp[smtp_ssl] == "true":
        sys.stderr.write("Error: Both options 'STARTTLS' and 'SMTP over SSL' are activated!\n")
        sys.exit(1)
    elif smtp[smtp_starttls] == "true":
        print(" Secured with STARTTLS")
    elif smtp[smtp_ssl] == "true":
        print(" Secured with SMTP over SSL")
    else:
        print(" Unsecure connection")
    # end if

    try:
        # GMail https://pythonassets.com/posts/send-email-via-gmail-and-smtp/
        if smtp[smtp_ssl] == "true":
            smtp_connection = smtplib.SMTP_SSL(smtp[smtp_server], smtp[smtp_port], \
                              hostname, None, None, int(smtp[smtp_timeout]))
        else:
            smtp_connection = smtplib.SMTP(smtp[smtp_server], smtp[smtp_port], \
                              hostname, int(smtp[smtp_timeout]))
        # end if
        print(" Connected")
        if smtp[smtp_starttls] == "true":
            smtp_connection.starttls()
            print(" STARTTLS activated")
        smtp_connection.login(smtp[smtp_username], smtp[smtp_userpassword])
        print(" Login successfull")
        smtp_connection.send_message(message)
        print(" Sent")
        smtp_connection.quit()
        print(" Quit")
        smtp_connection.close()
        print(" Close\nSuccessfully sent email")
    except smtplib.SMTPException:
        sys.stderr.write("Error: unable to send email\n")
        sys.exit(1)
    # end try
# end def

# Get the name of the satellite from exporter data string
def get_satellite(entry):
    return entry[entry.find("url=") + 5 : entry.find(":7777")]
# end def

# Parsing satellites data for node
def analyze_satellite_data(node, satellite):
    nodename = node[node_name]
    sat = satellite[satellite_name]
    # print(sat)

    # First run ever?
    try:
        if storage[nodename][sat]:
            # Satellite exist
            # print("exist")
            # fix
            try:
                if storage[nodename][sat][storage_used_last_day]:
                    pass
            except:
                storage[nodename][sat][storage_used_last_day] = 0.0
            # end try
            try:
                if storage[nodename][sat][storage_first_month]:
                    pass
            except:
                storage[nodename][sat][storage_first_month] = month
            # end try
        # end if
    except:
        # Satellite missing
        # print("added")
        storage[nodename][sat] = {}
        storage[nodename][sat][storage_first_day] = \
                (day - 1) + ((hours*3600) + (minutes*60) + seconds) / (24*3600)
        storage[nodename][sat][storage_first_month] = month
        storage[nodename][sat][storage_used_first_day] = \
                                                node[sat][satellite_storage_used]
        storage[nodename][sat][storage_used_last_day] = 0.0
    # end try

    # Rollover from last month?
    if storage[nodename][sat][storage_first_day] == 0.0 \
       and storage[nodename][sat][storage_used_last_day] > 0.0:
        storage[nodename][sat][storage_first_month] = month
        storage[nodename][sat][storage_used_first_day] = \
                storage[nodename][sat][storage_used_last_day]
        storage[nodename][sat][storage_used_last_day] = 0.0
    # end if

    # Running in Excel or Google Sheets mode?
    if args.excel or args.google:
        # Next month reached?
        if storage[nodename][sat][storage_first_month] != month:
            storage[nodename][sat][storage_first_month] = month
            storage[nodename][sat][storage_first_day] = 0.0
            storage[nodename][sat][storage_used_first_day] = \
                    storage[nodename][sat][storage_used_last_day]
            storage[nodename][sat][storage_used_last_day] = 0.0
        # end if
    # end if

    try:
        storage[nodename][sat][storage_used_current_day] = \
                                                node[sat][satellite_storage_used]
    except:
        sys.stderr.write("Data missing from satellite: " + sat + "check output from exporter!\n")
        storage[nodename][sat][storage_used_current_day] = 0.0
    # end try
    days_elapsed = ((day - 1) + ((hours*3600) + (minutes*60) + seconds) / (24*3600)) \
                   - storage[nodename][sat][storage_first_day]

    try:
        node[sat][satellite_payout] = \
                node[sat][satellite_storage_summary] \
                / month_days * payed_storage_tb \
                + (node[sat][satellite_egress_summary] \
                   - node[sat][satellite_repair_summary]) \
                * payed_customer_egress \
                + node[sat][satellite_repair_summary] \
                * payed_repair_egress
    except:
        # Try without satellite_repair_summary (start of month?)
        try:
            node[sat][satellite_payout] = \
                    (node[sat][satellite_storage_summary] \
                    / month_days * payed_storage_tb) \
                    + (node[sat][satellite_egress_summary] \
                    * payed_customer_egress)
        except:
            sys.stderr.write("Data missing from satellite: " + sat + "check output from exporter!\n")
            node[sat][satellite_payout] = 0.0
        # end try
    # end try

    node[sat][satellite_payout_month] = node[sat][satellite_payout] \
                                        / day_dec * month_days
    node[sat][satellite_payout_tb] = node[sat][satellite_payout] \
                                     / node[sat][satellite_storage_used]
    node[sat][satellite_payout_tb_month] = node[sat][satellite_payout_tb] \
                                           / day_dec * month_days
    if node[sat][satellite_payout_tb_month] < payed_storage_tb:
        node[sat][satellite_payout_tb_month] = payed_storage_tb
    # end if
    node[sat][satellite_payout_average] = node[sat][satellite_payout] / day_dec

    if days_elapsed > 0.0:
        storage[nodename][sat][storage_used_month] = \
                    (storage[nodename][sat][storage_used_current_day] \
                    - storage[nodename][sat][storage_used_first_day]) \
                    / days_elapsed \
                    * (month_days - storage[nodename][sat][storage_first_day]) \
                    + storage[nodename][sat][storage_used_first_day]
        storage[nodename][sat][storage_used_growth] = \
                (storage[nodename][sat][storage_used_current_day] \
                - storage[nodename][sat][storage_used_first_day]) \
                * terabyte / gigabyte
        storage[nodename][sat][storage_used_average] = \
                storage[nodename][sat][storage_used_growth] \
                / days_elapsed \
                * gigabyte / megabyte
    else:
        storage[nodename][sat][storage_used_month] = \
                storage[nodename][sat][storage_used_first_day]
        storage[nodename][sat][storage_used_growth] = 0.0
        storage[nodename][sat][storage_used_average] = 0.0
    # end if

    # Last run for this month?
    if args.midnight:
        storage[nodename][sat][storage_first_day] = 0.0
        storage[nodename][sat][storage_used_last_day] = \
                storage[nodename][sat][storage_used_current_day]
    # end if
# end def

# Parsing nodes data from exporter
def analyze_node_data(node, storage):
    if (not args.excel) and (not args.google) or args.csv:
        print("Node:", node[node_name])

    nodename = node[node_name]
    # print("Node:", nodename])
    try:
        exporter = urllib.request.urlopen(node[node_url])
    except urllib.error.HTTPError as error:
        sys.stderr.write("Error: Node: " + nodename + " Error code: ". error.code + "\n")
        return
    except urllib.error.URLError as error:
        sys.stderr.write("Error: Node: " + nodename + " Error reason: ", error.reason + "\n")
        return
    # end try
    exporter_text = exporter.readlines()
    exporter.close()

    node[node_payout] = 0.0

    # Update time values (for excel or google mode)
    day_dec = get_day_dec()
    month_days = get_month_days(year, month)

    for line in exporter_text:
        line  = line.decode('utf-8')
        line  = line.replace("\n", "")
        words = line.split()

        if words[0] == 'storj_total_diskspace{type="used"}':
            node[node_storage_used] = float(words[1]) / terabyte
        elif words[0] == 'storj_payout_currentMonth{type="payout"}':
            node[node_payout] += float(words[1]) / 100.0
        elif words[0] == 'storj_payout_currentMonth{type="held"}':
            node[node_payout] += float(words[1]) / 100.0
        elif "storageSummary" in words[0]:
            sat = get_satellite(words[0])
            try:
                node[sat][satellite_storage_summary] = float(words[1]) / terabyte
            except KeyError:
                node[sat] = {}
                node[sat][satellite_storage_summary] = float(words[1]) / terabyte
            # end try
        elif "egressSummary" in words[0]:
            sat = get_satellite(words[0])
            try:
                node[sat][satellite_egress_summary] = float(words[1]) / terabyte
            except KeyError:
                node[sat] = {}
                node[sat][satellite_egress_summary] = float(words[1]) / terabyte
            # end try
        elif "repair" in words[0] and "storj_sat_month_egress" in words[0]:
            sat = get_satellite(words[0])
            try:
                node[sat][satellite_repair_summary] = float(words[1]) / terabyte
            except KeyError:
                node[sat] = {}
                node[sat][satellite_repair_summary] = float(words[1]) / terabyte
            # end try
        elif "currentStorageUsed" in words[0]:
            sat = get_satellite(words[0])
            try:
               node[sat][satellite_storage_used] = float(words[1]) / terabyte
            except KeyError:
                node[sat] = {}
                node[sat][satellite_storage_used] = float(words[1]) / terabyte
            # end try
        # end if
    # end for

    node[node_payout_tb]       = node[node_payout]    / node[node_storage_used]
    node[node_payout_month]    = node[node_payout]    / day_dec * month_days
    node[node_payout_tb_month] = node[node_payout_tb] / day_dec * month_days
    if node[node_payout_tb_month] < 1.50:
        node[node_payout_tb_month] = 1.50
    node[node_payout_average]  = node[node_payout]    / day_dec

    # First run ever?
    try:
        if storage[nodename]:
            # Node exist
            # fix
            try:
                if storage[nodename][storage_node_total][storage_used_last_day]:
                    pass
            except:
                storage[node[node_name]][storage_node_total][storage_used_last_day] = 0.0
            # end try
            try:
                if storage[nodename][storage_node_total][storage_first_month]:
                    pass
            except:
                storage[nodename][storage_node_total][storage_first_month] = month
            # end try
        # end if
    except:
        # Node missing
        storage[nodename] = {}
        storage[nodename][storage_node_total] = {}
        storage[nodename][storage_node_total][storage_first_day] = \
                (day - 1) + ((hours*3600) + (minutes*60) + seconds) / (24*3600)
        storage[nodename][storage_node_total][storage_first_month] = month
        storage[nodename][storage_node_total][storage_used_first_day] = \
                                                            node[node_storage_used]
        storage[nodename][storage_node_total][storage_used_last_day] = 0.0
    # end try

    # Rollover from last month?
    if storage[nodename][storage_node_total][storage_first_day] == 0.0 \
       and storage[nodename][storage_node_total][storage_used_last_day] > 0.0:
        storage[nodename][storage_node_total][storage_first_month] = month
        storage[nodename][storage_node_total][storage_used_first_day] = \
                storage[nodename][storage_node_total][storage_used_last_day]
        storage[nodename][storage_node_total][storage_used_last_day] = 0.0
    # end if

    # Running in Excel or Google Sheets mode?
    if args.excel or args.google:
        # Next month reached?
        if storage[nodename][storage_node_total][storage_first_month] != month:
            storage[nodename][storage_node_total][storage_first_month] = month
            storage[nodename][storage_node_total][storage_first_day] = 0.0
            storage[nodename][storage_node_total][storage_used_first_day] = \
                    storage[nodename][storage_node_total][storage_used_last_day]
            storage[nodename][storage_node_total][storage_used_last_day] = 0.0
        # end if
    # end if

    storage[nodename][storage_node_total][storage_used_current_day] = \
                                                            node[node_storage_used]

    days_elapsed = ((day - 1) + ((hours*3600) + (minutes*60) + seconds) / (24*3600)) \
                   - storage[nodename][storage_node_total][storage_first_day]
    if days_elapsed > 0.0:
        storage[nodename][storage_node_total][storage_used_month] = \
                    (storage[nodename][storage_node_total][storage_used_current_day] \
                    - storage[nodename][storage_node_total][storage_used_first_day]) \
                    / days_elapsed \
                    * (month_days - storage[nodename][storage_node_total][storage_first_day]) \
                    + storage[nodename][storage_node_total][storage_used_first_day]
        storage[nodename][storage_node_total][storage_used_growth] = \
                (storage[nodename][storage_node_total][storage_used_current_day] \
                - storage[nodename][storage_node_total][storage_used_first_day]) \
                * terabyte / gigabyte
        storage[nodename][storage_node_total][storage_used_average] = \
                storage[nodename][storage_node_total][storage_used_growth] \
                / days_elapsed \
                * gigabyte / megabyte
    else:
        storage[nodename][storage_node_total][storage_used_month] = \
                storage[nodename][storage_node_total][storage_used_first_day]
        storage[nodename][storage_node_total][storage_used_growth] = 0.0
        storage[nodename][storage_node_total][storage_used_average] = 0.0
    # end if

    # Last run for this month?
    if args.midnight:
        storage[nodename][storage_node_total][storage_first_day] = 0.0
        storage[nodename][storage_node_total][storage_used_last_day] = \
                storage[nodename][storage_node_total][storage_used_current_day]
    # end if

    for satellite in satellites:
        analyze_satellite_data(node, satellite)
    # end for

    # print(node)
    # Totals for node doesn't match well, try adding satellites data
    estimated_total_disk = 0.0
    estimated_total_payout = 0.0
    for satellite in satellites:
        sat = satellite[satellite_name]
        estimated_total_disk   += storage[nodename][sat][storage_used_month]
        estimated_total_payout += node[sat][satellite_payout_month]
    # end for
    storage[nodename][storage_node_total][storage_used_month] = estimated_total_disk
    node[node_payout_month] = estimated_total_payout
# end def

# Which locale is used?
locale_str = str(locale.getlocale())
locale_value = locale_str[locale_str.find("('") + 2 : locale_str.find("',")]
# locale from arguemnets?
if args.locale:
    #print("locale overrides:", args.locale)
    locale_value = args.locale
# end if
# locale_value = "en_US"
# print("Using locale:", locale_value, "\n")

# Load needed JSON files
storj        = load_json(config_storj_filename)
nodes        = storj[node_list]
locations    = storj[location_list]
satellites   = storj[satellite_list]
config       = load_json(config_configuration_filename)
export       = config[export_config_dictionary]
localization = load_json(config_localization_filename)
storage      = load_json(config_storage_filename)

assign_localization()

threads_count = threading.active_count()
thread_lock = threading.Lock()

# Thread for Excel mode
def thread_for_excel(nodes, storage, excel):
    while True:
        thread_lock.acquire()
        for node in nodes_excel:
            analyze_node_data(node, storage_excel)
        csv_write_excel(nodes_excel, storage_excel, args.delimiter)
        save_json(storage_excel, config_excel_storage)
        thread_lock.release()

        seconds_to_wait = excel[excel_interval] * 60
        for wait in range(seconds_to_wait):
            time.sleep(1)
        # end for
    # end while
# end def

# Thread for Google Sheets mode
def thread_for_google(nodes, storage, sheets):
    while True:
        thread_lock.acquire()
        for node in nodes_google:
            analyze_node_data(node, storage_google)
        csv_write_google(nodes_google, storage_google, args.delimiter)
        save_json(storage_google, config_google_storage)
        thread_lock.release()

        # From Google Sheets Docs: about 15 minutes interval time
        # From reality: one access each hour is fast, sometimes it's really worse
        seconds_to_wait = sheets[sheets_interval] * 60
        for wait in range(seconds_to_wait):
            time.sleep(1)
        # end for
    # end while
# end def

# Excel mode?
if args.excel and not args.csv:
    print("Excel mode activated...")

    excel = config[excel_config_dictionary]
    nodes_excel = load_json(config_nodes_filename)
    global storage_excel
    storage_excel = load_json(config_excel_storage)

    thread_excel = threading.Thread(target = thread_for_excel, \
                            args = (nodes_excel, storage_excel, excel))
    thread_excel.start()
# end if

# Google Sheets mode?
if args.google and not args.csv:
    print("Google Sheets mode activated...")

    sheets = config[sheets_config_dictionary]
    nodes_google = load_json(config_nodes_filename)
    global storage_google
    storage_google = load_json(config_google_storage)

    thread_google = threading.Thread(target = thread_for_google, \
                            args = (nodes_google, storage_google, sheets))
    thread_google.start()
# end if

# More threads than usual running?
if threading.active_count() > threads_count:
    print("Threads:", threading.active_count() - threads_count)
    # Relax
    while True:
        time.sleep(1)
    # end while
# end if

# Lets rock
print("Lets rock...")
for node in nodes:
    analyze_node_data(node, storage)
# end for

# Finished parsing, save storage information
save_json(storage, config_storage_filename)

# Text file wanted?
if args.text:
    text_write()
# end if

# CSV file(s) wanted?
if args.csv and args.excel:
    csv_write_excel(nodes, storage, args.delimiter)
# end if
if args.csv and args.google:
    csv_write_google(nodes, storage, args.delimiter)
# end if

# JSON files wanted?
if args.json:
    write_json()
# end if

# Mail wanted?
if args.send:
    send_email()
# end if
