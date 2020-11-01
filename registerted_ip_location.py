from database import DatabaseQureies
import os
from dotenv import load_dotenv
import ipinfo
import pprint

db = DatabaseQureies()
load_dotenv()


def distinct_logins():
    logins = db.distinct_logins()
    return [x[0] for x in logins]


def list_distinct_ips(client_id):
    distinct_ips = db.distinct_ips(client_id)
    return distinct_ips


def call_geolocation_api(ip_address):
    access_token = os.getenv('ipinfo_token')
    handler = ipinfo.getHandler(access_token)
    details = handler.getDetails(ip_address)
    ip_details = details.all
    return ip_details['country_name'], ip_details['org']


def run():
    client_ids = distinct_logins()
    for client_id in client_ids:
        ips_against_client_id = [x[0] for x in list_distinct_ips(client_id)]
        if len(ips_against_client_id) > 1:
            multi_ip = 'yes'
        else:
            multi_ip = 'no'
        for ip_against_client_id in ips_against_client_id:
            ip_string_split = str(ip_against_client_id).split(':')
            try:

                if db.check_if_row_exists(client_id, ip_string_split[0]):
                    continue
                else:
                    country, isp = call_geolocation_api(ip_string_split[0])
                    db.insert_db_row(
                        client_id, ip_string_split[0], country, isp[:50], multi_ip)
            except Exception as e:
                print(e)


run()
