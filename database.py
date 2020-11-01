import mysql.connector
import mariadb
import os
from dotenv import load_dotenv
import sys

load_dotenv()


class DatabaseQureies:
    def connect_voip_db(self):
        try:
            voipswitch = mariadb.connect(
                user=os.getenv('user'),
                password=os.getenv('passwd'),
                host=os.getenv('host'),
                port=int(os.getenv('port')),
                database=os.getenv('database')
            )

            print('VOIP Database connected')
            return voipswitch
        except mariadb.Error as e:
            print('An error happed: ' + str(e))
            return None

    def connect_voip_report_db(self):
        try:
            voip_report_db = mysql.connector.connect(
                host=os.getenv('report_host'),
                user=os.getenv('report_user'),
                passwd=os.getenv('report_passwd'),
                database=os.getenv('report_database'),
                auth_plugin='mysql_native_password'
            )

            print('VOIP Report Database connected')
            return voip_report_db
        except mariadb.Error as e:
            print('An error happed: ' + str(e))
            return None

    def distinct_logins(self):
        voipdb = self.connect_voip_db()
        if not voipdb:
            print('Database not connected')
            sys.exit()
        cursor = voipdb.cursor()
        try:
            count_db_rows = "select distinct login from voipswitch.registered_users"
            cursor.execute(count_db_rows)
            result = cursor.fetchall()
            cursor.close()
            voipdb.close()
            return result
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voipdb.close()
            return None

    def distinct_ips(self, client_id):
        voipdb = self.connect_voip_db()
        if not voipdb:
            print('Database not connected')
            sys.exit()
        cursor = voipdb.cursor()
        try:
            count_db_rows = "select distinct ip_address from voipswitch.registered_users where login=%s"
            cursor.execute(count_db_rows, (client_id,))
            result = cursor.fetchall()
            cursor.close()
            voipdb.close()
            return result
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voipdb.close()
            return None

    def insert_db_row(self, client_id, ip_string_split, country, isp, multi_ip):
        voip_report_db = self.connect_voip_report_db()
        if not voip_report_db:
            print('VOIP Report Database not connected')
            sys.exit()
        cursor = voip_report_db.cursor()
        try:
            insert_ip_info = """INSERT INTO iptsp_reports.registered_ip_location (client_id,registered_ip,ip_country,ip_ISP,multi_ip) VALUES (%s, %s, %s, %s, %s)"""
            val = (client_id, ip_string_split, country, isp, multi_ip)
            cursor.execute(insert_ip_info, val)
            voip_report_db.commit()
            cursor.close()
            voip_report_db.close()
            print('Ip info inserted')
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voip_report_db.close()
            return None

    # def update_db_row(self, client_id, ip_string_split, country, isp, multi_ip):
    #     voip_report_db = self.connect_voip_report_db()
    #     if not voip_report_db:
    #         print('VOIP Report Database not connected')
    #         sys.exit()
    #     cursor = voip_report_db.cursor()
    #     try:
    #         cursor.execute("""
    #   UPDATE iptsp_reports.registered_ip_location
    #   SET ip_country=%s,ip_ISP=%s,multi_ip=%s
    #   WHERE client_id=%s and registered_ip=%s
    #     """, (last_ip, login_time, username, org_id))
    #         voip_report_db.commit()
    #         cursor.close()
    #         voip_report_db.close()
    #         return result
    #     except Exception as e:
    #         print('Error: ' + str(e))
    #         cursor.close()
    #         voip_report_db.close()
    #         return None

    def check_if_row_exists(self, client_id, ip):
        voip_report_db = self.connect_voip_report_db()
        if not voip_report_db:
            print('VOIP Report Database not connected')
            sys.exit()
        cursor = voip_report_db.cursor()
        try:
            check_db_row_exists = "select * from iptsp_reports.registered_ip_location where client_id = %s and registered_ip = %s"
            cursor.execute(check_db_row_exists, (client_id, ip))
            result = cursor.fetchone()
            cursor.close()
            voip_report_db.close()
            return result
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voip_report_db.close()
            return None

    def total_connected_calls(self, start_datetime, end_datetime):
        voipdb = self.connect_voip_db()
        if not voipdb:
            print('Database not connected')
            sys.exit()
        cursor = voipdb.cursor()
        try:
            connected_calls_query = "SELECT count(*) FROM voipswitch.calls WHERE call_start BETWEEN %s AND %s AND id_route IN (4,5) AND route_type=0"
            cursor.execute(connected_calls_query,
                           (start_datetime, end_datetime))
            result = cursor.fetchone()
            cursor.close()
            voipdb.close()
            return result
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voipdb.close()
            return None

    def total_failed_calls(self, start_datetime, end_datetime):
        voipdb = self.connect_voip_db()
        if not voipdb:
            print('Database not connected')
            sys.exit()
        cursor = voipdb.cursor()
        try:
            failed_calls_query = "select count(*) from voipswitch.callsfailed where call_start BETWEEN %s AND %s"
            cursor.execute(failed_calls_query,
                           (start_datetime, end_datetime))
            result = cursor.fetchone()
            cursor.close()
            voipdb.close()
            return result
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voipdb.close()
            return None

    def call_duration_in_min(self, start_datetime, end_datetime):
        voipdb = self.connect_voip_db()
        if not voipdb:
            print('Database not connected')
            sys.exit()
        cursor = voipdb.cursor()
        try:
            failed_calls_query = "SELECT SUM(duration)/60 FROM voipswitch.calls WHERE call_start BETWEEN %s AND %s AND id_route IN (4,5) AND route_type=0"
            cursor.execute(failed_calls_query,
                           (start_datetime, end_datetime))
            result = cursor.fetchone()
            cursor.close()
            voipdb.close()
            return result
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voipdb.close()
            return None

    def insert_asr_rate(self, *args):
        start_datetime, end_datetime, asr_cal = args
        voip_report_db = self.connect_voip_report_db()
        if not voip_report_db:
            print('VOIP Report Database not connected')
            sys.exit()
        cursor = voip_report_db.cursor()
        try:
            insert_asr_rate = """INSERT INTO iptsp_reports.asr_rate_per_day (start_datetime, end_datetime, asr_rate) VALUES (%s, %s, %s)"""
            val = (start_datetime, end_datetime, asr_cal)
            cursor.execute(insert_asr_rate, val)
            voip_report_db.commit()
            cursor.close()
            voip_report_db.close()
            print('ASR rate inserted')
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voip_report_db.close()
            return None

    def insert_acd(self, *args):
        start_datetime, end_datetime, acd = args
        voip_report_db = self.connect_voip_report_db()
        if not voip_report_db:
            print('VOIP Report Database not connected')
            sys.exit()
        cursor = voip_report_db.cursor()
        try:
            insert_acd = """INSERT INTO iptsp_reports.acd_per_day (start_datetime, end_datetime, acd) VALUES (%s, %s, %s)"""
            val = (start_datetime, end_datetime, acd)
            cursor.execute(insert_acd, val)
            voip_report_db.commit()
            cursor.close()
            voip_report_db.close()
            print('ACD inserted')
        except Exception as e:
            print('Error: ' + str(e))
            cursor.close()
            voip_report_db.close()
            return None
