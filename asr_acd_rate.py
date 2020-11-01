from database import DatabaseQureies
from datetime import datetime, timedelta

db = DatabaseQureies()


def get_start_end_datetime():
    Previous_Date = datetime.today() - timedelta(days=1)
    Previous_Date_formated = Previous_Date.strftime('%Y-%m-%d')
    return str(Previous_Date_formated) + ' 00:00:0', str(Previous_Date_formated) + ' 23:59:59'


def asr_calculation():
    ''' Total Calls = Total Failed Calls + Total Connected Calls
        ASR = 100*(Total Connected Calls/Total Calls)
    '''
    start_datetime, end_datetime = get_start_end_datetime()
    total_connected_calls = db.total_connected_calls(
        start_datetime, end_datetime)
    total_failed_calls = db.total_failed_calls(start_datetime, end_datetime)
    asr_cal = 100 * \
        (total_connected_calls[0] /
         (total_connected_calls[0]+total_failed_calls[0]))
    db.insert_asr_rate(start_datetime, end_datetime, format(asr_cal, '.2f'))


def acd_calculation():
    ''' ACD = Total Minutes / Total Connected Calls
    '''
    start_datetime, end_datetime = get_start_end_datetime()
    call_duration = db.call_duration_in_min(start_datetime, end_datetime)
    total_connected_calls = db.total_connected_calls(
        start_datetime, end_datetime)
    acd = call_duration[0]/total_connected_calls[0]
    db.insert_acd(start_datetime, end_datetime, format(acd, '.2f'))


asr_calculation()
acd_calculation()
