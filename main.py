from models import *
from report import make_report
import datetime
from file_manager import copy_for_date, copy_all_to_archive
import sys


def one_day(curr_date):
    copy_for_date(curr_date, 'data/')
    ILaccounts.load()
    ILcards.load()
    ILclients.load()
    ILterminals.load()
    ILtransactions.load()
    ILpassport_blacklist.load()
    make_report(curr_date)
    copy_all_to_archive('data/')


curr_date = datetime.datetime.now().date()
one_day(curr_date)

#print('First Day')
#curr_date = datetime.date(year=2021, month=3, day=2)
#one_day(curr_date)

#print('Second Day')
#curr_date = datetime.date(year=2021, month=3, day=3)
#one_day(curr_date)
#
#print('Third Day')
#curr_date = datetime.date(year=2021, month=3, day=4)
#one_day(curr_date)