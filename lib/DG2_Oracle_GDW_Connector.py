# coding=utf-8
import logging
import math
from datetime import datetime
import cx_Oracle
import os

from lib import DG2_Common


class OracleGDW(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.OracleGDW')
        os.environ["NLS_LANG"] = "JAPANESE_JAPAN.JA16SJISTILDE" # trial line
        self.db_connect = None
        self.cursor = None
        # self.encoding = 'shift-jis'  # Trial line
        self.hostname = '192.168.90.102'
        self.servicename = 'gdw'
        self.username = 'TSE_GLD_SRC'
        self.password = 'TSE_GLD_SRC'

    def connect_to_db(self):

        """ Connect to the database. """
        try:
            self.db_connect = cx_Oracle.connect(self.username + '/' + self.password + '@' + self.hostname + '/'
                                                + self.servicename)
            self.cursor = self.db_connect.cursor()
            self.logger.debug('connect/login in to oracle : %s', self.hostname + ':' + self.servicename)
        except cx_Oracle.DatabaseError as e:
            error = e.args
            self.logger.error('%s' % error.message)
            # logging.ERROR('%r', error.message)
            self.logger.error('connect_to_db : %r', error.message)
            raise

    def disconnect_from_db(self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist we don't really care.
        """

        try:
            self.cursor.close()
            self.db_connect.close()
            self.logger.debug('disconnect from  Oracle')
        except cx_Oracle.DatabaseError:
            pass

    def execute(self, sql, bindvars=None, commit=False):
        """
        Execute whatever SQL statements are passed to the method;
        commit if specified. Do not specify fetchall() in here as
        the SQL statement may not be a select.
        bindvars is a dictionary of variables you pass to execute.
        """

        try:
            self.cursor.execute(sql)
            # self.cursor.execute(sql, bindvars)
            # self.cursor.execute(sql)
            self.logger.info('Finished execution on Oracle: sql : %r', sql)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                self.logger.error('Table already exists')
            elif error.code == 1031:
                self.logger.error("Insufficient privileges")
            self.logger.error('execute : %r%s',error.code, error.message)
            # Raise the exception.
            raise

        # Only commit if it-s necessary.
        if commit:
            self.db_connect.commit()

    def fetch_all_data(self):
        try:
            lv_result = self.cursor.fetchall()
            self.logger.info('Finished fetch_all_data on Oracle')
            return lv_result
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self.logger.error('fetch_all_data : %r', error.message)
            raise

    def bulk_insert(self, in_sql_w_param, in_arr):
        logger_bulkinsert = logging.getLogger(__name__ + '.OracleGDW.bulk_insert')
        try:

            logger_bulkinsert.debug('start bulk_insert into GDW')
            logger_bulkinsert.debug('in_sql_w_param : ' +in_sql_w_param)
            self.cursor.prepare(in_sql_w_param)
            logger_bulkinsert.debug('defined the statement that will be executed')
            self.cursor.executemany(None, in_arr, batcherrors=True, arraydmlrowcounts=True)
            self.db_connect.commit()

            # where errors have taken place, the row count is 0; otherwise it is 1
            row_counts = self.cursor.getarraydmlrowcounts()
            logger_bulkinsert.debug('Array DML row counts:')
            logger_bulkinsert.debug('Array DML row counts: %r', row_counts)
            logger_bulkinsert.debug("where errors have taken place, the row count is 0; otherwise it is 1")
            logger_bulkinsert.debug('Finished bulk_insert/executemany on Oracle: sql : %r', in_sql_w_param)

            # display the errors that have taken place
            errors = self.cursor.getbatcherrors()
            logger_bulkinsert.debug("number of errors which took place: %r", len(errors))
            if len(errors) > 0:
                DG2_Common.send_error_mail('FAILED with error : bulk_insert')
            for error in errors:
                logger_bulkinsert.debug('Error %r at row offset %s', error.message.rstrip(), error.offset)

        except cx_Oracle.Error as e:
            error, = e.args
            logger_bulkinsert.error('FAILED with error : bulk_insert : %r', error.message)
            logger_bulkinsert.error('number of rows which succeeded: : %r', self.cursor.rowcount)
            DG2_Common.send_error_mail('FAILED with error : bulk_insert :' + error.message)


            raise

    def start_proc(self, in_sp_name):
        logger_startproc = logging.getLogger(__name__ + '.OracleGDW.start_proc')
        try:
            l_returned_msg = self.cursor.var(cx_Oracle.NCHAR)
            self.cursor.callproc(in_sp_name, [l_returned_msg])
            logger_startproc.debug('Finished SP on Oracle : %r', in_sp_name)
            logger_startproc.debug('Message from Oracle : %r', l_returned_msg.values[0])
            return l_returned_msg.values[0]
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logger_startproc.info('%s' % error.message)
            raise

    def start_func(self, in_func_name):
        logger_startfunc = logging.getLogger(__name__ + '.OracleGDW.start_func')
        try:
            l_returned_msg = self.cursor.callfunc(in_func_name, cx_Oracle.NCHAR)
            logger_startfunc.debug('Finished SFunction on Oracle : %r', in_func_name)
            logger_startfunc.debug('Message from Oracle : %r', l_returned_msg)
            return l_returned_msg
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logger_startfunc.error('%s' % error.message)


    def check_1min_already_imported(self, in_close_date):
        self.logger = logging.getLogger(__name__ + '.OracleGDW.check_1min_already_imported')
        try:
            self.connect_to_db()
            out_parameter = self.cursor.var(cx_Oracle.NCHAR)
            l_returned_msg = self.cursor.callfunc('pkg_dg2_python.find_close_1min', out_parameter, [in_close_date])
            self.logger.debug('Finished SF on Oracle : pkg_dg2_python.find_close_1min')
            self.logger.debug('Message from Oracle : checking result of %s had already been inserted : %r', in_close_date,
                         l_returned_msg)
            self.logger.debug('Return Message from oracle : checked if ' + str(in_close_date) + ' is existed or not : '
                  + l_returned_msg)
            return l_returned_msg

        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self.logger.error('FAILED with error : check 1min already imported : %r', error.message)
            DG2_Common.send_error_mail('FAILED with error : bulk_insert :' + error.message)

            raise

        finally:
            self.disconnect_from_db()  # Daily Routine

    def get_company_basic_info_latest_effective_date(self):
        logger_basiceffectdate = logging.getLogger(__name__ + '.get_company_basic_info_latest_effective_date')
        try:
            logger_basiceffectdate.info('checking if company_info data is existed on GDW......')
            self.connect_to_db()
            out_parameter = self.cursor.var(cx_Oracle.NCHAR)
            l_returned_msg = self.cursor.callfunc('pkg_dg2_python.get_company_info_valid_date', out_parameter)
            logger_basiceffectdate.debug('Finished SF on Oracle : pkg_dg2_python.get_company_info_valid_date')
            logger_basiceffectdate.debug('Message from Oracle : latest effective date of company_info : %r', l_returned_msg)
            logger_basiceffectdate.debug('Return Message from oracle : latest effective date of company_info : ' + l_returned_msg)
            return l_returned_msg

        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logger_basiceffectdate.error('FAILED with error : get effective date : %r', error.message)
            DG2_Common.send_error_mail('FAILED with error : get effective date :' + error.message)

            raise

        finally:
            self.disconnect_from_db()  # Daily Routine

    def batch__insert_1min_tsv_file_into_gdw(self, in_tar_file_abs_path):
        try:
            lo_tsv_list = DG2_Common.read_tsv_file_utf8_into_list(in_tar_file_abs_path)
            lo_converted_rows = self.cnvrt_dg2_tsv_1min_ashi_into_list4oracle(lo_tsv_list)
            self.bulk_insert_1min_ashi_arr(lo_converted_rows)

        except ValueError as err:
            logging.error(err.args)
            print(err.args)
            raise

    def batch__basic_company_info_tsv_file_into_gdw(self, in_tar_file_abs_path):
        logger_batch_bsiccompanyinfotsv = logging.getLogger(__name__ + '.batch__basic_company_info_tsv_file_into_gdw')
        try:
            lo_tsv_list = DG2_Common.read_tsv_file_utf8_into_list(in_tar_file_abs_path)
            lo_converted_rows = self.cnvrt_dg2_tsv_company_basic_into_list4oracle(lo_tsv_list)
            self.bulk_insert_basic_company_info_arr(lo_converted_rows)

        except ValueError as err:
            logger_batch_bsiccompanyinfotsv.error(err.args)
            DG2_Common.send_error_mail(err.args)
            raise

    def bulk_insert_1min_ashi_arr(self, in_arr):
        logger_bulkinser1min = logging.getLogger(__name__ + '.bulk_insert_1min_ashi_arr')
        try:
            self.connect_to_db()
            logger_bulkinser1min.info('Start import 1min ashi......')
            l_ret_msg0 = self.start_proc('pkg_dg2_python.truncate_table_1min')
            logger_bulkinser1min.debug('Return Message from oracle : truncate table 1min : ' + l_ret_msg0)

            lv_sql = 'insert into wb_dg2_tse_1min_eod (stock_code, close_1min_str, close_date, close_date_hour,' \
                     'close_date_et_count, close_month, close_year, price_o, ' \
                     'price_h, price_l, ' \
                     'price_c,' \
                     'volume,ts_create) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, sysdate) '
            self.bulk_insert(lv_sql, in_arr)

            l_ret_msg = self.start_func('pkg_dg2_python.get_inserted_row_count')

            logger_bulkinser1min.debug('Return Message from oracle : insert count : ' + l_ret_msg)
            logger_bulkinser1min.debug('Start data conversion 1min to hour, 5min ashi....')
            l_ret_msg1 = self.start_proc('pkg_dg2_python.start_format_1min')
            logger_bulkinser1min.debug('Return Message from oracle : conversion : ' + l_ret_msg1)
            if l_ret_msg1 == 'ERROR':
                raise ValueError
        except ValueError as e:
            error, = e.args
            logger_bulkinser1min.error(error.message)
            DG2_Common.send_error_mail(error.message)

        finally:
            self.disconnect_from_db()  # Daily Routine

    def bulk_insert_basic_company_info_arr(self, in_arr):
        logger_bulkins_basiccomp = logging.getLogger(__name__ + '.bulk_insert_basic_company_info_arr')
        try:
            self.connect_to_db()

            l_ret_msg0 = self.start_proc('pkg_dg2_python.truncate_WB_DG2_TSE_BASIC')
            logger_bulkins_basiccomp.debug('Return Message from oracle : truncate table WB_DG2_TSE_BASIC : ' + l_ret_msg0)

            lv_sql = 'insert into wb_dg2_tse_basic(' \
                     'data_date,' \
                     'week_pstn_type,' \
                     'stock_code,' \
                     'stock_name,market,' \
                     'market_segment,' \
                     'bizsegnum,n225_flag,' \
                     'shinyo_meigara_flag,' \
                     'volume_unit_today,' \
                     'volume_unit_yesterday,' \
                     'price_c_d1,volume_d1,' \
                     'stock_div_ratio,' \
                     'trade_unit_number,' \
                     'float_info,' \
                     'yobine_tani,' \
                     'min_candle_count,' \
                     'create_date)' \
                     'values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,sysdate) '
            self.bulk_insert(lv_sql, in_arr)

            logger_bulkins_basiccomp.info('Finished import basic company info to GDW....')
            l_ret_msg1 = self.start_proc('pkg_dg2_python.start_sp_company_master_bod')

            logger_bulkins_basiccomp.debug('Return Message from oracle : company_master_bod_conversion : ' + l_ret_msg1)
        except ValueError as e:
            logger_bulkins_basiccomp.error(e)
            DG2_Common.send_error_mail(e)
        finally:
            self.disconnect_from_db()  # Daily Routine

    @staticmethod
    def cnvrt_dg2_tsv_1min_ashi_into_list4oracle(in_list):
        logger_cnvt_1min2list = logging.getLogger(__name__ + '.cnvrt_dg2_tsv_1min_ashi_into_list4oracle')
        lv_rows = []
        lv_date = ''
        # in_list = [w.replace('-', '') for w in in_list]
        for i, lv_cur in enumerate(in_list):
            if i == 0:
                lv_date = str(lv_cur[0])
            else:
                lv_column_cnt = int(lv_cur[1])
                lv_stock_code = lv_cur[0]
                lv_float_position = float(lv_cur[2])

                for cntr_cur_column in range(lv_column_cnt):
                    lv_cur[4 + 6 * cntr_cur_column] = ''.join(
                        e for e in lv_cur[4 + 6 * cntr_cur_column] if e.isalnum())
                    lv_cur[5 + 6 * cntr_cur_column] = ''.join(
                        e for e in lv_cur[5 + 6 * cntr_cur_column] if e.isalnum())
                    lv_cur[6 + 6 * cntr_cur_column] = ''.join(
                        e for e in lv_cur[6 + 6 * cntr_cur_column] if e.isalnum())
                    lv_cur[7 + 6 * cntr_cur_column] = ''.join(
                        e for e in lv_cur[7 + 6 * cntr_cur_column] if e.isalnum())
                    lv_cur[8 + 6 * cntr_cur_column] = ''.join(
                        e for e in lv_cur[8 + 6 * cntr_cur_column] if e.isalnum())
                    if lv_cur[4 + 6 * cntr_cur_column] and lv_cur[8 + 6 * cntr_cur_column]:
                        lv_date_time_cnvrt = lv_date[0:4] + '-' + lv_date[4:6] + '-' \
                                             + lv_date[6:8] + ' ' + lv_cur[3 + 6 * cntr_cur_column][0:2] \
                                             + ':' + lv_cur[3 + 6 * cntr_cur_column][2:4] + ':00'
                        # close_date_hour special logic
                        lv_hour = int(lv_cur[3 + 6 * cntr_cur_column][0:2])
                        # if lv_hour == 15:
                        #     lv_hour = 14 - 8
                        # else:
                        lv_hour = lv_hour - 8

                        # close_date_et_count special logic
                        lv_time_str = lv_cur[3 + 6 * cntr_cur_column][0:2] + lv_cur[3 + 6 * cntr_cur_column][2:4]
                        lv_time = datetime.strptime(lv_time_str, '%H%M')
                        lv_time_900 = datetime.strptime('08:55', '%H:%M')
                        lv_time_diff = lv_time - lv_time_900
                        lv_time_diff_min = math.floor(lv_time_diff.total_seconds() / 60)
                        lv_et_count = math.floor(lv_time_diff_min / 5)
                        # if lv_et_count == 73:
                        #     lv_et_count = 72
                        lv_cur_row = (lv_stock_code, lv_date_time_cnvrt,  # close_1min_str
                                      int(lv_date),  # close_date
                                      int(lv_date + str(lv_hour)),  # close_date_hour
                                      int(lv_date + str('000' + str(lv_et_count))[-4:]),  # close_date_et_count
                                      int(lv_date[0:6]),  # close_month
                                      int(lv_date[0:4]),  # close_year
                                      float(lv_cur[4 + 6 * cntr_cur_column]) / (pow(10, lv_float_position)),
                                      float(lv_cur[5 + 6 * cntr_cur_column]) / (pow(10, lv_float_position)),
                                      float(lv_cur[6 + 6 * cntr_cur_column]) / (pow(10, lv_float_position)),
                                      float(lv_cur[7 + 6 * cntr_cur_column]) / (pow(10, lv_float_position)),
                                      float(lv_cur[8 + 6 * cntr_cur_column]))
                        lv_rows.append(lv_cur_row)
                        # if lv_stock_code == '6501':
                        #     print(lv_cur_row)

        logger_cnvt_1min2list.info('target file was converted into a list which has row count : %s', len(lv_rows))
        return lv_rows

    @staticmethod
    def cnvrt_dg2_tsv_company_basic_into_list4oracle(in_list):
        logger_cnvt_basic2list = logging.getLogger(__name__ + '.cnvrt_dg2_tsv_company_basic_into_list4oracle')
        lv_rows = []
        for i, lv_cur in enumerate(in_list):
            # remove special character from first line: ord function return integer value in decimal
            lv_date = str("".join(x for x in lv_cur[0] if 31 < ord(x) < 127))
            lv_day_type_in_week = int(lv_cur[1])
            lv_stock_code = lv_cur[2]
            # shift jis does not have this assignment so that remove it.
            lv_stock_name = lv_cur[3].replace('～', '').replace('－', '').replace('∥', '')
            lv_market_code = lv_cur[4]
            lv_market_code_species = lv_cur[5]
            lv_market_segment = lv_cur[6]
            lv_n225_flag = lv_cur[7]
            lv_taisyaku_flag = lv_cur[8]
            lv_volume_unit = lv_cur[9]
            lv_lastday_volume_unit = lv_cur[10] if lv_cur[10] != '-' else 0
            lv_lastday_price = lv_cur[11] if lv_cur[11] != '-' else 0
            lv_lastday_volume = lv_cur[12] if lv_cur[12] != '-' else 0
            # remove special character from first line: ord function return integer value in decimal
            # '-' was removed
            lv_stock_split_ratio = "".join(x for x in lv_cur[13] if ord(x) != 45)
            # remove special character from first line: ord function return integer value in decimal
            # '-' was removed
            lv_trade_minimum_unit = lv_cur[14] if lv_cur[14] != '-' else 0
            lv_price_float_position = "".join(x for x in lv_cur[15] if ord(x) != 45)
            lv_price_ticks_minimum = "".join(x for x in lv_cur[16] if ord(x) != 45)
            lv_min_ashi_max_count = lv_cur[17]

            lv_cur_row = (str(lv_date),
                          str(lv_day_type_in_week),
                          str(lv_stock_code),
                          str(lv_stock_name),
                          float(lv_market_code),
                          float(lv_market_code_species),
                          float(lv_market_segment),
                          float(lv_n225_flag),
                          float(lv_taisyaku_flag),
                          float(lv_volume_unit),
                          float(lv_lastday_volume_unit),
                          float(lv_lastday_price),
                          float(lv_lastday_volume),
                          float(lv_stock_split_ratio),
                          float(lv_trade_minimum_unit),
                          float(lv_price_float_position),
                          float(lv_price_ticks_minimum),
                          float(lv_min_ashi_max_count))
            lv_rows.append(lv_cur_row)
        logger_cnvt_basic2list.info('tsr file was converted into a list which has row count : %s', len(lv_rows))
        return lv_rows

    def gdw_connection_test(self):
        try:
            self.connect_to_db()
            sql = "SELECT USERENV ('language'), sysdate FROM DUAL"
            self.execute(sql)
            rows = self.fetch_all_data()
            for row in rows:
                print(row)
        finally:
            self.disconnect_from_db()
