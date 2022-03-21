# coding=utf-8
import io
import logging
import os
import time
import zipfile
from datetime import datetime
import pathlib
from pathlib import Path
import requests
from configobj import ConfigObj
import settings
from lib.DG2_Common import DG2AccessMode, DG2URLParameterDescriptions, DG2ErrCodesMustStop, \
    DG2ErrCodesTryAgainWithHashCode, \
    DG2ErrCodesTryAgainWithWait3Seconds, read_tsv_file_utf8_into_list, get_directory_list

cur_directory = os.path.dirname(os.path.abspath(__file__))


class DataGet2Driver(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.DataGet2Driver')
        self.lv_error_code = None
        self.wb = None
        self.Dataget2_Access_IP_Address = ''
        self.Dataget2_HashCD = ''
        self.Dataget2_TS_HashCD = ''
        self.Dataget2_Host = ''
        self.Dataget2_Port = ''
        self.Dataget2_ID = ''
        self.Dataget2_PW = ''
        self.Dataget2_TS_Last_Access = ''
        self.uri_for_get_hash_code = ''
        self.uri_for_get_data = ''
        self.uri_for_zip_file_list = ''
        self.uri_for_basic_company_data = ''
        self.uri_for_download_zip_file_wo_filename = \
        self.uri_for_latest_price_list = ''
        self.get_parameter_from_param()
        self.update_uri_w_new_hash_cd()

    @staticmethod
    def url_txt_builder_basic(in_host, in_port):
        lv_url = "http://" + in_host + ":" + in_port + "/"
        return lv_url

    def update_uri_w_new_hash_cd(self):
        self.uri_for_get_hash_code = self.url_txt_builder_basic(self.Dataget2_Host, self.Dataget2_Port) \
                                     + '?userid=' + self.Dataget2_ID + '&passwd=' + self.Dataget2_PW
        self.uri_for_get_data = self.url_txt_builder_basic(self.Dataget2_Access_IP_Address, self.Dataget2_Port) \
                                + '?hashcd=' + self.Dataget2_HashCD
        self.uri_for_zip_file_list = self.uri_for_get_data + DG2URLParameterDescriptions.DataMode.value \
                                     + str(DG2AccessMode.CompressedFileList.value)
        self.uri_for_download_zip_file_wo_filename = self.uri_for_get_data \
                                                     + DG2URLParameterDescriptions.DownloadFileNameMode.value
        self.uri_for_basic_company_data = self.uri_for_get_data + DG2URLParameterDescriptions.DataMode.value \
                                          + str(DG2AccessMode.BasicStockInfoAllStock.value)
        self.uri_for_latest_price_list = self.uri_for_get_data + DG2URLParameterDescriptions.DataMode.value \
                                          + str(DG2AccessMode.LatestPriceAllStock.value)

    def get_parameter_from_param(self):
        logger_param = logging.getLogger(__name__ + '.get_parameter_from_param')
        try:

            lo_config_obj = ConfigObj("AppsConfig.ini")
            self.Dataget2_ID = lo_config_obj['dg2_auth_user_data']['login_id']
            self.Dataget2_PW = lo_config_obj['dg2_auth_user_data']['login_password']
            self.Dataget2_Host = lo_config_obj['dg2_auth_user_authentication_server']['host_name']
            self.Dataget2_Port = lo_config_obj['dg2_auth_user_authentication_server']['port_number']
            self.Dataget2_Access_IP_Address = lo_config_obj['dg2_data_server']['host_ip']
            self.Dataget2_HashCD = lo_config_obj['dg2_data_server']['hash_code']
            self.Dataget2_TS_HashCD = lo_config_obj['dg2_auth_log']['ts_last_hash_code']
            self.Dataget2_TS_Last_Access = lo_config_obj['dg2_data_usr_log']['ts_last_access']
            self.update_uri_w_new_hash_cd()
            # get new hash code if last hash code in 12 hours past
            lv_last_ts_obj = datetime.strptime(self.Dataget2_TS_HashCD, "%Y/%m/%d %H:%M:%S")
            lv_now_ts_obj = datetime.now()
            lv_ts_diff_obj = lv_now_ts_obj - lv_last_ts_obj
            if (lv_ts_diff_obj.days * 86400 + lv_ts_diff_obj.seconds) >= 12 * 60 * 60:  # 12 hour past
                logging.debug('Hash code was expired : %r ..getting new hash code', self.Dataget2_TS_HashCD)
                self.get_new_hash_code_and_ip_address()
            else:
                logger_param.debug('Hash code is valid : %r', self.Dataget2_TS_HashCD)
        except ValueError as err:
            logger_param.error(err.args)
            raise

    def save_parameter_into_file(self):
        logger = logging.getLogger(__name__ + '.save_parameter_into_file')
        try:
            lo_config_obj = ConfigObj("AppsConfig.ini")
            lo_config_obj['dg2_data_usr_log']['ts_last_access'] = self.Dataget2_TS_Last_Access
            lo_config_obj['dg2_auth_log']['ts_last_hash_code'] = self.Dataget2_TS_HashCD
            lo_config_obj['dg2_data_server']['host_ip'] = self.Dataget2_Access_IP_Address
            lo_config_obj['dg2_data_server']['hash_code'] = self.Dataget2_HashCD
            lo_config_obj.write()
        except ValueError as err:
            logger.error(err.args)
            raise

    def get_data(self, in_uri, **kwargs):
        logger_getdata = logging.getLogger(__name__ + '.get_data')
        try:
            # Check if there is 3 sec intervals
            lv_last_ts_obj = datetime.strptime(self.Dataget2_TS_Last_Access, "%Y/%m/%d %H:%M:%S")
            lv_now_ts_obj = datetime.now()
            lv_ts_diff_obj = lv_now_ts_obj - lv_last_ts_obj

            logger_getdata.debug('calling get_data : %s', in_uri)
            if lv_ts_diff_obj.seconds <= 3:
                logger_getdata.debug('connection interval is too short.. wait next 4 second to retry')
                time.sleep(4)
                logger_getdata.debug(' finished wait for 4 second... retrying get the data')
            self.wb = requests.get(in_uri, **kwargs)
            lv_ret_text = self.wb.text
            self.Dataget2_TS_Last_Access = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            self.save_parameter_into_file()
            # Error Handling from Dataget2
            if lv_ret_text.startswith('@ErrorCode:'):
                self.lv_error_code = lv_ret_text[11:16]
                if self.lv_error_code in DG2ErrCodesMustStop:
                    lv_log_txt = self.lv_error_code + ' : ' + DG2ErrCodesMustStop[self.lv_error_code]
                    logger_getdata.error('DataGet2 returned error : %r', lv_log_txt)
                    raise ValueError('DataGet2 returned error : ' + self.lv_error_code + ' : '
                                     + DG2ErrCodesMustStop[self.lv_error_code])
                elif self.lv_error_code in DG2ErrCodesTryAgainWithHashCode:
                    self.get_new_hash_code_and_ip_address()
                    time.sleep(3)
                    self.get_data(in_uri)

                elif self.lv_error_code in DG2ErrCodesTryAgainWithWait3Seconds:
                    logger_getdata.warning('DG2 error : 60003 : connection interval is too short. try again after 3seconds')
                    logger_getdata.debug('Wait 3 seconds')
                    time.sleep(3)
                    self.get_data(in_uri)

            return self.wb

        except ValueError as err:
            logger_getdata.error(err.args)
            raise

    def get_new_hash_code_and_ip_address(self):
        logger_new_hash = logging.getLogger(__name__ + '.get_new_hash_code_and_ip_address')
        try:
            lo_mixed = self.get_data(self.uri_for_get_hash_code)
            lv_arr_r_txt = lo_mixed.text.split("	")

            self.Dataget2_HashCD = lv_arr_r_txt[0]
            self.Dataget2_TS_HashCD = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            self.Dataget2_Access_IP_Address = lv_arr_r_txt[1]
            self.update_uri_w_new_hash_cd()
            self.save_parameter_into_file()
            logger_new_hash.info('Got new hash code from DG2')
        except ValueError as err:
            logger_new_hash.error(err.args)
            raise

    def get_target_zip_file(self, in_target_file_name):
        logger_target_zip = logging.getLogger(__name__ + '.get_target_zip_file')

        lv_url_text = self.uri_for_download_zip_file_wo_filename + in_target_file_name
        logger_target_zip.debug('access url for getting zip : %r', lv_url_text)
        lo_requests = self.get_data(lv_url_text, stream=True)

        lo_zip_obj = zipfile.ZipFile(io.BytesIO(lo_requests.content))
        # z.write(get_cur_dir_abs_path() + '/data/' + in_target_file_name, 'w')

        # Switch save-destination by file name
        lv_target_file_path_from_base = ''
        if in_target_file_name.startswith('min') and in_target_file_name.endswith('s'):
            lv_target_file_path_from_base = 'data/ohlcv/1min'
        elif in_target_file_name.startswith('day') and in_target_file_name.endswith('s'):
            if in_target_file_name == 'daypastdatas':
                lv_target_file_path_from_base = 'data/ohlcv/eod_index'
            else:
                lv_target_file_path_from_base = 'data/ohlcv/eod'
        elif in_target_file_name.startswith('wek') and in_target_file_name.endswith('s'):
            if in_target_file_name == 'wekpastdatas':
                lv_target_file_path_from_base = 'data/ohlcv/eow_index'
            else:
                lv_target_file_path_from_base = 'data/ohlcv/eow'
        elif in_target_file_name.startswith('mon') and in_target_file_name.endswith('s'):
            if in_target_file_name == 'monpastdatas':
                lv_target_file_path_from_base = 'data/ohlcv/eom_index'
            else:
                lv_target_file_path_from_base = 'data/ohlcv/eom'
        elif in_target_file_name.startswith('mar') and in_target_file_name.endswith('s'):
            lv_target_file_path_from_base = 'data/shinyozan'
        elif in_target_file_name.startswith('cor') and in_target_file_name.endswith('s'):
            lv_target_file_path_from_base = 'data/ohlcv/correction'
        lv_zip_extract_file_loc = os.path.join(settings.PROJECT_ROOT_DIR, lv_target_file_path_from_base,
                                               in_target_file_name)
        logger_target_zip.debug('zip file was saved and extracted to location : %r', lv_zip_extract_file_loc)
        lo_zip_obj.extractall(lv_zip_extract_file_loc)
        lo_zip_obj.close()

    def save_dg2_zip_files_list_2_local_file(self):
        logger_zip_list = logging.getLogger(__name__ + '.save_dg2_zip_files_list_2_local_file')
        lv_url_text = self.uri_for_zip_file_list
        logger_zip_list.debug('access url for getting a file which include a list of zip file  : %r', lv_url_text)
        lo_ret = self.get_data(lv_url_text)
        l_ret_text_2 = lo_ret.text.strip().replace('\n', '')

        lo_file = open('DG2_zip_files_list', 'w', encoding='utf-8')
        lo_file.write(l_ret_text_2)
        lo_file.close()

    def save_dg2_latest_price_list_2_local_file(self):
        logger_price_list = logging.getLogger(__name__ + '.save_dg2_latest_price_list_2_local_file')
        lv_url_text = self.uri_for_latest_price_list
        logger_price_list.debug('access url for getting a file which include a list of price file  : %r', lv_url_text)
        lo_ret = self.get_data(lv_url_text)
        l_ret_text_2 = lo_ret.text.strip().replace('\n', '')
        l_ret_text_3 = l_ret_text_2.replace('\x02', '')
        l_ret_text_4 = l_ret_text_3.replace('\x03', '')
        l_ret_text_5 = l_ret_text_4.replace('-', '')

        #Try: Removing empty line from text Add Hideki '20191002
        #https://stackoverflow.com/questions/1140958/whats-a-quick-one-liner-to-remove-empty-lines-from-a-python-string
        # l_ret_text_6 = os.linesep.join([s for s in l_ret_text_5.splitlines() if s])
        # https: // codereview.stackexchange.com / questions / 145126 / open - a - text - file - and -remove - any - blank - lines
        '\n'.join(filter(lambda x: x.strip(), l_ret_text_5.split('\n')))

        lv_file_name_txt = 'DG2_latest_price_list.txt'
        lv_save_file_txt = pathlib.PurePosixPath(Path.cwd(),
                                                 'data',
                                                 lv_file_name_txt)

        lo_file = open(lv_save_file_txt, 'w+', encoding='utf-8')
        lo_file.write(l_ret_text_5)
        lo_file.close()
        logger_price_list.debug('company_basic_data file was saved at : ' + str(lv_save_file_txt))

        return lv_save_file_txt


    def save_dg2_latest_price_list_2_array(self):
        logger_price_list = logging.getLogger(__name__ + '.save_dg2_latest_price_list_2_local_file')
        lv_url_text = self.uri_for_latest_price_list
        logger_price_list.debug('access url for getting a file which include a list of price file  : %r', lv_url_text)
        l_ret_text_2 = self.get_data(lv_url_text)
        l_ret_text_3 = l_ret_text_2.text.replace('\x02', '')
        l_ret_text_4 = l_ret_text_3.replace('\x03', '')
        l_ret_text_5 = l_ret_text_4.replace('-', '')
        lv_array = l_ret_text_5.split('\r\n')
        ret_array = []
        for occr_line in lv_array:
            occr_arr = occr_line.split('\t')
            ret_array.append(occr_arr)
        return ret_array

    def save_dg2_company_basic_data_2_local_file_w_timestamp(self):
        logger_cbda_lc = logging.getLogger(__name__ + '.save_dg2_company_basic_data_2_local_file_w_timestamp')
        try:
            lv_url_text = self.uri_for_basic_company_data
            logger_cbda_lc.debug('access url for basic company data : %r', lv_url_text)
            lo_ret = self.get_data(lv_url_text)
            l_ret_text_2 = lo_ret.text.strip().replace('\n', '')
            lv_file_name_txt = 'DG2_basic_company_data_' + datetime.now().strftime("%Y%m%d") + '.txt'
            lv_save_file_txt = os.path.join(settings.PROJECT_ROOT_DIR,
                                            'data',
                                            'company_basic_data',
                                            lv_file_name_txt)

            lo_file = open(lv_save_file_txt, 'w', encoding='utf-8')
            lo_file.write(l_ret_text_2)
            lo_file.close()
            logger_cbda_lc.debug('company_basic_data file was saved at : ' + lv_save_file_txt)
        except ValueError as err:
            logger_cbda_lc.error(err.args)

    def save_dg2_company_basic_data_2_local_file(self):
        logger_cbda_lc = logging.getLogger(__name__ + '.save_dg2_company_basic_data_2_local_file_w_timestamp')
        try:
            lv_url_text = self.uri_for_basic_company_data
            logger_cbda_lc.debug('access url for basic company data : %r', lv_url_text)
            lo_ret = self.get_data(lv_url_text)
            l_ret_text_2 = lo_ret.text.strip().replace('\n', '')
            l_ret_text_3 = l_ret_text_2.replace('\x02', '')
            l_ret_text_4 = l_ret_text_3.replace('\x03', '')
            l_ret_text_5 = l_ret_text_4.replace('-', '')

            # Try: Removing empty line from text Add Hideki '20191002
            # https://stackoverflow.com/questions/1140958/whats-a-quick-one-liner-to-remove-empty-lines-from-a-python-string
            # l_ret_text_5 = os.linesep.join([s for s in l_ret_text_4.splitlines() if s])
            # l_ret_text_6 = filter(lambda x: x.strip(), l_ret_text_5)
            '\n'.join(filter(lambda x: x.strip(), l_ret_text_5.split('\n')))

            lv_file_name_txt = 'DG2_basic_company_data.txt'
            lv_save_file_txt = pathlib.PurePosixPath(Path.cwd(),
                                            'data',
                                            lv_file_name_txt)

            lo_file = open(lv_save_file_txt, 'w+', encoding='utf-8')
            lo_file.write(l_ret_text_5)
            lo_file.close()
            logger_cbda_lc.debug('company_basic_data file was saved at : ' + str(lv_save_file_txt))
            return lv_save_file_txt

        except ValueError as err:
            logger_cbda_lc.error(err.args)


    def get_all_dg2_data_file_to_local_pc(self):
        logger_alldg2 = logging.getLogger(__name__ + '.get_all_dg2_data_file_to_local_pc')
        lv_save_file_abs = os.path.join(settings.PROJECT_ROOT_DIR, 'DG2_zip_files_list')
        lv_csv_list = read_tsv_file_utf8_into_list(lv_save_file_abs)

        logger_alldg2.debug('Finding download target files within %r', lv_save_file_abs)
        lv_dir_1min = os.path.join(settings.PROJECT_ROOT_DIR, 'data', 'ohlcv', '1min')
        # Check whether the specified path exists or not
        is_exist_1min = os.path.exists(lv_dir_1min)
        if not is_exist_1min:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_1min)

        lv_dg2_zip_file_list_1min = []
        lv_dir_1min_list = get_directory_list(lv_dir_1min)

        for row in lv_csv_list:
            if len(row) >= 2 and row[1].startswith('min') and row[1].endswith('s'):
                lv_dg2_zip_file_list_1min.append(row[1])

        lv_dir_eod = os.path.join(settings.PROJECT_ROOT_DIR, 'data', 'ohlcv', 'eod')
        # Check whether the specified path exists or not
        is_exist_eod = os.path.exists(lv_dir_eod)
        if not is_exist_eod:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_eod)

        lv_dg2_zip_file_list_eod = []
        lv_dir_eod_list = get_directory_list(lv_dir_eod)
        for row in lv_csv_list:
            if len(row) >= 2 and row[1].startswith('day') and row[1].endswith('s'):
                lv_dg2_zip_file_list_eod.append(row[1])

        lv_dir_eow = os.path.join(settings.PROJECT_ROOT_DIR, 'data', 'ohlcv', 'eow')
        # Check whether the specified path exists or not
        is_exist_eow = os.path.exists(lv_dir_eow)
        if not is_exist_eow:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_eow)

        lv_dg2_zip_file_list_eow = []
        lv_dir_eow_list = get_directory_list(lv_dir_eow)
        for row in lv_csv_list:
            if len(row) >= 2 and row[1].startswith('wek') and row[1].endswith('s'):
                lv_dg2_zip_file_list_eow.append(row[1])

        lv_dir_eom = os.path.join(settings.PROJECT_ROOT_DIR, 'data', 'ohlcv', 'eom')
        # Check whether the specified path exists or not
        is_exist_eom = os.path.exists(lv_dir_eom)
        if not is_exist_eom:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_eom)

        lv_dg2_zip_file_list_eom = []
        lv_dir_eom_list = get_directory_list(lv_dir_eom)
        for row in lv_csv_list:
            if len(row) >= 2 and row[1].startswith('mon') and row[1].endswith('s'):
                lv_dg2_zip_file_list_eom.append(row[1])

        lv_dir_shinyozan = os.path.join(settings.PROJECT_ROOT_DIR, 'data', 'shinyozan')
        # Check whether the specified path exists or not
        is_exist_shinyozan = os.path.exists(lv_dir_shinyozan)
        if not is_exist_shinyozan:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_shinyozan)

        lv_dg2_zip_file_list_shinyozan = []
        lv_dir_shinyozan_list = get_directory_list(lv_dir_shinyozan)
        for row in lv_csv_list:
            if len(row) >= 2 and row[1].startswith('mar') and row[1].endswith('s'):
                lv_dg2_zip_file_list_shinyozan.append(row[1])

        lv_dir_correction = os.path.join(settings.PROJECT_ROOT_DIR, 'data', 'ohlcv', 'correction')
        # Check whether the specified path exists or not
        is_exist_correction = os.path.exists(lv_dir_correction)
        if not is_exist_correction:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_correction)

        lv_dg2_zip_file_list_correction = []
        lv_dir_correction_list = get_directory_list(lv_dir_correction)
        for row in lv_csv_list:
            if len(row) >= 2 and row[1].startswith('cor') and row[1].endswith('s'):
                lv_dg2_zip_file_list_correction.append(row[1])

        lv_dir_imported = os.path.join(settings.PROJECT_ROOT_DIR, 'data_imported', 'ohlcv', '1min')
        # Check whether the specified path exists or not
        is_exist_imported = os.path.exists(lv_dir_imported)
        if not is_exist_imported:
            # Create a new directory because it does not exist
            os.makedirs(lv_dir_imported)

        # 1min only imported to GDW so that check imported folder
        lv_imported_files_1min_list = []
        for lv_filename_w_ext in os.listdir(lv_dir_imported):
            lv_imported_files_1min_list.append(os.path.splitext(lv_filename_w_ext)[0])

        # Download target 1min files
        lv_diff_1min_list = list(set(lv_dg2_zip_file_list_1min) - set(lv_dir_1min_list)
                                 - set(lv_imported_files_1min_list))
        for row_1min in lv_diff_1min_list:
            logger_alldg2.info('Found target file to download : name :' + row_1min)
            self.get_target_zip_file(row_1min)
        # Download target EOD files
        lv_diff_eod_list = list(set(lv_dg2_zip_file_list_eod) - set(lv_dir_eod_list))
        for row_eod in lv_diff_eod_list:
            logger_alldg2.info('Found target file to download : name :' + row_eod)
            self.get_target_zip_file(row_eod)
        # Download target EOW files
        lv_diff_eow_list = list(set(lv_dg2_zip_file_list_eow) - set(lv_dir_eow_list))
        for row_eow in lv_diff_eow_list:
            logger_alldg2.info('Found target file to download : name :' + row_eow)
            self.get_target_zip_file(row_eow)
        # Download target EOM files
        lv_diff_eom_list = list(set(lv_dg2_zip_file_list_eom) - set(lv_dir_eom_list))
        for row_eom in lv_diff_eom_list:
            logger_alldg2.info('Found target file to download : name :' + row_eom)
            self.get_target_zip_file(row_eom)
        # Download target shinyozan files
        lv_diff_shinyozan_list = list(set(lv_dg2_zip_file_list_shinyozan) - set(lv_dir_shinyozan_list))
        for row_shinyozan in lv_diff_shinyozan_list:
            logger_alldg2.info('Found target file to download : name :' + row_shinyozan)
            self.get_target_zip_file(row_shinyozan)
        # Download target correction files
        lv_diff_correction_list = list(set(lv_dg2_zip_file_list_correction) - set(lv_dir_correction_list))
        for row_correction in lv_diff_correction_list:
            logger_alldg2.info('Found target file to download : name :' + row_correction)
            self.get_target_zip_file(row_correction)

    def bach__synch_local_all_zip_file_to_latest(self):
        try:
            self.logger = logging.getLogger(__name__ + '.bach__synch_local_all_zip_file_to_latest')
            self.save_dg2_zip_files_list_2_local_file()
            self.save_dg2_company_basic_data_2_local_file_w_timestamp()
            self.get_all_dg2_data_file_to_local_pc()

        except ValueError as err:
            self.logger.error(err.args)

