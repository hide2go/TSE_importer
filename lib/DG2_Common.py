# coding=utf-8
import csv
import os
from enum import Enum
import logging
import settings
import zipfile
import codecs
import yagmail
import datetime
from datetime import timedelta


class DG2AccessMode(Enum):
    def __str__(self):
        return str(self.value)

    BasicStockInfoAllStock = 10
    LatestPriceAllStock = 11
    LatestPricePlusKehai50 = 12
    LatestPricePlusKehaiPlusHunAshi4 = 13
    RegisteredCompanyNameAllStock = 19
    CompressedFileList = 20


class DG2URLParameterDescriptions(Enum):
    def __str__(self):
        return str(self.value)

    DataMode = '&charmd=UTF-8&linefd=CRLF&datamd='
    DownloadFileNameMode = '&dlname='


class DG2AccessPurpose(Enum):
    GetHashCode = 1,
    GetData = 2


def get_file_list(path):
    file_list = []
    for (root, dirs, files) in os.walk(path):
        for file in files:
            # パスセパレータは\\より/の方が好きなので置換
            file_list.append(os.path.join(root, file).replace("\\", "/"))

    return file_list


# this is in ./lib so need to get parent directory
# def get_cur_abs_path():
#     l_ret_txt = os.path.abspath(__file__)
#     return l_ret_txt


def read_tsv_file_utf8_into_list(in_file_name_w_path):
    logger = logging.getLogger(__name__ + '.read_tsv_file_into_list')
    try:
        # TODO : Use codec.open command then specify the encode to utf-8
        # with codecs.open(in_file_name_w_path, 'r', encoding='Shift-JIS', errors='ignore') as f_object:
        with codecs.open(in_file_name_w_path, encoding='utf-8') as f_object:
            logger.debug('reading tsv file : %r', in_file_name_w_path)
            f_csv = csv.reader(f_object,  delimiter='\t')
            l_ret_list = []
            for lv_row in f_csv:
                # REMOVE End of Text
                if ord(lv_row[0][0]) != 3: l_ret_list.append(lv_row)
            return l_ret_list
    except ValueError as err:
        logger.error(err.args)
        send_error_mail(err.args)


def get_directory_list(in_dir_path):
    lv_names = os.listdir(in_dir_path)
    l_ret_list = list(lv_names)

    return l_ret_list


def create_empty_zip_archive(in_arch_file_name_w_full_path_and_ext):
    lv_empty_zip_data = b'PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
                     b'\x00\x00\x00\x00\x00'
    with open(in_arch_file_name_w_full_path_and_ext, 'wb') as zip:
        zip.write(lv_empty_zip_data)


def add_file_to_zip_archive(in_target_arch_fila_name_w_full_path_w_ext, in_target_file_to_be_add_w_full_path_and_ext):
    function_logger = logging.getLogger(__name__ + '.add_file_to_zip_archive')
    function_logger.debug('in_target_arch_fila_name_w_full_path_w_ext : %r',in_target_arch_fila_name_w_full_path_w_ext)
    lo_zipfile = zipfile.ZipFile(in_target_arch_fila_name_w_full_path_w_ext, "a")
    lo_zipfile.write(in_target_file_to_be_add_w_full_path_and_ext,
                     os.path.basename(in_target_file_to_be_add_w_full_path_and_ext),
                     compress_type=zipfile.ZIP_DEFLATED)


def send_error_mail(in_text_message):
    function_logger = logging.getLogger(__name__ + '.send_error_mail')
    function_logger.info('Send mail with CONTENTS : %s', in_text_message)
    yagmail.SMTP('ohzoro.system.notify@gmail.com').send('ohzoro.system.notify@icloud.com',
                                                        '【Error with DataGet2_Crawler】',
                                                        contents=yagmail.raw(in_text_message))


def send_notice_mail(in_title_txt, in_text_message):
    # create logger object for this module
    function_logger = logging.getLogger(__name__ + '.send_notice_mail')
    function_logger.debug('Send mail with title : %r, contents : %s', in_title_txt, in_text_message)
    # TODO : Use codec.open command then specify the encode to utf-8
    with open(os.path.join(settings.PROJECT_ROOT_DIR, 'log', 'app_info.log'), 'r') as myfile:
        lv_log_info_data = myfile.read()
    yagmail.SMTP('ohzoro.system.notify@gmail.com').send('ohzoro.system.notify@icloud.com',
                                                        in_title_txt,
                                                        contents=yagmail.raw(in_text_message
                                                                        + '\n\n\n' + lv_log_info_data))


def get_xdays_before_yyyymmdd(in_base_date, in_date_to_subtract):
    function_logger = logging.getLogger(__name__ + '.get_xdays_before_yyyymmdd')
    function_logger.debug('start to calculate for date to sugtract')
    lv_date = datetime.datetime.strptime(in_base_date, '%Y%m%d').date()
    function_logger.debug('lv_date : %r', lv_date)
    lv_subtracted = lv_date - timedelta(days=in_date_to_subtract)
    function_logger.debug('lv_subtracted : %r', lv_subtracted)
    lv_ret = lv_subtracted.strftime('%Y%m%d')
    function_logger.debug('lv_ret : %r', lv_ret)
    return lv_ret


DG2ErrCodesMustStop = {
    '60001': 'ユーザー認証に失敗しました。 ユーザーIDまたはパスワードに誤りがあります。',
    '60010': '要求コードの取得に失敗しました。 codelsパラメータで指定される文字列の書式を確認してください',
    '60011': 'サーバーでデータ通信に関する障害が発生しています。 再取得を行っても同じエラーが繰り返される場合、HPのおらせをご確認ください。',
    '60020': '指定のファイルは存しません。 dlnameラメータで指定したファイルが存しないか、取得できないデータプランでの契約です。 ',
    '60097': 'メンナンス中です。 詳細はHPのおらせを確認ください。 ',
    '60098': '要求モードが不明です。要求パラメータを確認してください。'
}
DG2ErrCodesTryAgainWithHashCode = {
    '60002': '認証コードの照合に失敗しました。 認証コードに誤りがあるか期限が切れているがあります。 再度、認証処理を行てください。 ',
}
DG2ErrCodesTryAgainWithWait3Seconds = {
    '60003': 'データ取得のセッション間隔が短すぎます。 接続してデータモードまたはダウンロードの要求が行れました。 データの取得に際しては３秒以上の間隔をおいてください。 ',
}