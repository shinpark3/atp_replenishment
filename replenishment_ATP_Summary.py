# -*- coding: utf-8 -*-#
from __future__ import division
import argparse
import datetime as dt
import os
import json
# from noattach import NoAttach
# import sendmail
# from pyspark.sql.types import *
# import pyspark
# from bi_address_book import send_mail
import re
import time
import warnings

import pandas as pd
# from unidecode import unidecode
import sql_queries
from helper_functions_correct import append_df_to_excel, write_to_csv
from helper_functions_correct import Regional_ATP_rewrite, Regional_Replenishment_rewrite, save_report_path, \
    load_report_path
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from process_ATP_correct_v3 import process_atp_df, process_replenishment_df, process_raw_data_frames

# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext, HiveContext, SparkSession
# from pyspark.sql.column import Column
# from pyspark.sql.dataframe import DataFrame
# from pyspark.sql.functions import udf, when, lit, asc, broadcast
# from pyspark.sql import Row
# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive
# import unicode

warnings.simplefilter("ignore")


def main(today_date, country_list, data_queried, files_processed, replenishment_done, reports_written,
         summary_completed):
    # Processing for each country
    if replenishment_done:
        files_processed = True
        data_queried = True
    if reports_written:
        replenishment_done = True
        files_processed = True
        data_queried = True
    for country in country_list:

        start = time.time()

        # try:
        print('Processing for Country: ', country)

        ############
        TODAY = today_date

        ###################
        print("TODAY's DATE: {}".format(TODAY))

        date_format = '%Y-%m-%d_%H%M%Shrs'
        file_date = dt.datetime.today().strftime(date_format)
        country_folder_path = './' + country + '/'

        if not os.path.exists(country_folder_path):
            os.makedirs(country_folder_path)

        input_file_path = "{}input_files".format(country_folder_path)

        if not os.path.exists(input_file_path):
            os.makedirs(input_file_path)
        print("INPUT FILE PATH:", input_file_path)

        four_weeks_ago_days = 28

        two_weeks_ago_days = 14

        yesterday_days = 1

        str_date_format = "%Y-%m-%d"

        four_weeks_ago_date = (TODAY - dt.timedelta(days=four_weeks_ago_days))
        str_four_weeks_ago_date = four_weeks_ago_date.strftime(str_date_format)

        two_weeks_ago_date = (TODAY - dt.timedelta(days=two_weeks_ago_days))
        str_two_weeks_ago_date = two_weeks_ago_date.strftime(str_date_format)

        yesterday_date = (TODAY - dt.timedelta(days=yesterday_days))
        str_yesterday_date = yesterday_date.strftime(str_date_format)

        file_keys = ['brand', 'cogs', 'new_color', 'replenishment', 'ext', 'pr_po_inbound', 'item_profile_history',
                     'cat_cluster', 'cfs_wk2', 'cfs_wk4', 'cfs_ytd', 'purchase_type']

        print("Reading files from CSV...")
        file_names = {}
        for file_key in file_keys:
            print('{}/{}_{}.csv'.format(input_file_path, country, file_key))
            file_names[file_key] = '{}/{}_{}.csv'.format(input_file_path, country, file_key)

        print("Querying Database for items....")
        if not data_queried:
            cogs_color = sql_queries.get_cogs_color(country, TODAY)
            cogs = cogs_color.select('sku_id', 'grass_date', 'cogs')
            color = cogs_color.select('sku_id', 'color', 'stock_on_hand', 'grass_date')
            write_to_csv(cogs, file_names['cogs'])
            write_to_csv(sql_queries.get_purchase_type(country), file_names['purchase_type'])
            write_to_csv(
                sql_queries.get_brands(country, str_four_weeks_ago_date, str_two_weeks_ago_date, str_yesterday_date),
                file_names['brand'])
            write_to_csv(color, file_names['new_color'])
            write_to_csv(sql_queries.get_pr_po_inbound(country), file_names['pr_po_inbound'])
            write_to_csv(sql_queries.get_cat_map(country), file_names['cat_cluster'])
            write_to_csv(sql_queries.get_cfs(country, TODAY, str_four_weeks_ago_date), file_names['cfs_wk4'])
            write_to_csv(sql_queries.get_cfs(country, TODAY, str_two_weeks_ago_date), file_names['cfs_wk2'])
            write_to_csv(sql_queries.get_cfs(country, TODAY, str_yesterday_date), file_names['cfs_ytd'])
            write_to_csv(sql_queries.get_item_profile(country), file_names['item_profile_history'])
            write_to_csv(sql_queries.get_replenishment_ext(country, str_four_weeks_ago_date, str_two_weeks_ago_date,
                                                           str_yesterday_date), file_names['ext'])
            write_to_csv(sql_queries.get_replenishment_core(country, TODAY), file_names['replenishment'])

            print("Data Queried and converted to csv files in 'input_file' directory!")

        # #
        if not replenishment_done:
            print("Reading input files after conversion...")

            brand = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'brand.csv'), encoding='utf-8-sig',
                                index_col=False)
            print('brand!')
            cogs = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'cogs.csv'), encoding='utf-8-sig',
                               index_col=False)
            print('cogs!')
            new_color = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'new_color.csv'),
                                    encoding='utf-8-sig', index_col=False)
            print('new_color!')
            replenishment = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'replenishment.csv'),
                                        encoding='utf-8-sig', index_col=False)
            print('replenishment!')
            ext = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'ext.csv'), encoding='utf-8-sig',
                              index_col=False)
            pr_po_inbound = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'pr_po_inbound.csv'),
                                        encoding='utf-8-sig', index_col=False)
            item_profile_history = pd.read_csv(
                '{}'.format(input_file_path + '/' + country + '_' + 'item_profile_history.csv'), encoding='utf-8-sig',
                index_col=False)
            cat_cluster = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'cat_cluster.csv'),
                                      encoding='utf-8-sig', index_col=False)
            cfs_wk2 = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'cfs_wk2.csv'),
                                  encoding='utf-8-sig', index_col=False)
            cfs_wk4 = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'cfs_wk4.csv'),
                                  encoding='utf-8-sig', index_col=False)
            cfs_ytd = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'cfs_ytd.csv'),
                                  encoding='utf-8-sig', index_col=False)
            purchase_type = pd.read_csv('{}'.format(input_file_path + '/' + country + '_' + 'purchase_type.csv'),
                                        encoding='utf-8-sig', index_col=False)

            print("All files read!")

            ext = ext.sort_values('sourcing_status').drop_duplicates(['sku_id', 'grass_date'], keep='first')

            purchase_type, ext, replenishment, pr_po_inbound, new_color = process_raw_data_frames(purchase_type, ext,
                                                                                                  brand,
                                                                                                  replenishment,
                                                                                                  pr_po_inbound,
                                                                                                  new_color)

            print('Dataframes processed!')

        if not files_processed:
            print('Creating ATP DataFrames/CSV Files...')
            wk_4_atp_df, wk_4_fn, wk_4_cogs = process_atp_df(country, 4, four_weeks_ago_date, pr_po_inbound,
                                                             replenishment,
                                                             cat_cluster, cogs, brand, item_profile_history, new_color,
                                                             ext,
                                                             cfs_wk4, purchase_type, prev_iteration_atp_df=None)

            wk_2_atp_df, wk_2_fn, wk_2_cogs = process_atp_df(country, 2, two_weeks_ago_date, pr_po_inbound,
                                                             replenishment,
                                                             cat_cluster, cogs, brand, item_profile_history, new_color,
                                                             ext,
                                                             cfs_wk2, purchase_type,
                                                             prev_iteration_atp_df=wk_4_atp_df)

            ytd_atp_df, ytd_fn, ytd_cogs = process_atp_df(country, 0, yesterday_date, pr_po_inbound, replenishment,
                                                          cat_cluster, cogs, brand, item_profile_history, new_color,
                                                          ext,
                                                          cfs_ytd, purchase_type, prev_iteration_atp_df=wk_2_atp_df)
            print('All ATP Dataframes created and written to CSV!')

        def process_dfs_before_write(df):
            df['last_purchasable_date'] = pd.to_datetime(df['last_purchasable_date'], format='%Y-%m-%d')
            df['last_purchasable_date'].fillna(df['last_purchasable_date'].min(), inplace=True)
            df = df.applymap(lambda x: re.sub(ILLEGAL_CHARACTERS_RE, '', x) if isinstance(x, str) else x)
            return df

        if not replenishment_done:
            wk_4_fn = "{}/w-4_product_info.csv".format(country)
            wk_2_fn = "{}/w-2_product_info.csv".format(country)
            ytd_fn = "{}/ytd_product_info.csv".format(country)
            ytd_cogs = pd.read_csv("{}/ytd_last_cogs.csv".format(country))
            wk_2_atp_df = pd.read_csv(wk_2_fn, na_values="n.a.", encoding='utf-8-sig')
            wk_2_write = process_dfs_before_write(wk_2_atp_df)

            wk_4_atp_df = pd.read_csv(wk_4_fn, na_values="n.a.", encoding='utf-8-sig')

            ytd_atp_df = pd.read_csv(ytd_fn, na_values="n.a.", encoding='utf-8-sig')
            ytd_write = process_dfs_before_write(ytd_atp_df)

            print("Writing for Replenishment Progress Tracker...")
            replenishment_prod_info, inbounded_tab, rop_to_pr_tab = process_replenishment_df(country,
                                                                                             yesterday_date,
                                                                                             four_weeks_ago_date,
                                                                                             purchase_type,
                                                                                             pr_po_inbound,
                                                                                             replenishment,
                                                                                             ytd_cogs,
                                                                                             item_profile_history,
                                                                                             ext,
                                                                                             wk_4_atp_df
                                                                                             )

        if not reports_written:
            if replenishment_done:
                new_folder = './' + country + '/input_files/'
                replenishment_prod_info = pd.read_csv(new_folder + 'replenishment_tab_v4.csv', encoding='utf-8-sig',
                                                      index_col=0)
                inbounded_tab = pd.read_csv(new_folder + "inbounded_tab.csv", encoding='utf-8-sig', index_col=0)
                rop_to_pr_tab = pd.read_csv(new_folder + "rop_to_pr.csv", encoding='utf-8-sig', index_col=0)
                ytd_fn = "{}/ytd_product_info.csv".format(country)
                ytd_atp_df = pd.read_csv(ytd_fn, na_values="n.a.", encoding='utf-8-sig')
                ytd_write = process_dfs_before_write(ytd_atp_df)
                wk_2_fn = "{}/w-2_product_info.csv".format(country)
                wk_2_atp_df = pd.read_csv(wk_2_fn, na_values="n.a.", encoding='utf-8-sig')
                wk_2_write = process_dfs_before_write(wk_2_atp_df)

            r_write = replenishment_prod_info.applymap(
                lambda x: re.sub(ILLEGAL_CHARACTERS_RE, '', x) if isinstance(x, str) or isinstance(x, unicode) else x)
            r_write = r_write.applymap(lambda x: x.encode('unicode_escape').
                                       decode('utf-8-sig') if isinstance(x, str) else x)
            wk_2_write = wk_2_write.applymap(
                lambda x: re.sub(ILLEGAL_CHARACTERS_RE, '', x) if isinstance(x, str) or isinstance(x, unicode) else x)
            wk_2_write = wk_2_write.applymap(lambda x: x.encode('unicode_escape').
                                             decode('utf-8-sig') if isinstance(x, str) else x)
            ytd_write = ytd_write.applymap(
                lambda x: re.sub(ILLEGAL_CHARACTERS_RE, '', x) if isinstance(x, str) or isinstance(x, unicode) else x)
            ytd_write = ytd_write.applymap(lambda x: x.encode('unicode_escape').
                                           decode('utf-8-sig') if isinstance(x, str) else x)
            r_write_dict = {
                "SKU detail - Replenishment": {
                    'df': r_write,
                    'dates': {
                        'four_weeks_ago': four_weeks_ago_date,
                        'yesterday': yesterday_date
                    }
                },
                "SKUs Inbounded - PR PO Detail": inbounded_tab,
                "SKU Replenished - PR ROP detail": rop_to_pr_tab
            }

            r_file_name = "./templates/Replenishment_Performance_Tracker_Template.xlsx"
            r_output_file = "{}{}_{}_{}.xlsx".format(country_folder_path, country, file_date,
                                                     r_file_name[r_file_name.rfind('/') + 1:
                                                                 r_file_name.rfind("Template") - 1])
            # save the reports path into json file
            Replenishment_reports_path = load_report_path('Replenishment_reports_path')
            Replenishment_reports_path[country] = r_output_file
            save_report_path('Replenishment_reports_path', Replenishment_reports_path)

            filename = "./templates/ATP_Template.xlsx"
            output_file = "{}{}_{}_ATP Report.xlsx".format(country_folder_path, country, file_date)
            ATP_reports_path = load_report_path('ATP_reports_path')
            ATP_reports_path[country] = output_file
            save_report_path('ATP_reports_path', ATP_reports_path)

            stock_sync_columns = ['cluster', 'category', 'shopid', 'shop_name', 'supplier_id', 'supplier_name',
                                  'shopee_merchANDiser', 'brand', 'sku_id', 'ItemName', 'ModelName', 'purchase_type',
                                  'stock_on_hand', 'organic_sales', 'be_stock']
            oos_columns = ['cluster', 'category', 'shopid', 'shop_name', 'supplier_id', 'supplier_name',
                           'shopee_merchANDiser', 'brand', 'sku_id', 'ItemName', 'ModelName', 'purchase_type',
                           'stock_on_hand', 'organic_sales', 'be_stock', 'last_purchasable_date', 'cancelled_pr_po',
                           'red_black_grey_lst_wk']
            pr_no_po_columns = ['cluster', 'category', 'shopid', 'shop_name', 'supplier_id', 'supplier_name',
                                'shopee_merchANDiser', 'brand', 'sku_id', 'ItemName', 'ModelName', 'purchase_type',
                                'stock_on_hand', 'organic_sales', 'be_stock', 'pr_creation_date', 'last_pr_id']
            po_no_wh_stock_columns = ['cluster', 'category', 'shopid', 'shop_name', 'supplier_id', 'supplier_name',
                                      'shopee_merchANDiser', 'brand', 'sku_id', 'ItemName', 'ModelName',
                                      'purchase_type',
                                      'stock_on_hand', 'organic_sales', 'be_stock', 'po_creation_date', 'last_order_id']

            stock_sync_ytd = ytd_write[ytd_write['stock_sync_issue'] == 1][stock_sync_columns]

            oos_14_days_ytd = ytd_write[ytd_write['OOS_14_days_no_PR'] == 1][oos_columns]

            PR_but_no_PO_ytd = ytd_write[ytd_write['PR_but_no_PO'] == 1][pr_no_po_columns]

            PO_but_no_wh_stock_ytd = ytd_write[ytd_write['PO_but_no_wh_stock'] == 1][po_no_wh_stock_columns]

            OOS_more_than_14_days_no_PR_ytd = ytd_write[ytd_write['OOS_more_than_14_days_no_PR'] == 1][oos_columns]

            # dictionary where keys are the sheetnames and the values are the dataframes to be written in each sheet
            wk_2_write.drop(['last_pr_id', 'last_order_id'], axis=1, inplace=True)
            ytd_write.drop(['last_pr_id', 'last_order_id'], axis=1, inplace=True)
            write_dict = {
                "SKU details - ATP - W-2": {'df': wk_2_write, 'date': two_weeks_ago_date},
                "SKU details - ATP - Yesterday": {'df': ytd_write, 'date': yesterday_date},
                "SKUs - Stock Sync issue": {'df': stock_sync_ytd, 'date': yesterday_date},
                "SKUs - OOS 14 days or less": {'df': oos_14_days_ytd, 'date': yesterday_date},
                "SKUs - PR but no PO": {'df': PR_but_no_PO_ytd, 'date': yesterday_date},
                "SKUs - OOS more than 14 days": {'df': OOS_more_than_14_days_no_PR_ytd, 'date': yesterday_date},
                "SKUs - PO but no inbound": {'df': PO_but_no_wh_stock_ytd, 'date': yesterday_date}
            }

            append_df_to_excel(r_file_name, r_write_dict, r_output_file, start_col=1, start_row=4, header_bool=False)
            print("Written for Replenishment Progress Tracker!")

            print("Writing for ATP Report...")
            append_df_to_excel(filename, write_dict, output_file, start_row=3, header_bool=False)
            print("Written for ATP Report!")

            print('It took', (time.time() - start) / 60, 'mins to complete the script.')

    if not summary_completed:
        country_l = ['MY', 'TH', 'TW', 'ID', 'VN', 'PH']
        ATP_reports_path = load_report_path('ATP_reports_path')
        Replenishment_reports_path = load_report_path('Replenishment_reports_path')

        print('Reading for templates!')
        Rep_summary_template = "./Summary templates/Regional_Replenishment_template_small.xlsx"
        ATP_summary_template = "./Summary templates/Regional_ATP_template_small.xlsx"
        for c in range(len(country_l)):
            if not os.path.exists(ATP_reports_path[country_l[c]]):
                print(ATP_reports_path[country_l[c]] + ' not found')
                continue
            else:
                print('Reading ' + ATP_reports_path[country_l[c]])
                Regional_ATP_rewrite(ATP_reports_path[country_l[c]], ATP_summary_template, c)
                print(ATP_reports_path[country_l[c]] + ' is done')
        print('Written for ATP Summary!')

        for c in range(len(country_l)):
            if not os.path.exists(Replenishment_reports_path[country_l[c]]):
                print(Replenishment_reports_path[country_l[c]] + ' not found')
                continue
            else:
                print('Reading ' + Replenishment_reports_path[country_l[c]])
                Regional_Replenishment_rewrite(Replenishment_reports_path[country_l[c]], Rep_summary_template, c)
                print(Replenishment_reports_path[country_l[c]] + ' is done')
        print('Written for Replenishment Summary!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--processed', help='has files been processed for atp', default=False)
    parser.add_argument('-q', '--queried', help='has data been queried?', default=False)
    parser.add_argument('-r', '--rep_done', help='has rep finished?', default=False)
    parser.add_argument('-c', '--countries', nargs='+', default=['ID', 'MY', 'TH', 'VN', 'TW', 'PH'])
    parser.add_argument("-d", "--date",
                        default=dt.date.today(),  # dt.date.today()
                        type=lambda d: dt.datetime.strptime(d, '%Y%m%d').date(),
                        help="Date in the format yyyymmdd")
    parser.add_argument('-w', '--reports_written', help='has reports been written for ATP and Rep?', default=False)
    parser.add_argument('-s', '--summary_done', help='has summary been completed?', default=False)
    args = parser.parse_args()
    main(args.date, args.countries, args.queried, args.processed, args.rep_done, args.reports_written,
         args.summary_done)