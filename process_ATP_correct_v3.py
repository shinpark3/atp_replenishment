import pandas as pd
import numpy as np
import warnings
import datetime as dt

warnings.simplefilter("ignore")


def process_date(date):
    '''
    :param date: date in nanoseconds
    :type int: time-delta format
    ---
    :return days: timedelta converted to days
    :rtype int: Representing number of days
    '''
    if date is None or date is pd.NaT:
        return

    if type(date) == int:
        # convert nanoseconds to Days
        return np.timedelta64(np.timedelta64(date, 'ns'), 'D')

    return date


v_process_date = np.vectorize(process_date)


def get_reorder_point_df(start_lookback_date, lookback_days, replenishment, ext, pr_po_inbound):
    '''
    This function looks back 14 days and gets the earliest reorder point value at the latest block. It then returns
    a dataframe of sku_ids with the appropiate reorder_point value to be populated in the final dataframe.
    :param start_lookback_date: The start of the lookback_date
    :param lookback_days: How many days to lookback to find the reorder_point block
    :param replenishment: replenishment dataframe
    :param l30df: l30day dataframe
    :param ext: replenishment_ext dataframe
    :param pr_po_inbound: pr_po dataframe
    :return reorder_block_df: Dataframe with the correct reorder_reccomendation, reorder point and reorder date
    '''

    print("Getting Reorder Point Block with a lookback of {} days...".format(lookback_days))

    for minus_days in range(lookback_days):
        print('starting look back day {} out of {}'.format(minus_days + 1, lookback_days))
        current_date = start_lookback_date - dt.timedelta(days=minus_days)
        current_r = replenishment[replenishment.grass_date == current_date]

        purchasable = replenishment[
            (replenishment['grass_date'] <= current_date) & (
                    replenishment['grass_date'] >= current_date - dt.timedelta(days=12 * 7))]
        purchasable = purchasable[purchasable['real_sales'] != 'x']
        avg_purchasable = purchasable.groupby('sku_id')['sales'].mean().reset_index()
        std_purchasable = purchasable.groupby('sku_id')['sales'].std().reset_index()
        volatility = pd.merge(avg_purchasable, std_purchasable, on='sku_id', how='left')
        volatility.columns = ['sku_id', 'avg', 'std']
        volatility['vol_days_sales'] = volatility['std'] / volatility['avg']
        volatility.fillna(0, inplace=True)
        volatility['vol_days_sales'] = np.where(volatility['vol_days_sales'] > 7, 7, volatility['vol_days_sales'])
        volatility = volatility[['sku_id', 'vol_days_sales']]

        # processing L30D
        l30d_end_date = current_date
        l30d_start_date = current_date - dt.timedelta(29)
        l30d_r = replenishment[
            (replenishment['grass_date'] >= l30d_start_date) & (replenishment['grass_date'] <= l30d_end_date)]

        temp2 = l30d_r[l30d_r['real_sales'] != 'x']
        temp2_std = temp2.groupby('sku_id')['sales'].std().reset_index()
        temp2_std.columns = ['sku_id', 'std_dev']

        temp3 = temp2.groupby('sku_id')['sales'].mean().reset_index()
        temp3.columns = ['sku_id', 'mean']
        campaign_df = pd.merge(temp2_std, temp3, on='sku_id', how='right')
        campaign_df['mean+2std'] = campaign_df['mean'] + 2 * campaign_df['std_dev']
        temp4 = pd.merge(temp2, campaign_df, on='sku_id', how='left')
        temp4['keep'] = 1
        temp4['keep'] = np.where(temp4['mean+2std'] < temp4['sales'], 0, 1)
        temp5 = temp4[temp4['keep'] == 1]
        l30_df = temp5.groupby('sku_id')['sales'].mean().reset_index()
        l30_df.columns = ['sku_id', 'L30D_ADIS']
        l30_df['L30D_ADIS'].fillna('n.a.', inplace=True)
        pr_in_progress = pr_po_inbound[
            (((pr_po_inbound['pr_status'].isin([1, 3, 4]))
              & (pr_po_inbound['pr_date'] <= current_date)
              & (pr_po_inbound['pr_reason'] != 'Re-Inbound')) |
             ((pr_po_inbound['po_status'].isin([1, 2, 3, 5]))
              & (pr_po_inbound['po_date'] <= current_date)
              & (pr_po_inbound['pr_reason'] != 'Re-Inbound')))
            & ((pr_po_inbound['inbound_time'] > current_date) | (pr_po_inbound['inbound_time'].isnull()))]

        pr_in_progress0 = pr_in_progress.sort_values(['pr_date', 'po_date', 'inbound_time'])
        # pr_in_progress0[pr_in_progress0['sku_id'] == '161686959_0']
        pr_in_progress1 = pr_in_progress0.groupby(['sku_id', 'pr_date', 'po_date']).head(1)
        pr_in_progress2 = pr_in_progress1.groupby('sku_id').tail(1)
        # pr_in_progress2[pr_in_progress2['sku_id'] == '161686959_0']
        pr_in_progress2['pr_in_progress'] = 'y'
        pr_in_progress3 = pr_in_progress2[['sku_id', 'pr_in_progress']]

        prod_info = current_r.merge(ext[['sku_id', 'grass_date', 'avg_lead_time', 'stddev_lead_time']],
                                    on=['sku_id', 'grass_date'], how='left').reset_index()
        prod_info = prod_info.merge(l30_df, on='sku_id', how='left')
        prod_info = prod_info.merge(pr_in_progress3, on='sku_id', how='left')
        prod_info = prod_info.merge(volatility, on='sku_id', how='left')

        prod_info['coverage'] = prod_info['be_stock'] / prod_info['L30D_ADIS']
        prod_info['coverage'] = np.where(prod_info['coverage'] == np.inf, 91,
                                         prod_info['coverage'])
        prod_info['coverage'] = np.where(prod_info['be_stock'] == 0, 0, prod_info['coverage'])
        prod_info["safety_stock"] = np.where(prod_info['stddev_lead_time'].isna() & prod_info['vol_days_sales'].isna(),
                                             0,
                                             np.where(prod_info['stddev_lead_time'].isna(),
                                                      (prod_info['vol_days_sales'].astype(float) ** 2) ** 0.5,
                                                      (prod_info['stddev_lead_time'].astype(float) ** 2 + prod_info[
                                                          'vol_days_sales'].astype(float) ** 2) ** 0.5))

        prod_info["internal_lead_time"] = 3
        prod_info["reorder_point"] = np.where(prod_info['avg_lead_time'].isna(),
                                              prod_info["internal_lead_time"] + prod_info["safety_stock"],
                                              prod_info["internal_lead_time"] + prod_info["safety_stock"] + prod_info["avg_lead_time"])

        prod_info['reorder_recommendation'] = np.where(prod_info["pr_in_progress"] == 'y',
                                                       "no",
                                                       np.where(prod_info["coverage"] > prod_info["reorder_point"],
                                                                'no',
                                                                'yes'))

        prod_info['reorder_date'] = current_date
        lookback_day_df = prod_info[['sku_id', "reorder_recommendation", "reorder_point", "reorder_date"]]

        if minus_days == 0:  # if first iteration, create dataframe
            reorder_block_df = lookback_day_df
        else:
            reorder_block_df = reorder_block_df.append(lookback_day_df)
    print("Reorder blocks done processing")
    print("Getting earliest reorder point value...")
#     reorder_block_df = reorder_block_df.groupby('sku_id').apply(get_reorder_slice).reset_index(drop=True)
#     print("Reorder point data done!")
    return reorder_block_df


def get_reorder_slice(slice_df):
    '''
    This function takes in a dataframe and extracts out the earliest reorder_point for the latest reorder == 'yes' block
    :param slice_df:
    :return
    '''
    slice_df = slice_df.sort_values(by='reorder_date', ascending=False)
    # get index of value counts in a list
    # if yes does not exist in index, means that all are no, in that case, just return the row with the latest reorder_date)
    yes_and_no_value_counts = slice_df['reorder_recommendation'].value_counts()
    yes_and_no = yes_and_no_value_counts.index.tolist()
    if "yes" not in yes_and_no:
        temp = slice_df.tail(1)
        temp['keep'] = 0
        return temp
    dates = list(slice_df['reorder_date'])
    # print(dates)
    for index, date in enumerate(dates):
        if index == (len(dates) - 1):
            return slice_df[slice_df['reorder_date'] == date]
        elif index != 0:
            before_date = dates[index - 1]
            current_date = date
            before_rec = slice_df[slice_df['reorder_date'] == before_date].iloc[0]['reorder_recommendation']
            current_rec = slice_df[slice_df['reorder_date'] == current_date].iloc[0]['reorder_recommendation']
            if current_rec == "no" and before_rec == "yes":
                return slice_df[slice_df['reorder_date'] == before_date]


def process_raw_data_frames(purchase_type, ext, brand, replenishment, pr_po_inbound, new_color):
    '''
    Processes Dataframes and returns the Processed dataframes
    :param purchase_type: dataframe
    :param ext: dataframe
    :param brand: dataframe
    :param replenishment: dataframe
    :param pr_po_inbound: dataframe
    :param new_color: dataframe
    :return purchase_type, ext, replenishment, pr_po_inbound, new_color: Processed Dataframes
    :type purchase_type: tuple
    '''
    purchase_type = purchase_type.drop_duplicates(subset='sku_id', keep='first')
    ext['grass_date'] = pd.to_datetime(ext['grass_date'], yearfirst=True)
    # replenishment.drop(columns=['c_sku_id', 'c_grass_date', 'c_grass_region'], inplace=True)
    replenishment.columns = [x.lower() for x in replenishment.columns]
    pr_po_inbound['pr_date'] = pd.to_datetime(pr_po_inbound['pr_date'])  # ,unit='s')
    pr_po_inbound['po_date'] = pd.to_datetime(pr_po_inbound['po_date'])  # ,unit='s')
    pr_po_inbound['inbound_time'] = pd.to_datetime(pr_po_inbound['inbound_time'])  # ,unit='s')
    replenishment['grass_date'] = pd.to_datetime(replenishment['grass_date'], yearfirst=True)
    replenishment = replenishment[replenishment['grass_date'] <= ext['grass_date'].max()]
    replenishment.columns = [x.lower() for x in replenishment.columns]
    new_color['grass_date'] = pd.to_datetime(new_color['grass_date'])
    ext['grass_date'] = pd.to_datetime(ext['grass_date'])
    replenishment = pd.merge(replenishment, brand[['sku_id', 'brand']], on='sku_id', how='left')
    # Edit PR-PO-Inbound file
    pr_po_inbound['inbound_quantity'].fillna('n.a.', inplace=True)
    pr_po_inbound['pivot_value'] = pr_po_inbound['pr_quantity'].astype(int).astype(str) + '_' + pr_po_inbound[
        'pr_reason'] + '_' + pr_po_inbound['pr_id']
    pr_po_inbound['color_value'] = pr_po_inbound['pivot_value'].astype(str) + '_' + pr_po_inbound[
        'order_id'].astype(str) + '_' + pr_po_inbound[
                                       'inbound_quantity'].astype(str) + '_' + pr_po_inbound[
                                       'po_quantity'].astype(str) + '_' + pr_po_inbound[
                                       'pr_date'].astype(str)
    pr_po_inbound['pr_date'] = pd.to_datetime(pr_po_inbound['pr_date'])
    # pr_po_inbound['po_date'] = pd.to_datetime(pr_po_inbound['po_date'])
    # pr_po_inbound = pr_po_inbound[pr_po_inbound['pr_date'] >= new_color['grass_date'].min()]
    pr_po_inbound = pr_po_inbound[pr_po_inbound['pr_reason'] != 'Re-Inbound']
    return purchase_type, ext, replenishment, pr_po_inbound, new_color


def process_atp_df(country, wk_count, df_date, pr_po_inbound, replenishment, cat_cluster, cogs, brand,
                   item_profile_history, new_color, ext, cfs_stock, purchase_type, prev_iteration_atp_df=None):
    '''
    Takes in DataFrames and Processes them by country and lookback period.
    Then, returns two processed dataframes:
        1) Processed Dataframe of Data during lookback period
        2) Processed Dataframe of COGS during lookback period
    And also returns the filename of the data for the lookback period
    :param country: country currency processing
    :type country: str
    :param wk_count: Week lookback period
    :param wk_count: int
    :param df_date: Lookback period
    :type df_date: Datetime64
    :param pending_minus_days: days to minus from max replenishment date currently in dataframe
    :type pending_minus_days: int
    :param pr_po_inbound: dataframe
    :param replenishment: dataframe
    :param cat_cluster: dataframe
    :param cogs: dataframe
    :param brand: dataframe
    :param item_profile_history: dataframe
    :param new_color: dataframe
    :param ext: dataframe
    :param cfs_stock: dataframe
    :param purchase_type: dataframe
    :param prev_iteration_atp_df: Previous iteration's dataframe. For example, during wk-2, the previous iteration is wk-4. There is no previous iteration dataframe for wk-4
    :type prev_iteration_atp_df: pandas Dataframe
    :return ytd_product_info, filename, last_cogs: tuple of dataframe, filename and cogs
    :rtype  ytd_product_info, filename, last_cogs: tuple(pandas.DataFrame, str, pandas.DataFrame)
    '''

    new_folder = './' + country + '/'
    #     if not os.path.exists(new_folder):
    #         os.makedirs(new_folder)

    print('Writing ATP Dataframe for Week {}, Current Lookback Date: {}...'.format(wk_count,
                                                                                   df_date.strftime("%Y-%m-%d")))

    # if wk_count argument matches certain criteria, adjust the number of days to minus
    # All days to minus are subtracted by one as replenishment core max_date is one day before today
    # for e.g, if today is 28th of June, Replenishment Core's Max Date is 27th June as it only gets updated the next day

    # Files clean up
    pr_po_inbound['pr_date'] = pd.to_datetime(pr_po_inbound['pr_date'])
    pr_po_inbound['po_date'] = pd.to_datetime(pr_po_inbound['po_date'])
    pr_po_inbound['inbound_time'] = pd.to_datetime(pr_po_inbound['inbound_time'])
    replenishment['grass_date'] = pd.to_datetime(replenishment['grass_date'])
    temp_pr_df = pr_po_inbound[(pr_po_inbound['pr_status'].isin([1, 3, 4]))
                               & ((pr_po_inbound['po_status'].isin([1, 2, 3, 5])) | (pr_po_inbound['po_date'].isnull()))
                               & (pr_po_inbound['pr_date'] <= df_date)
                               & (pr_po_inbound['pr_date'] >= df_date - dt.timedelta(12 * 7))]
    temp_pr_df = temp_pr_df.sort_values('pr_date')
    last_pr = temp_pr_df.groupby('sku_id').tail(1).reset_index()
    last_pr = last_pr[['sku_id', 'pr_quantity', 'pr_date', 'pr_id']]
    last_pr.columns = ['sku_id', 'incoming_stock', 'pr_creation_date', 'last_pr_id']

    temp_po_df = pr_po_inbound[(pr_po_inbound['po_status'].isin([1, 2, 3, 5]))
                               & (pr_po_inbound['po_date'] <= df_date)
                               & (pr_po_inbound['po_date'] >= df_date - dt.timedelta(12 * 7))]
    temp_po_df = temp_po_df.sort_values('po_date')
    last_po = temp_po_df.groupby('sku_id').tail(1).reset_index()
    last_po = last_po[['sku_id', 'po_date', 'order_id']]
    last_po.columns = ['sku_id', 'po_creation_date', 'last_order_id']
    last_po['po_creation_date'] = pd.to_datetime(last_po['po_creation_date'])

    ext['grass_date'] = pd.to_datetime(ext['grass_date'], yearfirst=True)
    replenishment = replenishment.sort_values('grass_date')
    replenishment.columns = [x.lower() for x in replenishment.columns]

    last_replenishment_data = replenishment[replenishment['grass_date'] == df_date]

    rejected_pr = pr_po_inbound[(pr_po_inbound['pr_status'].isin([2, 5, 6])) & (pr_po_inbound['pr_date'] < df_date)]
    rejected_pr = rejected_pr.sort_values('pr_date').reset_index()
    rejected_pr = rejected_pr.groupby('sku_id').tail(1).reset_index()
    rejected_pr = rejected_pr[['sku_id', 'pr_date']]
    rejected_pr.columns = ['sku_id', 'last_pr_rejected_deleted_withdrawn']

    last_inbound = pr_po_inbound[(pr_po_inbound['pr_status'] == 4)
                                 & (pr_po_inbound['po_status'].isin([2, 3, 5])) & (pr_po_inbound['pr_date'] < df_date)]
    last_inbound = last_inbound.dropna()
    last_inbound = last_inbound[last_inbound['inbound_quantity'] != 0]
    last_inbound = last_inbound.sort_values('inbound_time')
    last_inbound = last_inbound.groupby('sku_id').tail(1).reset_index()
    last_inbound = last_inbound[['sku_id', 'inbound_time']]
    last_inbound.columns = ['sku_id', 'last_inbound_date']

    pr_generated = pr_po_inbound[(pr_po_inbound['pr_status'].isin([1, 3, 4]))
                                 & (pr_po_inbound['pr_date'] >= (df_date - dt.timedelta(days=14)))
                                 & (pr_po_inbound['pr_date'] <= df_date)]
    pr_generated['pr_generated'] = 'y'
    pr_generated = pr_generated.groupby('sku_id').tail(1).reset_index()
    pr_generated = pr_generated[['sku_id', 'pr_generated']]

    po_generated = pr_po_inbound[(pr_po_inbound['po_status'].isin([1, 2, 3, 5]))
                                 & (pr_po_inbound['po_date'] <= df_date)
                                 & (pr_po_inbound['po_date'] >= (df_date - dt.timedelta(days=14)))]
    po_generated['po_generated'] = 'y'
    po_generated = po_generated.groupby('sku_id').tail(1).reset_index()
    po_generated = po_generated[['sku_id', 'po_generated']]

    pr_in_progress = pr_po_inbound[(pr_po_inbound['pr_status'].isin([1, 3, 4]))
                                   & (pr_po_inbound['pr_date'] >= (df_date - dt.timedelta(days=14)))
                                   & (pr_po_inbound['pr_date'] <= df_date)
                                   & ((pr_po_inbound['po_date'] > df_date) | (pr_po_inbound['po_date'].isnull()))]

    pr_in_progress['pr_in_progress'] = 'y'
    pr_in_progress = pr_in_progress.groupby('sku_id').tail(1).reset_index()
    pr_in_progress = pr_in_progress[['sku_id', 'pr_in_progress']]

    po_in_progress = pr_po_inbound[(pr_po_inbound['po_status'].isin([1, 2, 3, 5]))
                                   & (pr_po_inbound['po_date'] <= df_date)
                                   & (pr_po_inbound['po_date'] >= (df_date - dt.timedelta(days=14)))
                                   & ((pr_po_inbound['inbound_time'] > df_date) | (
        pr_po_inbound['inbound_time'].isnull()))]
    po_in_progress['po_in_progress'] = 'y'
    po_in_progress = po_in_progress.groupby('sku_id').tail(1).reset_index()
    po_in_progress = po_in_progress[['sku_id', 'po_in_progress']]

    purchasable_df = replenishment[(replenishment['grass_date'] <= df_date) & (replenishment['real_sales'] != 'x')]
    purchasable_df = purchasable_df.sort_values('grass_date')
    purchasable_df = purchasable_df.groupby('sku_id').tail(1)
    purchasable_df = purchasable_df[['sku_id', 'grass_date']]
    purchasable_df.columns = ['sku_id', 'last_purchasable_date']
    purchasable_df['last_purchasable_date'].fillna(purchasable_df['last_purchasable_date'].min(), inplace=True)
    new_color['grass_date'] = pd.to_datetime(new_color['grass_date'])

    latest_color = new_color[new_color['grass_date'] == df_date]
    latest_color = latest_color[['sku_id', 'color', 'stock_on_hand']]
    latest_color = latest_color.drop_duplicates()

    # cogs.columns = ['sku_id', 'grass_date', 'cogs']
    cogs['grass_date'] = pd.to_datetime(cogs['grass_date'])
    last_cogs = cogs[cogs['grass_date'] == df_date]
    last_cogs = last_cogs[['sku_id', 'cogs']]
    last_cogs = last_cogs.drop_duplicates()

    item_profile = pd.merge(cat_cluster[['catid', 'cluster']], item_profile_history, left_on='catid',
                            right_on='main_cat', how='right')
    item_profile['sku_id'] = item_profile['itemid'].astype(str) + '_' + item_profile['modelid'].astype(str)

    temp_end_date = df_date
    temp_start_date = df_date - dt.timedelta(days=29)
    temp1 = replenishment[
        (replenishment['grass_date'] >= temp_start_date) & (replenishment['grass_date'] <= temp_end_date)]

    temp2 = temp1[temp1['real_sales'] != 'x']
    temp2_std = temp2.groupby('sku_id')['sales'].std().reset_index()
    temp2_std.columns = ['sku_id', 'std_dev']

    temp3 = temp2.groupby('sku_id')['sales'].mean().reset_index()
    temp3.columns = ['sku_id', 'mean']
    campaign_df = pd.merge(temp2_std, temp3, on='sku_id', how='right')
    campaign_df['mean+2std'] = campaign_df['mean'] + 2 * campaign_df['std_dev']
    temp4 = pd.merge(temp2, campaign_df, on='sku_id', how='left')
    temp4['keep'] = 1
    temp4['keep'] = np.where(temp4['mean+2std'] < temp4['sales'], 0, 1)
    temp5 = temp4[temp4['keep'] == 1]
    l30_df = temp5.groupby('sku_id')['sales'].mean().reset_index()
    l30_df.columns = ['sku_id', 'L30D_ADIS']
    l30_df['L30D_ADIS'].fillna('n.a.', inplace=True)

    last_replenishment = replenishment[replenishment['grass_date'] == df_date]
    last_replenishment['purchasable_today'] = 'y'
    last_replenishment['purchasable_today'] = np.where(last_replenishment['real_sales'] == 'x', 'n',
                                                       last_replenishment['purchasable_today'])
    last_replenishment = last_replenishment[['sku_id', 'purchasable_today']]

    purchasable = replenishment[
        (replenishment['grass_date'] <= df_date) & (replenishment['grass_date'] >= df_date - dt.timedelta(days=12 * 7))]
    purchasable = purchasable[purchasable['real_sales'] != 'x']
    avg_purchasable = purchasable.groupby('sku_id')['sales'].mean().reset_index()
    std_purchasable = purchasable.groupby('sku_id')['sales'].std().reset_index()
    volatility = pd.merge(avg_purchasable, std_purchasable, on='sku_id', how='left')
    volatility.columns = ['sku_id', 'avg', 'std']
    volatility['vol_days_sales'] = volatility['std'] / volatility['avg']
    volatility.fillna(0, inplace=True)
    volatility['vol_days_sales'] = np.where(volatility['vol_days_sales'] > 7, 7, volatility['vol_days_sales'])
    volatility = volatility[['sku_id', 'vol_days_sales']]

    ytd_product_info = pd.merge(last_replenishment_data, ext[
        ['supplier_id', 'supplier_name', 'ItemName', 'ModelName', 'sku_id', 'grass_date', 'sourcing_status',
         'stddev_lead_time', 'avg_lead_time', 'shopee_merchANDiser']], on=['sku_id', 'grass_date'],
                                how='left').reset_index()

    ytd_product_info = ytd_product_info[
        ['category', 'shopid', 'supplier_id', 'supplier_name', 'sku_id', 'ItemName', 'ModelName',
         'sourcing_status',
         'be_stock', 'grass_date', 'stddev_lead_time', 'avg_lead_time', 'shopee_merchANDiser']]
    ytd_product_info = pd.merge(ytd_product_info, latest_color, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, item_profile[['sku_id', 'shop_name', 'item_price', 'cluster']],
                                on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, last_cogs, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, last_pr, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, purchasable_df, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, rejected_pr, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, last_inbound, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, pr_in_progress, on='sku_id', how='left')
    ytd_product_info['pr_in_progress'].fillna('n', inplace=True)
    ytd_product_info = pd.merge(ytd_product_info, po_in_progress, on='sku_id', how='left')
    ytd_product_info['po_in_progress'].fillna('n', inplace=True)
    ytd_product_info['pr_in_progress'] = np.where((ytd_product_info['po_in_progress'] == 'y')
                                                  & (ytd_product_info['pr_in_progress'] == 'n'), 'y',
                                                  ytd_product_info['pr_in_progress'])

    ytd_product_info = pd.merge(ytd_product_info, pr_generated, on='sku_id', how='left')
    ytd_product_info['pr_generated'].fillna('n', inplace=True)
    ytd_product_info = pd.merge(ytd_product_info, po_generated, on='sku_id', how='left')
    ytd_product_info['po_generated'].fillna('n', inplace=True)
    ytd_product_info['pr_generated'] = np.where((ytd_product_info['po_generated'] == 'y')
                                                & (ytd_product_info['pr_generated'] == 'n'), 'y',
                                                ytd_product_info['pr_generated'])
    ytd_product_info = pd.merge(ytd_product_info, volatility, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, l30_df, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, last_po, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, cfs_stock, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, purchase_type, on='sku_id', how='left')
    ytd_product_info = pd.merge(ytd_product_info, brand[['sku_id', 'brand']], on='sku_id', how='left')

    ytd_product_info['coverage'] = ytd_product_info['be_stock'] / ytd_product_info['L30D_ADIS']
    ytd_product_info['coverage'] = np.where(ytd_product_info['coverage'] == np.inf, 91,
                                            ytd_product_info['coverage'])
    ytd_product_info['coverage'] = np.where(ytd_product_info['be_stock'] == 0, 0, ytd_product_info['coverage'])
    ytd_product_info["safety_stock"] = np.where(ytd_product_info['stddev_lead_time'].isna()
                                                & ytd_product_info['vol_days_sales'].isna(),
                                                0,
                                                np.where(ytd_product_info['stddev_lead_time'].isna(),
                                                         (ytd_product_info['vol_days_sales'].astype(float) ** 2) ** 0.5,
                                                         (ytd_product_info['stddev_lead_time'].astype(float) ** 2 +
                                                          ytd_product_info[
                                                              'vol_days_sales'].astype(float) ** 2) ** 0.5))

    ytd_product_info["internal_lead_time"] = 3
    ytd_product_info["reorder_point"] = np.where(ytd_product_info['avg_lead_time'].isna(),
                                                 ytd_product_info["internal_lead_time"] + ytd_product_info[
                                                     "safety_stock"],
                                                 ytd_product_info["internal_lead_time"] + ytd_product_info[
                                                     "safety_stock"] + ytd_product_info["avg_lead_time"])

    ytd_product_info['reorder_recommendation'] = np.where(ytd_product_info["pr_in_progress"] == 'y',
                                                          "no",
                                                          np.where(ytd_product_info["coverage"] > ytd_product_info[
                                                              "reorder_point"],
                                                                   'no',
                                                                   'yes'))

    starting = replenishment['grass_date'].max()
    main_df = last_replenishment_data
    for i in range(0, 12):
        temp_end_date = starting - dt.timedelta(days=i * 7)
        temp_start_date = temp_end_date - dt.timedelta(days=29)
        temp1 = replenishment[
            (replenishment['grass_date'] >= temp_start_date) & (replenishment['grass_date'] <= temp_end_date)]
        print(temp_start_date, temp_end_date, len(temp1))
        temp2 = temp1[temp1['real_sales'] != 'x']
        temp2_std = temp2.groupby('sku_id')['sales'].std().reset_index()
        temp2_std.columns = ['sku_id', 'std_dev']
        temp3 = temp2.groupby('sku_id')['sales'].mean().reset_index()
        temp3.columns = ['sku_id', 'mean']
        campaign_df = pd.merge(temp2_std, temp3, on='sku_id', how='right')
        campaign_df['mean+2std'] = campaign_df['mean'] + 2 * campaign_df['std_dev']
        temp4 = pd.merge(temp2, campaign_df, on='sku_id', how='left')
        temp4['keep'] = 1
        temp4['keep'] = np.where(temp4['mean+2std'] < temp4['sales'], 0, 1)
        temp5 = temp4[temp4['keep'] == 1]
        l30_df = temp5.groupby('sku_id')['sales'].mean().reset_index()
        l30_df.columns = ['sku_id', 'W-{} L30D_ADIS'.format(i + 1)]
        #     l30_df['W-{} L30D_ADIS'.format(i + 1)].fillna('n.a.', inplace=True)
        main_df = pd.merge(main_df, l30_df, on='sku_id', how='left')
    main_df.set_index('sku_id', inplace=True)
    main_df = main_df.dropna(how='all', axis=0)
    main_df = main_df.iloc[:, ::-1]
    main_df = main_df[list(main_df.columns[:12])]
    ytd_product_info = pd.merge(ytd_product_info, main_df.reset_index(), on='sku_id', how='left')
    ytd_product_info.drop_duplicates(keep='first', inplace=True)
    ytd_product_info = pd.merge(ytd_product_info, last_replenishment, on='sku_id', how='left')

    item_profile = pd.merge(cat_cluster[['catid', 'cluster']], item_profile_history, left_on='catid',
                            right_on='main_cat', how='right')
    item_profile['sku_id'] = item_profile['itemid'].astype(str) + '_' + item_profile['modelid'].astype(str)

    ytd_product_info['coverage'] = ytd_product_info['be_stock'] / ytd_product_info['L30D_ADIS']
    ytd_product_info['coverage'] = np.where(ytd_product_info['coverage'] == np.inf, 91, ytd_product_info['coverage'])
    ytd_product_info['coverage'] = np.where(ytd_product_info['be_stock'] == 0, 0, ytd_product_info['coverage'])
    # ytd_product_info["pr_creation_date"].fillna(ytd_product_info["pr_creation_date"].min(), inplace=True)
    # print('2', ytd_product_info['pr_creation_date'].value_counts())
    ytd_product_info['pr_creation_date'] = pd.to_datetime(ytd_product_info['pr_creation_date'])
    # ytd_product_info["pr_creation_date"].fillna('n.a.', inplace=True)
    # Reccomendation 14 days ago code
    if wk_count == 4:
        ytd_product_info['Recommendation (14 days ago)'] = 'n.a.'
    else:
        rec = prev_iteration_atp_df[["sku_id", 'reorder_recommendation']]
        rec.columns = ["sku_id", "Recommendation (14 days ago)"]
        ytd_product_info = ytd_product_info.merge(rec, how="left", on="sku_id")
    ytd_product_info['Reserved stock for CFS'] = ytd_product_info["last_fs_total_stock"]
    ytd_product_info['Total Sellable Stock - Excl. CFS'] = np.where(ytd_product_info['Reserved stock for CFS'].isna(),
                                                                    ytd_product_info['stock_on_hand'],
                                                                    ytd_product_info['stock_on_hand'] -
                                                                    ytd_product_info['Reserved stock for CFS'])
    ytd_product_info['Total Sellable Stock - Excl. CFS'] = np.where(
        ytd_product_info['Total Sellable Stock - Excl. CFS'] < 0,
        0, ytd_product_info['Total Sellable Stock - Excl. CFS'])

    ytd_product_info['stock_on_hand'].fillna(0, inplace=True)

    ytd_product_info['Total Sellable Stock - Excl. CFS'] = np.where(ytd_product_info['stock_on_hand'] == 0,
                                                                    0,
                                                                    ytd_product_info[
                                                                        'Total Sellable Stock - Excl. CFS'])

    df_date = dt.datetime(df_date.year, df_date.month, df_date.day)

    # print("PRODUCT INFO: ###########################:", ytd_product_info.info)
    ytd_product_info['# days since last purchasable'] = np.where(ytd_product_info['last_purchasable_date'].isna(),
                                                                 pd.to_timedelta(df_date - ytd_product_info[
                                                                     'last_purchasable_date'].min()).days,
                                                                 pd.to_timedelta(df_date - ytd_product_info[
                                                                     'last_purchasable_date']).dt.days)
    ytd_product_info['last_purchasable_date'] = pd.to_datetime(ytd_product_info['last_purchasable_date'])
    # ytd_product_info.to_csv('ytd_product_info_sample1.csv')
    ytd_product_info['# of days from last PR to OOS'] = np.where(
        (ytd_product_info["pr_creation_date"].isna() | ytd_product_info["last_purchasable_date"].isna()),
        pd.NaT,
        (pd.to_timedelta(ytd_product_info['last_purchasable_date'] - ytd_product_info['pr_creation_date']).dt.days))
    # ytd_product_info['# of days from last PR to OOS'] = ytd_product_info['# of days from last PR to OOS'].dt.day
    x = pd.NaT
    ytd_product_info["# of days from last PR to last PO"] = np.where(ytd_product_info["pr_creation_date"].isna(),
                                                                     x, pd.to_timedelta(
            ytd_product_info["po_creation_date"] - ytd_product_info["pr_creation_date"]).dt.days)
    ytd_product_info["# of days of last PR vs 4 wks ago today"] = np.where(ytd_product_info["pr_creation_date"].isna(),
                                                                           x,
                                                                           pd.to_timedelta(df_date - ytd_product_info[
                                                                               "pr_creation_date"]).dt.days)
    # ['# of days of last PO vs 4 wks ago today'] column is being filled with df_date - timedelta(days=90) because some
    # po_creation_date is so far back it can't be traced. To prevent data discrepancies on the excel side, we
    # filled df_date (df_date - days = 90) if po_creation_date happens to be na
    ytd_product_info["# of days of last PO vs 4 wks ago today"] = np.where(
        ytd_product_info["po_creation_date"].isna(),
        pd.to_timedelta(df_date - (df_date - dt.timedelta(days=90))).days,
        pd.to_timedelta(df_date - ytd_product_info["po_creation_date"]).dt.days)

    # converting datetime64[ns] to integer for report viewing
    cols_to_convert = ["# of days from last PR to last PO",
                       "# of days of last PR vs 4 wks ago today",
                       "# of days of last PO vs 4 wks ago today",
                       '# of days from last PR to OOS',
                       '# days since last purchasable']

    ytd_product_info["safety_stock"] = np.where(
        ytd_product_info['stddev_lead_time'].isna() & ytd_product_info['vol_days_sales'].isna(),
        0, np.where(ytd_product_info['stddev_lead_time'].isna(),
                    (ytd_product_info['vol_days_sales'].astype(float) ** 2) ** 0.5,
                    (ytd_product_info['stddev_lead_time'].astype(float) ** 2 + ytd_product_info[
                        'vol_days_sales'].astype(float) ** 2) ** 0.5
                    ))
    ytd_product_info["internal_lead_time"] = 3

    # if any value is na in these weeks, return na
    moving_l8w_cols = ['W-12 L30D_ADIS', 'W-11 L30D_ADIS', 'W-10 L30D_ADIS',
                       'W-9 L30D_ADIS', 'W-8 L30D_ADIS', 'W-7 L30D_ADIS', 'W-6 L30D_ADIS',
                       'W-5 L30D_ADIS', 'W-4 L30D_ADIS', 'W-3 L30D_ADIS', 'W-2 L30D_ADIS',
                       'W-1 L30D_ADIS']
    max_list_len = len(moving_l8w_cols)
    query_moving_cols = moving_l8w_cols[(4 - wk_count):max_list_len - wk_count]
    temp_df = ytd_product_info[query_moving_cols]
    ytd_product_info["W-4 L8W ADIS"] = temp_df.mean(axis=1)
    print("Rows before dropping: ", ytd_product_info.shape[0])
    ytd_product_info = ytd_product_info[ytd_product_info['sourcing_status'].notna()]
    print("Rows after dropping: ", ytd_product_info.shape[0])
    ytd_product_info = ytd_product_info[
        ['cluster', 'category', 'shopid', 'shop_name', 'supplier_id', 'supplier_name', 'shopee_merchANDiser', 'brand',
         'sku_id',
         'ItemName', 'ModelName', 'purchasable_today', 'item_price', 'cogs', 'Recommendation (14 days ago)',
         'stock_on_hand',
         'Reserved stock for CFS', 'Total Sellable Stock - Excl. CFS',
         'sourcing_status',
         'be_stock', 'incoming_stock', 'last_purchasable_date', '# days since last purchasable', 'pr_creation_date',
         '# of days from last PR to OOS', 'po_creation_date', "# of days from last PR to last PO",
         "# of days of last PR vs 4 wks ago today",
         "# of days of last PO vs 4 wks ago today", 'last_pr_rejected_deleted_withdrawn',
         'last_inbound_date', 'pr_in_progress', 'po_in_progress', 'stddev_lead_time', 'vol_days_sales',
         "safety_stock", 'avg_lead_time', "internal_lead_time", "reorder_point",
         'L30D_ADIS', 'coverage', "reorder_recommendation", 'W-4 L8W ADIS', 'W-12 L30D_ADIS', 'W-11 L30D_ADIS',
         'W-10 L30D_ADIS', 'W-9 L30D_ADIS',
         'W-8 L30D_ADIS', 'W-7 L30D_ADIS', 'W-6 L30D_ADIS', 'W-5 L30D_ADIS', 'W-4 L30D_ADIS',
         'W-3 L30D_ADIS', 'W-2 L30D_ADIS', 'W-1 L30D_ADIS', 'purchase_type', 'pr_generated',
         'po_generated', 'last_pr_id', 'last_order_id']]

    ytd_product_info.drop_duplicates(subset="sku_id", keep='first', inplace=True)

    col_list = [
        "last_purchasable_date",
        "pr_creation_date",
        "po_creation_date",
        "last_pr_rejected_deleted_withdrawn"
    ]

    ytd_product_info[col_list] = ytd_product_info[col_list].apply(pd.to_datetime, format='%Y-%m-%d', errors='ignore')
    # processing organic sales
    organic_raw = replenishment[['sku_id', 'grass_date', 'sales', 'real_sales']]
    df0 = organic_raw[(organic_raw['grass_date'] >= df_date - dt.timedelta(days=30)) & (
            organic_raw['grass_date'] <= df_date)]
    df1 = df0[df0['real_sales'] != 'x']
    std_df = df1.groupby('sku_id').std().reset_index()
    std_df.columns = ['sku_id', 'std']
    first_avg = df1.groupby('sku_id').mean().reset_index()
    first_avg.columns = ['sku_id', 'avg']
    df2 = df0[df0['grass_date'] == df_date]
    df3 = pd.merge(df2, std_df, how='left', on='sku_id')
    df4 = pd.merge(df3, first_avg, how='left', on='sku_id')
    df4['spike_value'] = df4['avg'] + (2 * df4['std'])
    df4['organic_sales'] = np.where(df4['sales'] > df4['spike_value'], 0, df4['sales'])
    organic_sales = df4[['sku_id', 'organic_sales']]

    ytd_product_info = ytd_product_info.merge(organic_sales, on='sku_id', how='left')

    ytd_product_info['stock_sync_issue'] = np.where((ytd_product_info['sourcing_status'] == 0)
                                                    & (ytd_product_info['purchasable_today'] == 'n')
                                                    & (ytd_product_info['stock_on_hand'] > 0), 1, 0)
    ytd_product_info['OOS_14_days_no_PR'] = np.where(
        (ytd_product_info['sourcing_status'] == 0) & (ytd_product_info['purchasable_today'] == 'n') & (
                ytd_product_info['stock_on_hand'] == 0) & (
                ytd_product_info['pr_in_progress'] == 'n') & (
                ytd_product_info['po_in_progress'] == 'n') & (
                ytd_product_info['# days since last purchasable'] >= 0) & (
                ytd_product_info['# days since last purchasable'] <= 14), 1, 0)

    ytd_product_info['PR_but_no_PO'] = np.where(
        (ytd_product_info['sourcing_status'] == 0) & (ytd_product_info['purchasable_today'] == 'n') & (
                ytd_product_info['stock_on_hand'] == 0) & (
                ytd_product_info['pr_in_progress'] == 'y') & (
                ytd_product_info['po_in_progress'] == 'n'), 1, 0)

    ytd_product_info['PO_but_no_wh_stock'] = np.where(
        (ytd_product_info['sourcing_status'] == 0) & (ytd_product_info['purchasable_today'] == 'n') & (
                ytd_product_info['stock_on_hand'] == 0) & (
                ytd_product_info['pr_in_progress'] == 'y') & (
                ytd_product_info['po_in_progress'] == 'y'), 1, 0)
    ytd_product_info['OOS_more_than_14_days_no_PR'] = np.where(
        (ytd_product_info['sourcing_status'] == 0) & (ytd_product_info['purchasable_today'] == 'n') & (
                ytd_product_info['stock_on_hand'] == 0) & (
                ytd_product_info['pr_in_progress'] == 'n') & (
                ytd_product_info['po_in_progress'] == 'n') & (
                ytd_product_info['# days since last purchasable'] > 14), 1, 0)

    cancelled_pr_po = pr_po_inbound[((pr_po_inbound['pr_status'].isin([2, 5, 6])) |
                                     (pr_po_inbound['pr_status'].isin([4])))
                                    & (pr_po_inbound['pr_date'] >= df_date - dt.timedelta(13))
                                    & (pr_po_inbound['pr_date'] <= df_date)]
    cancelled_pr_po0 = cancelled_pr_po.groupby('sku_id').head(1)
    cancelled_pr_po0['cancelled_pr_po'] = 'yes'
    cancelled_pr_po1 = cancelled_pr_po0[['sku_id', 'cancelled_pr_po']]
    ytd_product_info = ytd_product_info.merge(cancelled_pr_po1, how='left', on='sku_id')
    ytd_product_info['cancelled_pr_po'].fillna('no', inplace=True)

    last_week_color = new_color[(new_color['grass_date'] >= df_date - dt.timedelta(6))
                                & (new_color['grass_date'] <= df_date)]
    last_week_color0 = last_week_color[last_week_color['color'].isin(['Grey', 'Black', 'Red'])]
    last_week_color0['red_black_grey_lst_wk'] = 'yes'
    last_week_color0 = last_week_color0.groupby('sku_id').head(1)
    last_week_color1 = last_week_color0[['sku_id', 'red_black_grey_lst_wk']]
    ytd_product_info = ytd_product_info.merge(last_week_color1, how='left', on='sku_id')
    ytd_product_info['red_black_grey_lst_wk'].fillna('no', inplace=True)

    date_col_list = ['last_purchasable_date', 'pr_creation_date', 'po_creation_date',
                     'last_pr_rejected_deleted_withdrawn', 'last_inbound_date']

    no_decimal_cols = ['stddev_lead_time', 'vol_days_sales', 'safety_stock', 'avg_lead_time',
                       'reorder_point', 'coverage']
    two_decimal_cols = ['item_price', 'cogs', 'L30D_ADIS', 'W-4 L8W ADIS', 'W-12 L30D_ADIS',
                        'W-11 L30D_ADIS', 'W-10 L30D_ADIS', 'W-9 L30D_ADIS', 'W-8 L30D_ADIS',
                        'W-7 L30D_ADIS', 'W-6 L30D_ADIS', 'W-5 L30D_ADIS', 'W-4 L30D_ADIS',
                        'W-3 L30D_ADIS', 'W-2 L30D_ADIS', 'W-1 L30D_ADIS']

    for no_dec_col in no_decimal_cols:
        ytd_product_info[no_dec_col] = ytd_product_info[no_dec_col].round()
    for two_dec_col in two_decimal_cols:
        ytd_product_info[two_dec_col] = ytd_product_info[two_dec_col].round(decimals=2)
    for date_col in date_col_list:
        ytd_product_info[date_col] = ytd_product_info[date_col].dt.date

    ytd_product_info.fillna('n.a.', inplace=True)

    if wk_count == 0:
        filename = new_folder + 'ytd_product_info.csv'
        ytd_product_info.to_csv(filename, encoding='utf-8-sig', index=False)
        last_cogs.to_csv(new_folder + "ytd_last_cogs.csv", encoding='utf-8-sig', index=False)
    else:
        filename = new_folder + 'w-{}_product_info.csv'.format(wk_count)
        ytd_product_info.to_csv(filename, encoding='utf-8-sig',
                                index=False)
        last_cogs.to_csv(new_folder + "w-{}_last_cogs.csv".format(wk_count), encoding='utf-8-sig', index=False)

    print('ATP Dataframe for week {} written to csv!'.format(wk_count))
    return ytd_product_info, filename, last_cogs


def process_replenishment_df(country, yesterday_date, four_weeks_ago_date, purchase_type, pr_po_inbound,
                             replenishment, last_cogs, item_profile, ext, wk_4_df):
    pr_po_inbound['inbound_time'] = pd.to_datetime(pr_po_inbound['inbound_time'])
    pr_po_inbound['pr_date'] = pd.to_datetime(pr_po_inbound['pr_date'])

    rop_hunt_df = pr_po_inbound[(pr_po_inbound['pr_date'] < yesterday_date)
                                & (pr_po_inbound['pr_date'] >= four_weeks_ago_date)
                                & (pr_po_inbound['pr_reason'] != 'Re-Inbound')
                                & (pr_po_inbound['pr_status'].isin([1, 3, 4]))]
    rop_hunt_df = rop_hunt_df[(rop_hunt_df['po_date']).isnull() | ((rop_hunt_df['po_date'] < yesterday_date)
                                                                   & (rop_hunt_df['po_date'] >= four_weeks_ago_date)
                                                                   & (rop_hunt_df['po_status'].isin([1, 2, 3, 5])))]

    wk_4_df0 = wk_4_df[['cluster',
                        'category',
                        'shopid',
                        'shop_name',
                        'supplier_id',
                        'supplier_name',
                        'shopee_merchANDiser',
                        'brand',
                        'sku_id',
                        'ItemName',
                        'ModelName',
                        'reorder_recommendation',
                        'be_stock', 'coverage', 'reorder_point'
                        ]]
    wk_4_rec = wk_4_df0[wk_4_df0['reorder_recommendation'] == 'yes']

    aa = wk_4_rec.merge(rop_hunt_df[['sku_id', 'pr_date', 'po_date', 'pr_quantity',
                                     'inbound_time', 'pr_id', 'order_id', 'inbound_id', 'inbound_quantity']],
                        how='left', on='sku_id').drop_duplicates()
    aa['reorder_date'] = four_weeks_ago_date
    replenishment['grass_date'] = pd.to_datetime(replenishment['grass_date'])
    aa['reorder_date'] = pd.to_datetime(aa['reorder_date'])

    wk_4_coverage_df = pd.merge(aa[['sku_id', 'reorder_date']],
                                replenishment[['sku_id', 'grass_date', 'be_stock', 'l30d_forecast']], how='left',
                                left_on=['sku_id', 'reorder_date'], right_on=['sku_id', 'grass_date'])
    wk_4_coverage_df['coverage'] = wk_4_coverage_df['be_stock'] / wk_4_coverage_df['l30d_forecast']
    wk_4_coverage_df['coverage'] = np.where(wk_4_coverage_df['coverage'] == np.inf, 91,
                                            wk_4_coverage_df['coverage'])
    wk_4_coverage_df['coverage'] = np.where(wk_4_coverage_df['be_stock'] == 0, 0, wk_4_coverage_df['coverage'])
    wk_4_coverage_df = wk_4_coverage_df[['sku_id', 'coverage', 'reorder_date']]
    wk_4_coverage_df.rename(columns={'coverage': 'coverage_at_rop'}, inplace=True)

    replenishment_purchasable = replenishment.copy()
    replenishment_purchasable = replenishment_purchasable[['sku_id', 'real_sales', 'grass_date']]
    replenishment_purchasable['grass_date'] = pd.to_datetime(replenishment_purchasable['grass_date'])
    replenishment_purchasable = replenishment_purchasable.sort_values('grass_date')

    replenishment_purchasable = replenishment_purchasable[
        replenishment_purchasable['grass_date'] >= (yesterday_date - dt.timedelta(days=27))]
    replenishment_purchasable['prev_purchasability'] = replenishment_purchasable.groupby('sku_id')['real_sales'].shift()

    replenishment_purchasable['first_oos'] = np.where((replenishment_purchasable['prev_purchasability'] != 'x')
                                                      & (replenishment_purchasable['real_sales'] == 'x')
                                                      & (~replenishment_purchasable['prev_purchasability'].isnull()), 1,
                                                      0)

    first_oos_df0 = replenishment_purchasable[replenishment_purchasable['first_oos'] == 1]
    first_oos_df = first_oos_df0[['sku_id', 'grass_date']]
    first_oos_df.columns = ['sku_id', 'oos_date']

    reorder_n_oos1 = aa.merge(wk_4_coverage_df, on=['sku_id', 'reorder_date'], how='left')
    reorder_n_oos2 = reorder_n_oos1.merge(first_oos_df, on='sku_id', how='left')
    reorder_n_oos3 = reorder_n_oos2[
        (reorder_n_oos2['reorder_date'] <= reorder_n_oos2['oos_date']) | (reorder_n_oos2['oos_date'].isnull())]

    wk_4_pr_coverage_df = pd.merge(reorder_n_oos3[['sku_id', 'pr_date', 'pr_quantity']],
                                   replenishment[['sku_id', 'grass_date', 'be_stock', 'l30d_forecast']], how='left',
                                   left_on=['sku_id', 'pr_date'], right_on=['sku_id', 'grass_date'])
    wk_4_pr_coverage_df['coverage'] = (wk_4_pr_coverage_df['be_stock'] + wk_4_pr_coverage_df['pr_quantity']) / \
                                      wk_4_pr_coverage_df['l30d_forecast']
    wk_4_pr_coverage_df['coverage'] = np.where(wk_4_pr_coverage_df['coverage'] == np.inf, 91,
                                               wk_4_pr_coverage_df['coverage'])
    wk_4_pr_coverage_df['coverage'] = np.where(
        (wk_4_pr_coverage_df['be_stock'] + wk_4_pr_coverage_df['pr_quantity']) == 0, 0, wk_4_pr_coverage_df['coverage'])
    wk_4_pr_coverage_df = wk_4_pr_coverage_df[['sku_id', 'coverage', 'pr_date', 'be_stock', 'l30d_forecast']]
    wk_4_pr_coverage_df.rename(columns={'coverage': 'coverage_at_pr'}, inplace=True)

    reorder_n_oos3 = reorder_n_oos3.merge(wk_4_pr_coverage_df, on=['sku_id', 'pr_date'], how='left')

    reorder_n_oos3['lead_time_rop_to_pr'] = pd.to_timedelta(
        reorder_n_oos3['pr_date'] - reorder_n_oos3['reorder_date']).dt.days
    reorder_n_oos3['lead_time_pr_to_po'] = reorder_n_oos3['po_date'] - reorder_n_oos3['pr_date']
    reorder_n_oos3['lead_time_pr_to_po'] = reorder_n_oos3['lead_time_pr_to_po'].map(lambda x: pd.to_timedelta(x).days)
    reorder_n_oos3['lead_time_po_to_inbound'] = reorder_n_oos3['inbound_time'] - reorder_n_oos3['po_date']
    reorder_n_oos3['lead_time_po_to_inbound'] = reorder_n_oos3['lead_time_po_to_inbound'].map(
        lambda x: pd.to_timedelta(x).days)
    reorder_n_oos3['lead_time_rop_to_inbound'] = reorder_n_oos3['lead_time_rop_to_pr'] + reorder_n_oos3[
        'lead_time_pr_to_po'] + \
                                                 reorder_n_oos3['lead_time_po_to_inbound']

    reorder_n_oos3 = reorder_n_oos3.sort_values('lead_time_rop_to_pr')
    fulfillment_df = pr_po_inbound[pr_po_inbound['inbound_quantity'] != 'n.a.']
    fulfillment_df['inbound_quantity'] = fulfillment_df['inbound_quantity'].astype(int)
    fulfillment_df0 = fulfillment_df.groupby(['sku_id', 'pr_id', 'order_id'])[
        'pr_quantity', 'inbound_quantity'].sum().reset_index()
    fulfillment_df0['fulfillment_pct'] = fulfillment_df0['inbound_quantity'] / fulfillment_df0['pr_quantity']
    reorder_n_oos3 = reorder_n_oos3.merge(fulfillment_df0, on=['sku_id', 'pr_id', 'order_id'], how='left')

    reorder_n_oos4 = reorder_n_oos3.groupby('sku_id')[['sku_id', 'lead_time_rop_to_pr', 'lead_time_pr_to_po',
                                                       'lead_time_po_to_inbound', 'lead_time_rop_to_inbound',
                                                       'coverage_at_rop',
                                                       'coverage_at_pr', 'fulfillment_pct']].head(1)
    wk_4_df1 = wk_4_df0.merge(reorder_n_oos4, on='sku_id', how='left')

    item_profile['sku_id'] = item_profile['itemid'].astype(str) + '_' + item_profile['modelid'].astype(str)
    last_cogs = last_cogs[['sku_id', 'cogs']]
    last_cogs = last_cogs.drop_duplicates()
    last_replenishment = replenishment[replenishment['grass_date'] == yesterday_date]
    last_replenishment['purchasable'] = 'y'
    last_replenishment['purchasable'] = np.where(last_replenishment['real_sales'] == 'x', 'n',
                                                 last_replenishment['purchasable'])
    last_replenishment = last_replenishment[['sku_id', 'purchasable']]

    wk_4_df2 = pd.merge(wk_4_df1, last_cogs, on='sku_id', how='left')

    wk_4_df3 = pd.merge(wk_4_df2, last_replenishment, on='sku_id', how='left')

    first_oos_df0 = first_oos_df.sort_values('oos_date')
    first_oos_df1 = first_oos_df0.groupby('sku_id').head(1).reset_index()

    four_weeks_ago_datetime = dt.datetime(four_weeks_ago_date.year, four_weeks_ago_date.month, four_weeks_ago_date.day)
    wk_4_df3 = wk_4_df3.merge(first_oos_df1[['sku_id', 'oos_date']], how='left', on='sku_id')
    wk_4_df3['rop_to_oos'] = np.where(
        ((wk_4_df3['purchasable'] == 'n') & (wk_4_df3['reorder_recommendation'] == 'yes')),
        pd.to_timedelta(
            wk_4_df3['oos_date'] - four_weeks_ago_datetime).dt.days, pd.NaT)

    wk_4_df3['rop_to_oos'] = np.where(wk_4_df3['rop_to_oos'] < 0,
                                      0, wk_4_df3['rop_to_oos'])

    yst_df = replenishment[replenishment['grass_date'] == yesterday_date]
    yst_df = yst_df[['sku_id', 'l30d_forecast']]
    yst_df = yst_df.rename(columns={'l30d_forecast': 'ADIS_yesterday'})
    yst_df0 = pd.merge(yst_df, item_profile[['sku_id', 'item_price']], on='sku_id',
                       how='left')
    wk_4_df4 = wk_4_df3.merge(yst_df0, how='left', on='sku_id')

    purchase_type.drop_duplicates(subset='sku_id', inplace=True)
    wk_4_df5 = wk_4_df4.merge(purchase_type, how='left', on='sku_id')
    yst_ext = ext[ext['grass_date'] == yesterday_date][['sku_id', 'sourcing_status', 'avg_lead_time']]
    wk_4_df6 = wk_4_df5.merge(yst_ext, how='left', on='sku_id')
    wk_4_df7 = wk_4_df6[wk_4_df6['sourcing_status'] == 0]
    prev_replenishment = replenishment.copy()

    prev_replenishment['grass_date'] = pd.to_datetime(prev_replenishment['grass_date'])

    prev_replenishment = prev_replenishment[prev_replenishment['grass_date'] < four_weeks_ago_date]
    prev_replenishment = prev_replenishment.groupby('sku_id')['sku_id', 'real_sales'].tail(1)
    prev_replenishment['previous_date_oos'] = np.where(prev_replenishment['real_sales'] == 'x', 1, 0)

    prev_replenishment = prev_replenishment[['sku_id', 'previous_date_oos']]

    wk_4_df7 = wk_4_df7.merge(prev_replenishment, how='left', on='sku_id')
    wk_4_df7['rop_to_oos'] = np.where(wk_4_df7['purchasable'] == 'y', pd.NaT, wk_4_df7['rop_to_oos'])
    cancelled_pr_po = pr_po_inbound[((pr_po_inbound['pr_status'].isin([2, 5, 6])) |
                                     (pr_po_inbound['pr_status'].isin([4])))
                                    & (pr_po_inbound['pr_date'] >= yesterday_date - dt.timedelta(13))
                                    & (pr_po_inbound['pr_date'] <= yesterday_date)]
    cancelled_pr_po0 = cancelled_pr_po.groupby('sku_id').head(1)
    cancelled_pr_po0['cancelled_pr_po'] = 'yes'
    cancelled_pr_po1 = cancelled_pr_po0[['sku_id', 'cancelled_pr_po']]
    wk_4_df7 = wk_4_df7.merge(cancelled_pr_po1, how='left', on='sku_id')
    wk_4_df7['cancelled_pr_po'].fillna('no', inplace=True)
    ninety_days_replenishment = replenishment[(replenishment['grass_date'] <= yesterday_date)
                                              & (replenishment['grass_date'] >= yesterday_date - dt.timedelta(days=89))]
    ninety_days_replenishment0 = ninety_days_replenishment.groupby('sku_id')['sales', 'flash_sales'].sum().reset_index()
    ninety_days_replenishment0['total_90D_sales'] = ninety_days_replenishment0['sales'] + ninety_days_replenishment0[
        'flash_sales']
    wk_4_df7 = wk_4_df7.merge(ninety_days_replenishment0[['sku_id', 'total_90D_sales']], how='left', on='sku_id')

    wk_4_df8 = wk_4_df7[['cluster', 'category', 'shopid', 'shop_name', 'supplier_id',
                         'supplier_name', 'shopee_merchANDiser', 'brand', 'sku_id', 'ItemName',
                         'ModelName', 'item_price', 'cogs', 'purchasable', 'rop_to_oos', 'reorder_recommendation',
                         'ADIS_yesterday', 'coverage_at_rop', 'lead_time_rop_to_pr', 'lead_time_pr_to_po',
                         'lead_time_po_to_inbound', 'lead_time_rop_to_inbound', 'coverage_at_pr', 'purchase_type',
                         'previous_date_oos', 'fulfillment_pct', 'avg_lead_time', 'total_90D_sales', 'cancelled_pr_po',
                         'be_stock', 'coverage', 'reorder_point']]
    wk_4_df8['rop_to_oos'] = np.where(wk_4_df8['purchasable'] == 'y', pd.NaT, wk_4_df8['rop_to_oos'])

    pr_hunt_df = pr_po_inbound[(pr_po_inbound['pr_date'] < yesterday_date)
                               & (pr_po_inbound['pr_date'] >= (yesterday_date - dt.timedelta(days=55)))
                               & (pr_po_inbound['pr_reason'] != 'Re-Inbound')
                               & (pr_po_inbound['pr_status'].isin([1, 3, 4]))]
    pr_hunt0 = pr_hunt_df[(pr_hunt_df['po_status'].isin([1, 2, 3, 5])) | (pr_hunt_df['po_status'].isnull())]
    week4_sku_list = wk_4_df8.groupby('sku_id').tail(1)['sku_id'].tolist()
    week4_replenishment_df = replenishment[replenishment['sku_id'].isin(week4_sku_list)]
    pr_hunt_df1 = pr_hunt0[pr_hunt0['sku_id'].isin(week4_sku_list)]
    # ext['grass_date'] = pd.to_datetime(ext['grass_date'])
    week4_replenishment_df.columns = [x.lower() for x in week4_replenishment_df.columns]
    pr_rop_df = get_reorder_point_df(yesterday_date, 56, week4_replenishment_df, ext, pr_po_inbound)
    pr_rop_df['prev_recommendation'] = pr_rop_df.groupby('sku_id')['reorder_recommendation'].shift()
    pr_rop_df['first_reorder_yes'] = np.where(
        (pr_rop_df['prev_recommendation'] == 'no') & (pr_rop_df['reorder_recommendation'] == 'yes'), 1, 0)
    first_reorder_only = pr_rop_df[pr_rop_df['first_reorder_yes'] == 1]

    sorted_pr_hunt_df = pr_hunt_df1.sort_values(['pr_date', 'po_date', 'inbound_time'])

    pr_reorder_pr_df = first_reorder_only.merge(sorted_pr_hunt_df[['sku_id', 'pr_date', 'po_date',
                                                                   'inbound_time', 'pr_id', 'order_id', 'inbound_id']],
                                                how='left', on='sku_id').drop_duplicates()
    pr_reorder_pr_df['reorder_date'] = pd.to_datetime(pr_reorder_pr_df['reorder_date'])
    pr_reorder_pr_df0 = pr_reorder_pr_df[(pr_reorder_pr_df['reorder_date'] <= pr_reorder_pr_df['pr_date'])]
    pr_reorder_pr_df1 = pr_reorder_pr_df0.groupby(['sku_id', 'reorder_date']).head(1)

    pr_reorder_pr_df1['lead_time_rop_to_pr'] = pd.to_timedelta(
        pr_reorder_pr_df1['pr_date'] - pr_reorder_pr_df1['reorder_date']).dt.days

    pr_coverage_at_rop = pd.merge(pr_reorder_pr_df1[['sku_id', 'reorder_date']],
                                  replenishment[['sku_id', 'grass_date', 'be_stock', 'l30d_forecast']], how='left',
                                  left_on=['sku_id', 'reorder_date'], right_on=['sku_id', 'grass_date'])
    pr_coverage_at_rop['coverage'] = pr_coverage_at_rop['be_stock'] / pr_coverage_at_rop['l30d_forecast']
    pr_coverage_at_rop['coverage'] = np.where(pr_coverage_at_rop['coverage'] == np.inf, 91,
                                              pr_coverage_at_rop['coverage'])
    pr_coverage_at_rop['coverage'] = np.where(pr_coverage_at_rop['be_stock'] == 0, 0, pr_coverage_at_rop['coverage'])
    pr_coverage_at_rop = pr_coverage_at_rop[['sku_id', 'coverage', 'reorder_date']]
    pr_coverage_at_rop.rename(columns={'coverage': 'coverage_at_rop'}, inplace=True)

    pr_coverage_at_pr = pd.merge(pr_reorder_pr_df1[['sku_id', 'pr_date']],
                                 replenishment[['sku_id', 'grass_date', 'be_stock', 'l30d_forecast']], how='left',
                                 left_on=['sku_id', 'pr_date'], right_on=['sku_id', 'grass_date'])
    pr_coverage_at_pr['coverage'] = pr_coverage_at_pr['be_stock'] / pr_coverage_at_pr['l30d_forecast']
    pr_coverage_at_pr['coverage'] = np.where(pr_coverage_at_pr['coverage'] == np.inf, 91,
                                             pr_coverage_at_pr['coverage'])
    pr_coverage_at_pr['coverage'] = np.where(pr_coverage_at_pr['be_stock'] == 0, 0, pr_coverage_at_pr['coverage'])
    pr_coverage_at_pr = pr_coverage_at_pr[['sku_id', 'coverage', 'pr_date']]
    pr_coverage_at_pr.rename(columns={'coverage': 'coverage_at_pr'}, inplace=True)

    pr_reorder_pr_df2 = pr_reorder_pr_df1.merge(pr_coverage_at_rop, on=['sku_id', 'reorder_date'], how='left')
    pr_reorder_pr_df3 = pr_reorder_pr_df2.merge(pr_coverage_at_pr, on=['sku_id', 'pr_date'], how='left')

    pr_reorder_pr_df4 = pr_reorder_pr_df3.merge(
        wk_4_df8[['sku_id', 'cluster', 'purchase_type', 'supplier_name', 'supplier_id']], how='left',
        on='sku_id')
    pr_reorder_pr_df5 = pr_reorder_pr_df4.drop(['cluster', 'purchase_type'], axis=1)

    pr_cluster = pr_reorder_pr_df4.groupby(['pr_id'])['cluster'].apply(lambda x: x.value_counts().head(1)).reset_index()
    pr_cluster = pr_cluster[['pr_id', 'level_1']]
    pr_cluster.columns = ['pr_id', 'cluster']

    pr_ptype = pr_reorder_pr_df4.groupby(['pr_id'])['purchase_type'].apply(
        lambda x: x.value_counts().head(1)).reset_index()
    pr_ptype = pr_ptype[['pr_id', 'level_1']]
    pr_ptype.columns = ['pr_id', 'purchase_type']

    pr_reorder_pr_df6 = pr_reorder_pr_df5.groupby(['pr_id', 'supplier_name', 'supplier_id'])[
        'lead_time_rop_to_pr', 'coverage_at_rop', 'coverage_at_pr'].mean().reset_index()

    pr_reorder_pr_df7 = pr_reorder_pr_df6.merge(pr_cluster, on=['pr_id'], how='left')
    pr_reorder_pr_df8 = pr_reorder_pr_df7.merge(pr_ptype, on=['pr_id'], how='left')
    pr_reorder_pr_df8['supplier_id'] = pr_reorder_pr_df8['supplier_id'].astype(int)

    print('Finished ROP to PR tab')

    inbound_df = pr_reorder_pr_df5[(pr_reorder_pr_df5['inbound_time'] <= yesterday_date)
                                   & (pr_reorder_pr_df5['inbound_time'] >= (yesterday_date - dt.timedelta(days=55)))]

    inbound_cluster = pr_reorder_pr_df4.groupby(['pr_id'])['cluster'].apply(
        lambda x: x.value_counts().head(1)).reset_index()
    inbound_cluster = inbound_cluster[['pr_id', 'level_1']]
    inbound_cluster.columns = ['pr_id', 'cluster']

    inbound_ptype = pr_reorder_pr_df4.groupby(['pr_id'])['purchase_type'].apply(
        lambda x: x.value_counts().head(1)).reset_index()
    inbound_ptype = inbound_ptype[['pr_id', 'level_1']]
    inbound_ptype.columns = ['pr_id', 'purchase_type']

    inbound_df['lead_time_pr_to_po'] = inbound_df['po_date'] - inbound_df['pr_date']
    inbound_df['lead_time_pr_to_po'] = inbound_df['lead_time_pr_to_po'].map(lambda x: pd.to_timedelta(x).days)
    inbound_df['lead_time_po_to_inbound'] = inbound_df['inbound_time'] - inbound_df['po_date']
    inbound_df['lead_time_po_to_inbound'] = inbound_df['lead_time_po_to_inbound'].map(lambda x: pd.to_timedelta(x).days)
    inbound_df['lead_time_rop_to_inbound'] = inbound_df['lead_time_rop_to_pr'] + inbound_df['lead_time_pr_to_po'] + \
                                             inbound_df['lead_time_po_to_inbound']

    inbound_df0 = inbound_df.groupby(['pr_id', 'order_id', 'inbound_id', 'supplier_name', 'supplier_id'])[
        'coverage_at_pr', 'lead_time_rop_to_pr', 'lead_time_pr_to_po', 'lead_time_po_to_inbound',
        'lead_time_rop_to_inbound'].mean().reset_index()

    inbound_df1 = inbound_df0.merge(pr_cluster, on=['pr_id'], how='left')
    inbound_df2 = inbound_df1.merge(pr_ptype, on=['pr_id'], how='left')
    inbound_df3 = inbound_df2.sort_values(['lead_time_po_to_inbound'])
    inbound_df4 = inbound_df3.groupby(['pr_id', 'order_id']).head(1)
    last_ext = ext[['supplier_id', 'avg_lead_time', 'grass_date']]
    last_ext0 = last_ext.sort_values('grass_date', ascending=False)
    last_ext1 = last_ext0.groupby('supplier_id')['supplier_id', 'avg_lead_time'].head(1)
    last_inbound = inbound_df.sort_values('inbound_time').groupby('inbound_id')['inbound_id', 'inbound_time'].tail(1)

    inbound_df4['supplier_id'] = inbound_df4['supplier_id'].astype(str)
    last_ext1['supplier_id'] = last_ext1['supplier_id'].astype(str)

    inbound_df4 = inbound_df4.merge(last_ext1, on='supplier_id', how='left')
    inbound_df4 = inbound_df4.merge(last_inbound, on='inbound_id', how='left')
    print('Finished Inbound tab')

    new_folder = './' + country + '/input_files/'
    wk_4_df8.to_csv(new_folder + 'replenishment_tab_v4.csv', encoding='utf-8-sig')
    inbound_df4.to_csv(new_folder + "inbounded_tab.csv", encoding='utf-8-sig')
    pr_reorder_pr_df8.to_csv(new_folder + "rop_to_pr.csv", encoding='utf-8-sig')
    return wk_4_df8, inbound_df4, pr_reorder_pr_df8


