import json
import os
import warnings
from contextlib import contextmanager
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

import dotenv
import numpy as np
import pandas as pd
import pyodbc
import requests
from dateutil.relativedelta import relativedelta
from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientAbortError
from imapclient.exceptions import IMAPClientError
from lumina_db.db import DB
from psycopg_pool import ConnectionPool


# region resources
def get_dropbox_link():
    try:
        json_path = (Path(os.getenv('LOCALAPPDATA')) / 'Dropbox' / 'info.json').resolve()
    except FileNotFoundError:
        json_path = (Path(os.getenv('APPDATA')) / 'Dropbox' / 'info.json').resolve()
    with open(str(json_path)) as f:
        j = json.load(f)
    return str(Path(j['business']['path']))


# region Globals
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 1000)
TODAY = dt.today().date()
NOW = dt.now()
DROPBOX = get_dropbox_link()
dotenv.load_dotenv()
warnings.filterwarnings('ignore')
BASE_PATH = f'{DROPBOX}\\Gebo_prognoza_3'
FILES_PATH = f'{DROPBOX}\\STATISTICIANUL\\files'
NEO_PATH = f'{FILES_PATH}\\vanzari_neo.pickle'
MEDIA_DATA_PATH = f'{FILES_PATH}\\media_data'
BLACKLIST_PATH = f'{DROPBOX}\\PENITENCIAR\\lista_neagra'
VAL_STOC_LUNAR_PATH = f'{FILES_PATH}\\valoare_stoc_lunar.csv'
VAL_STOC_60_ZILE_PATH = f'{FILES_PATH}\\valoare_stoc_60_zile.xlsx'
MASTERFILE_PATH = f'{DROPBOX}\\Gebo_prognoza_3\\masterfile.pickle'
VANZARI_PATH = f'{BASE_PATH}\\concat_famshop\\ready_files\\vanzari.pickle'
STOCURI_PATH = f'{FILES_PATH}\\StocuriLaData.pickle'
GAMA_PATH = f'{FILES_PATH}\\gama.csv'
FACTURISTA_PATH = f'{FILES_PATH}\\vanzari_facturista.pickle'
PENITENCIAR_PATH = f'{DROPBOX}\\PENITENCIAR'
LAST_60_DAYS = [(TODAY - timedelta(days=i)).strftime('%d %b') for i in range(60)][::-1]


# sys.path.insert(1, f'{DROPBOX}\\raw_files_py\\mother_of_utils')
# endregion


def pickle_to_csv(path):
    path = f'{DROPBOX}\\{path}' if 'C:' not in path and 'Dropbox' not in path else path
    path = f'{path}.pickle' if '.pickle' not in path else path
    df = pd.read_pickle(path)
    slash = '/' if '/' in path else '\\'
    output = path.rsplit(slash, 1)[-1].replace('.pickle', '')
    df.to_csv(f'C:\\Elvin\\000\\files\\{output}.csv', index=False)


def excel_to_pickle(path):
    path = f'{path}.xlsx' if '.xlsx' not in path else path
    df = pd.read_excel(path)
    output = path.rsplit('\\', 1)[-1].replace('.xlsx', '')
    df.to_pickle(f'C:\\Elvin\\000\\files\\{output}.pickle')


def save_test(df, df_name=None):
    df_name = [x for x in globals() if globals()[x] is df][0] if df_name is None else df_name
    if len(df_name) > 0:
        df.to_csv(f'C:\\Elvin\\000\\files\\{df_name}.csv', index=False)


# endregion

# region Dummy DataFrame
df = pd.DataFrame({'brand': ['1', 2, 3], 'color': [4, 'abc', 678], 'size': [7, 8, '9']})


# endregion ###########################################################################################################


# # region xml
# import pandas as pd
# import xml.etree.ElementTree as ET
#
#
# favi = pd.read_csv('D:\\Elvin\\000\\favi_ro.csv')
# print(favi)
#
# root = ET.Element('Produse')
#
# for i, row in favi.iterrows():
#     row_elem = ET.SubElement(root, 'Produs', {'id': str(i)})
#     for col_name, col_value in row.iteritems():
#         col_elem = ET.SubElement(row_elem, col_name)
#         col_elem.text = str(col_value)
#
# tree = ET.ElementTree(root)
# tree.write('D:\\Elvin\\000\\favi_ro.xml', encoding='utf-8', xml_declaration=True)
# # endregion

# df = pd.DataFrame({'CodProdus': ['a', 'a', 'c', 'a', 'b', 'c', 'b', 'c'], 'Cantitate': [1, 4, 3, 2, 4, 9, 5, 6]})
# df['Cantitate'] = df.groupby('CodProdus')['Cantitate'].transform('sum')
# df.drop_duplicates(inplace=True, ignore_index=True)
# print(df)


# =====================================================================3


def get_istoric_produse_moarte():  # todo de luat din fisierele stoc_la_data in parte pe fiecare zi?
    vanzari = pd.read_pickle(VANZARI_PATH)[['CodProdus', 'Data']]
    vanzari = vanzari[vanzari['Data'] >= TODAY - timedelta(days=851)]  # ultimele 25 luni + 90 zile
    vanzari.drop_duplicates(inplace=True)

    vanzari_90 = set(vanzari[vanzari['Data'] >= dt(2023, 6, 18).date() - timedelta(days=90)]['CodProdus'])
    print(vanzari_90)
    breakpoint()

    stocuri = pd.read_pickle(
        f'{FILES_PATH}\\StocuriLaData.pickle')  # todo replace with f'{FILES_PATH}\\StocuriLaData.pickle'
    stocuri['stock_date'] = pd.to_datetime(stocuri['stock_date']).dt.date
    stocuri.sort_values(['stock_date'], inplace=True, ignore_index=True)
    stocuri = stocuri[
        (stocuri['stock_date'] > TODAY - timedelta(days=7)) & (stocuri['available'] > 0)]  # todo replace 7 with 761
    stocuri.to_pickle('StocuriLaData_7.pickle')

    produse_moarte = pd.read_pickle(f'{FILES_PATH}\\produse_moarte.pickle')
    current_date = TODAY
    for day in range(761):  # 25 months
        print(f'{day + 1}: {current_date}')
        stocuri_azi = stocuri[stocuri['stock_date'] == current_date]['product_code']
        vanzari_90 = set(vanzari[vanzari['Data'] >= current_date - timedelta(days=90)]['CodProdus'])
        prod_moarte = len([c for c in stocuri_azi if c not in vanzari_90])
        produse_moarte.loc[len(produse_moarte)] = {'date': current_date, 'amount': prod_moarte}
        current_date -= timedelta(days=1)
    produse_moarte.to_pickle(f'{FILES_PATH}\\produse_moarte.pickle')


def get_stocuri_la_data_cu_warehouses():
    STOCURI_PATH = f'{FILES_PATH}\\StocuriLaData_new.pickle'
    stocuri_la_data = pd.read_pickle(STOCURI_PATH)

    i = 1
    for filename in os.listdir(f'{FILES_PATH}\\stoc_la_data'):
        try:
            df_azi = pd.read_pickle(f'{FILES_PATH}\\stoc_la_data\\{filename}')
            df_azi = df_azi[
                ['product_name', 'product_code', 'provider', 'available', 'final_stock_value', 'warehouse_id']]
            df_azi['final_stock_value'] = df_azi['final_stock_value'].astype(float)
            df_azi['available1'] = df_azi['available']
            groups = df_azi.groupby('product_code')
            df_azi = pd.DataFrame()
            for name, group in groups:  # takes alot of time
                group.loc[(group['provider'].str.lower().isin(['persoana fizica', 'necunoscut', 'gebo'])) | (
                    group['provider'].apply(lambda x: 'gebo' in x.lower())), 'available1'] = 0
                group['provider'] = group.loc[group['available1'].idxmax(), 'provider']
                df_azi = pd.concat([df_azi, group])
            df_azi.drop(columns=['available1'], inplace=True)
            df_azi['final_stock_value'] = df_azi.groupby(['product_code'])['final_stock_value'].transform('sum')
            df_azi['available'] = df_azi.groupby(['product_code'])['available'].transform('sum')
            df_azi.drop_duplicates(subset='product_code', inplace=True)
            df_azi['stock_date'] = dt.strptime(filename.split('.')[0], '%Y-%m-%d').date()
            stocuri_la_data = pd.concat([stocuri_la_data, df_azi])
        except:
            pass
        print(f'{i}/834: {filename}')
        i += 1

    stocuri_la_data.drop_duplicates(inplace=True)
    stocuri_la_data.reset_index(inplace=True, drop=True)
    stocuri_la_data.to_pickle(STOCURI_PATH)
    stocuri_la_data.to_csv(f'{FILES_PATH}\\StocuriLaData_new.csv', index=False)


######################################


# def get_produse_moarte_azi(data, prod_moarte):
#     vanzari = pd.read_pickle(VANZARI_PATH)[['CodProdus', 'Data']]
#     vanzari.drop_duplicates(inplace=True)
#     vanzari_90 = set(vanzari[vanzari['Data'] >= data - timedelta(days=90)]['CodProdus'])
#
#     stocuri = pd.read_pickle(f'{FILES_PATH}\\StocuriLaData.pickle')[
#         ['product_code', 'stock_date', 'available', 'warehouse_id']]
#     stocuri = stocuri[~stocuri['warehouse_id'].isin(['0', '3', '6', '7'])]
#     stocuri_azi = stocuri[(stocuri['stock_date'] == data) & (stocuri['available'] > 0)]['product_code']
#
#     moarte = len([c for c in stocuri_azi if c not in vanzari_90])
#
#     # try:
#     #     prod_moarte = pd.read_pickle(f'{FILES_PATH}\\produse_moarte_warehouses.pickle')
#     # except:
#     #     prod_moarte = pd.DataFrame()
#
#     prod_moarte = pd.concat([pd.DataFrame({'date': [data], 'amount': [moarte]}), prod_moarte], ignore_index=True)
#     prod_moarte.drop_duplicates(subset='date', inplace=True, ignore_index=True)
#     return prod_moarte
#
#
# def add_stoc_pachete(df):
#     def calculate_stoc_pachet(contained_products, df):
#         if len(str(contained_products)) > 1 and str(contained_products) != 'nan':
#             min_stock = 9999
#             contained_products = str(contained_products).replace("'", '"')
#             for product in json.loads(contained_products):
#                 if product['sku'] in list(df['id']):
#                     produs = df.loc[df['id'] == product['sku']]
#                     if int(produs['stock'].iloc[0] / product['qty']) < min_stock:
#                         min_stock = int(produs['stock'].iloc[0] / product['qty'])
#             return min_stock if min_stock > 0 else 0
#
#     base_path = f'{DROPBOX}\\PRELUATORUL'
#     pachete = pd.read_excel(f'{base_path}\\pachete.xlsx', usecols=['ExternalProductCode', 'ProductCode'])
#     pachete = pachete.groupby('ExternalProductCode').apply(
#         lambda x: [{'sku': p, 'qty': x[x['ProductCode'] == p].shape[0]} for p in
#                    x['ProductCode'].unique()]).reset_index()
#     pachete.rename(columns={'ExternalProductCode': 'id', 0: 'contained_products'}, inplace=True)
#     df = pd.merge(df, pachete, on='id', how='left')
#     df.loc[df['id'].str.contains('PACHET'), 'stock'] = df['contained_products'].apply(calculate_stoc_pachet, df=df)
#     df['stock'].fillna(0, inplace=True)
#     df.drop(columns=['contained_products'], inplace=True)
#     return df
#
#
# stocuri = pd.read_pickle(STOCURI_PATH)
# stocuri = stocuri[stocuri['stock_date'] == dt(2023, 8, 14).date()]
# stocuri.to_csv('StocuriLaData_8aug.csv', index=False)
# stocuri['stock_date'] = pd.to_datetime(stocuri['stock_date'])
#
# stoc_zile = stocuri[stocuri['stock_date'] > pd.to_datetime(TODAY - timedelta(days=60))]
# stoc_zile = stoc_zile.groupby('stock_date')['final_stock_value'].sum().reset_index()
# print(stoc_zile)


# region DB
@contextmanager
def lumina() -> ConnectionPool:
    pool = ConnectionPool(kwargs={
        'host': os.getenv('LUMINA_HOST'),
        'port': os.getenv('LUMINA_PORT'),
        'dbname': os.getenv('LUMINA_DB'),
        'user': os.getenv('LUMINA_USER'),
        'password': os.getenv('LUMINA_PASSWORD'),
        'application_name': 'Elvin Test'
    })
    try:
        yield pool
    finally:
        pool.close()


def get_db():
    with lumina() as pool:
        with pool.connection() as conn:
            query = f'''
                SELECT date, margin
                FROM "SalesNEO"
                WHERE EXTRACT(MONTH FROM date::DATE) = {NOW.month}
                AND EXTRACT(YEAR FROM date::DATE) = {NOW.year}
                AND product_code <> '704'
                ORDER BY date
            '''
            return pd.read_sql(query, conn)


def get_db_simple():
    with lumina() as pool:
        with pool.connection() as conn:
            return pd.read_sql('SELECT date, margin FROM "SalesNEO"', conn)


# endregion


# def get_coduri_gb(df):
#     penitenciar = pd.read_pickle(f'{DROPBOX}\\PENITENCIAR\\raw_files\\penitenciar.pickle')[['sku', 'cod_furnizor']]
#     penitenciar.rename(columns={'sku': 'cod_gb', 'cod_furnizor': 'sku'}, inplace=True)
#     return pd.merge(df, penitenciar, how='left', on='sku')
#
#
# def split_images(df):
#     data = []
#     for i, r in df.iterrows():
#         for img in str(r['images']).split(', '):
#             data.append({'sku': r['sku'],
#                          'cod_gb': r['cod_gb'],
#                          'url': f"https://songmics.de{r['url']}",
#                          'img': img})
#     return pd.DataFrame(data)
#
#
# song = pd.read_pickle('songmics_products.pickle')
# song = get_coduri_gb(song)
# song = split_images(song)
# song.to_excel('songmics_products_imgs.xlsx', index=False)
#
# ===================================================================================================


###############################################################################################################
def make_request(list_of_cuis: list, method='POST', correlation_id: str = ''):
    url_param = f'?id={correlation_id}'
    url = f'https://webservicesp.anaf.ro/AsynchWebService/api/v6/ws/tva{url_param}'
    print([{'cui': cui, 'data': dt.today().strftime('%Y-%m-%d')} for cui in list_of_cuis])
    response = requests.Session().request(
        method=method,
        url=url,
        headers={'Content-Type': 'application/json'},
        json=[{'cui': cui, 'data': dt.today().strftime('%Y-%m-%d')} for cui in list_of_cuis],
        timeout=10
    )
    print(response.content)  # todo remove
    return json.loads(response.content) if response.ok else False


def get_daktela_calls():
    token = '81a63c6098fdd3f57bf2f7935f023fa4afa8da22'
    response = requests.get(f'https://gebotools.daktela.com/api/v6/activities.json?accessToken={token}')
    time_str = '%Y-%m-%d %H:%M:%S'
    calls = pd.DataFrame([{'agent': entry['item']['id_agent']['title'],
                           'phone_number': entry['title'],
                           'direction': entry['item']['direction'],
                           'duration': int((dt.strptime(entry['item']['cdr']['time_closed'], time_str) - dt.strptime(
                               entry['time'], time_str)).total_seconds())}
                          for entry in response.json()['result']['data'] if entry['item']['answered']])
    print(f'calls: {calls}')  # todo remove


def get_current_call_phone():
    daktela_token = '81a63c6098fdd3f57bf2f7935f023fa4afa8da22'
    response = requests.get(f'https://gebotools.daktela.com/api/v6/activitiesCall.json?accessToken={daktela_token}')
    for a in response.json()['result']['data'][0]['activities']:
        if a['user']['title'] == 'user1' and a['time_open'] is not None and a['time_close'] is None:
            return a['title']
    return 0


def split_poze(filename):
    poze = pd.read_excel(f'{filename}.xlsx')
    poze.dropna(subset=['images'], inplace=True)
    poze['images'] = poze['images'].str.rstrip(',')  # remove trailing ,
    result = pd.DataFrame(columns=['Cod GL', 'image'])
    for i, row in poze.iterrows():
        for img in row['images'].split(','):
            result.loc[len(result)] = {'Cod GL': row['Cod GL'], 'image': img}
    result.to_excel(f'{filename}_split.xlsx', index=False)


def split_poze_2(filename):
    df = pd.read_excel(filename)
    melt = df.melt(id_vars=['SKU'], var_name='Image', value_name='Image_URL')
    melt = melt.dropna(subset=['Image_URL'])
    melt = melt.drop(columns=['Image'])
    melt['nr'] = melt['Image_URL'].apply(lambda x: int(x.split('/')[-1].split('.')[0]))
    melt.sort_values(['SKU', 'nr'], inplace=True, ignore_index=True)
    melt.to_excel(f'{filename}_split.xlsx', index=False)


def concat_to_penitenciar(filename):  # must be 'xlsx'
    penitenciar = pd.read_pickle(f'{PENITENCIAR_PATH}\\raw_files\\penitenciar.pickle')
    penitenciar.to_pickle(f'{PENITENCIAR_PATH}\\raw_files\\penitenciar_backup.pickle')
    penitenciar.to_csv(f'{PENITENCIAR_PATH}\\raw_files\\penitenciar_backup.csv', index=False)
    atas = pd.read_excel(f'{filename}.xlsx')
    len_before = len(penitenciar)
    try:
        atas['maturity'] = atas['maturity'].apply(lambda x: x.date())
    except:
        try:
            atas['maturity'] = atas['maturity'].apply(lambda x: dt.strptime(x, '%Y-%m-%d').date())
        except:
            atas['maturity'] = TODAY
    penitenciar = pd.concat([penitenciar, atas], ignore_index=True)
    penitenciar.to_pickle(f'{PENITENCIAR_PATH}\\raw_files\\penitenciar.pickle')
    penitenciar.to_csv(f'{PENITENCIAR_PATH}\\raw_files\\penitenciar.csv', index=False)

    print(f'len penitenciar before: {len_before}')
    print(f'len atas: {len(atas)}')
    print(f'len penitenciar after: {len(penitenciar)}')
    print('✅ succes' if len_before + len(atas) == len(penitenciar) else '❌ error')


def anaf_request(cui):
    try:
        response = requests.post(
            url='https://webservicesp.anaf.ro/PlatitorTvaRest/api/v8/ws/tva',
            json=[{'cui': cui, 'data': TODAY.strftime('%Y-%m-%d')}]
        )
        info = response.json()['found'][0]['date_generale']
        return {'denumire': info['denumire'], 'adresa': info['adresa'], 'telefon': info['telefon']}
    except:
        return {}


def log_run(payload='test'):
    with lumina() as pool:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                insert_query = '''INSERT INTO "ComenziInAsteptareLog" ("run_time", "payload") VALUES (%s, %s)'''
                cursor.execute(insert_query, (NOW, payload))

                # region Keep only most recent 400 entries
                total_rows = cursor.execute('SELECT COUNT(*) FROM "ComenziInAsteptareLog"').fetchone()[0]
                if total_rows > 4:
                    delete_query = '''
                        DELETE FROM "ComenziInAsteptareLog"
                        WHERE "run_time" IN (
                            SELECT "run_time"
                            FROM "ComenziInAsteptareLog"
                            ORDER BY "run_time" ASC
                            LIMIT %s
                        )
                    '''
                    cursor.execute(delete_query, (total_rows - 4,))
                # endregion

                conn.commit()


def get_last_run():
    with lumina() as pool:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT "run_time" FROM "ComenziInAsteptareLog" ORDER BY "run_time" DESC LIMIT 1')
                return cursor.fetchone()[0]


def get_from_arulog():
    driver = '{ODBC Driver 18 for SQL Server}'
    conn = pyodbc.connect(
        f'DRIVER={driver};SERVER=192.168.2.87,1450;DATABASE=gebotools;UID=sa;PWD=111111;TrustServerCertificate=YES',
        readonly=True)
    return pd.read_sql('SELECT * FROM vColectareMultipla', conn)


def get_penitenciar_diferente_maturitati():
    pickle = pd.read_csv(f'{PENITENCIAR_PATH}\\raw_files\\penitenciar.csv')[['sku', 'cod_furnizor', 'maturity']]
    pickle.sort_values(['sku'], inplace=True, ignore_index=True)
    pickle['sku'] = pickle['sku'] + ' - ' + pickle['cod_furnizor']
    pickle['maturity'] = pickle['maturity'].astype(str)
    pickle['maturity'] = pickle['maturity'].apply(lambda x: x.split()[0])
    pickle.drop(columns=['cod_furnizor'], inplace=True)

    with lumina() as pool:
        with pool.connection() as conn:
            db = pd.read_sql('SELECT * FROM "ProductsPenitentiary"', conn)[
                ['product_code', 'supplier_code', 'maturity_date']]
    db.sort_values(['product_code'], inplace=True, ignore_index=True)
    db['maturity_date'] = db['maturity_date'].apply(lambda x: x.split()[0])
    db['product_code'] = db['product_code'] + ' - ' + db['supplier_code']
    db.drop(columns=['supplier_code'], inplace=True)

    i = 0
    result = pd.DataFrame(columns=['sku', 'pickle_maturity', 'db_maturity'])
    for sku in pickle['sku']:
        try:
            pickle_maturity = pickle[pickle['sku'] == sku].iloc[0]['maturity']
            db_maturity = db[db['product_code'] == sku].iloc[0]['maturity_date']
        except:
            continue

        if pickle_maturity != db_maturity:
            result.loc[len(result)] = {'sku': sku, 'pickle_maturity': pickle_maturity, 'db_maturity': db_maturity}
            i += 1
            print(f'{i}. {sku}:')
            print(f'   pickle_maturity: {pickle_maturity}')
            print(f'   db_maturity: {db_maturity}')
            print('--------------------------')

    result.to_csv('diferente_maturitati.csv', index=False)


def set_utb_supplier_true():
    with lumina() as pool:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                update_query = '''UPDATE "ProductsPenitentiary"
                                  SET only_supplier = TRUE
                                  WHERE brand = %s'''
                cursor.execute(update_query, ('UTB2',))
                conn.commit()


def get_neintrerupte():
    CODURI_TEST = ['GB0007']  # todo remove 'GB0002',
    with lumina() as pool:  # get stocuri
        with pool.connection() as conn:
            query = f'''
            SELECT product_code, available, date_requested_for
            FROM "StockNEO"
            WHERE warehouse_id NOT IN ('3','6','10')
            AND date_requested_for >= %s AND date_requested_for < %s
            AND product_code IN ({','.join(['%s'] * len(CODURI_TEST))})
            '''  # todo remove 'ORDER BY date_requested_for' and 'AND product_code IN...'
            stocuri = pd.read_sql(query, conn, params=[TODAY - timedelta(days=60),
                                                       TODAY] + CODURI_TEST)  # todo remove ' + CODURI_TEST'

    print(stocuri)  # todo remove

    stocuri.drop_duplicates(subset=['product_code', 'date_requested_for'], inplace=True)
    coduri_in_stoc = stocuri.loc[(stocuri['date_requested_for'] == TODAY - timedelta(days=1)), 'product_code'].tolist()
    stocuri = stocuri.loc[stocuri['product_code'].isin(coduri_in_stoc)]  # pastram doar codurile care au avut ieri stoc
    stocuri.sort_values(['product_code', 'date_requested_for'], ascending=False, inplace=True, ignore_index=True)

    def get_neintrerupte_group(group):
        group.reset_index(drop=True, inplace=True)
        previous_date = TODAY
        cod = group.iloc[0]['product_code']
        for i, row in group.iterrows():
            if row['date_requested_for'] != previous_date - timedelta(days=1):
                neintrerupte.loc[len(neintrerupte)] = {'product_code': cod, 'zile_in_stoc_neintrerupte': i}
                return
            previous_date -= timedelta(days=1)
        neintrerupte.loc[len(neintrerupte)] = {'product_code': cod, 'zile_in_stoc_neintrerupte': len(group)}

    neintrerupte = pd.DataFrame(columns=['product_code', 'zile_in_stoc_neintrerupte'])
    stocuri.groupby('product_code', group_keys=False).apply(get_neintrerupte_group)

    print(neintrerupte)


##############################################

def get_source_weights():
    with lumina() as pool:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f'SELECT sku, weight FROM "StoreProducts" WHERE country_code = %s', ('RO',))
                return pd.DataFrame(cursor.fetchall(), columns=['sku', 'weight'])


def is_valid_date(year, month, day):
    try:
        dt(year, month, day)
        return True
    except:
        return False


def filter_timespan(x, timespan):
    if timespan == 'MTD':
        return x.year == TODAY.year and x.month == TODAY.month
    if timespan == 'YTD':
        return x.year == TODAY.year
    if isinstance(x, pd.Timestamp):
        x = x.date()
    if timespan == 'Last MTD':
        month_ago = TODAY - relativedelta(months=1)
        start = month_ago.replace(day=1)
        try:
            end = start.replace(day=TODAY.day)
        except ValueError:
            end = TODAY.replace(day=1) - relativedelta(days=1)

        # st.text(f'start: {start} | end: {end}')  # todo remove
        return start <= x <= end
    if timespan == 'Last YMTD':
        return TODAY.replace(year=TODAY.year - 1, day=1) <= x <= TODAY.replace(year=TODAY.year - 1,
                                                                               day=TODAY.day if TODAY.month != 2 or TODAY.day != 29 else 28)
    if timespan == 'Last YTD':
        return TODAY.replace(year=TODAY.year - 1, month=1, day=1) <= x <= TODAY.replace(year=TODAY.year - 1,
                                                                                        day=TODAY.day if TODAY.month != 2 or TODAY.day != 29 else 28)
    if timespan == 'ALL TIME':
        return True
    return x == TODAY


# TIMESPANS = ['TODAY', 'MTD', 'YTD', 'Last MTD', 'Last YMTD', 'Last YTD']
# with lumina() as pool:
#     with pool.connection() as conn:
#         query = f'''
#         SELECT product_code, date_requested_for, available, final_stock_value, warehouse_id
#         FROM "StockNEO" WHERE date_requested_for >= %s ORDER BY date_requested_for'''
#         stoc = pd.read_sql(query, conn, params=[dt(TODAY.year - 1, 1, 1)])
# stoc.rename(columns={'product_code': 'sku', 'date_requested_for': 'data', 'available': 'stoc'}, inplace=True)
# stoc['stoc_gunoi'] = stoc.apply(lambda x: x['stoc'] if x['warehouse_id'] in [3, 10] else 0, axis=1)
# stoc['valoare_gunoi'] = stoc.apply(lambda x: x['final_stock_value'] if x['warehouse_id'] in [3, 10] else 0, axis=1)
# stoc = stoc[['sku', 'data', 'stoc', 'stoc_gunoi', 'valoare_gunoi']]
#
# rows = ['Valoare totală stoc din pierderi', 'Diferența dintre intrate și ieșite (val)', 'Total bucăți',
#         'Total coduri', 'Greutate totală (kg)', 'Greutate pierderi (kg)', 'Cost chirie (€)']
# tabel = pd.DataFrame(columns=TIMESPANS[1:], index=rows).fillna(0)
#
# weights = get_source_weights()
# weights['weight'] = weights['weight'].astype(float)
# for ts in TIMESPANS[1:]:
#     ds = stoc[stoc['data'].apply(filter_timespan, timespan=ts)].reset_index(drop=True)
#
#     # region Cost Chirie
#     if ts in ['MTD', 'YTD']:
#         chirie_ds = ds[ds['data'] == TODAY]
#     elif ts == 'Last MTD':
#         year = TODAY.year - 1 if TODAY.month == 1 else TODAY.year
#         month = 12 if TODAY.month == 1 else TODAY.month - 1
#         day = TODAY.day
#         while not is_valid_date(year, month, day):
#             day -= 1
#         chirie_ds = ds[ds['data'] == dt(year, month, day).date()]
#     else:  # 'Last YTD', 'Last YMTD'
#         chirie_ds = ds[ds['data'] == TODAY.replace(year=TODAY.year - 1,
#                                                    day=TODAY.day if TODAY.month != 2 or TODAY.day != 29 else 28)]
#     chirie_ds = pd.merge(chirie_ds, weights, how='left', on='sku')
#     chirie_ds['greutate_gunoi'] = chirie_ds['stoc_gunoi'] * chirie_ds['weight']
#     chirie_ds['weight'] = chirie_ds['stoc'] * chirie_ds['weight']
#     tabel.at['Greutate totală (kg)', ts] = chirie_ds['weight'].sum()
#     tabel.at['Greutate pierderi (kg)', ts] = chirie_ds['greutate_gunoi'].sum()
#     tabel.at['Cost chirie (€)', ts] = chirie_ds['greutate_gunoi'].sum() * 14000 / chirie_ds['weight'].sum()
#     # endregion
#
#     if 'M' in ts:
#         tabel.at['Valoare totală stoc din pierderi', ts] = ds['valoare_gunoi'].sum() / TODAY.day
#     else:  # YTD / Last YTD
#         if len(ds) > 0:
#             tabel.at['Valoare totală stoc din pierderi', ts] = \
#                 ds[ds['data'].apply(lambda x: x.month == TODAY.month and x.day == TODAY.day)]['valoare_gunoi'].sum()
#         else:
#             tabel.at['Valoare totală stoc din pierderi', ts] = 0
#
#     suma_inceput = ds[ds['data'] == ds.iloc[0]['data']]['valoare_gunoi'].sum()
#     suma_final = ds[ds['data'] == ds.iloc[-1]['data']]['valoare_gunoi'].sum()
#     tabel.at['Diferența dintre intrate și ieșite (val)', ts] = suma_final - suma_inceput
#     tabel.at['Total bucăți', ts] = ds[ds['data'] == ds.iloc[-1]['data']]['stoc_gunoi'].sum()
#     tabel.at['Total coduri', ts] = ds[ds['data'] == ds.iloc[-1]['data']]['sku'].count()
#
# print(tabel)


def get_penitenciar():
    with lumina() as pool:
        with pool.connection() as conn:
            penitenciar = pd.read_sql('SELECT *, ctid FROM "ProductsPenitentiary" ORDER BY "maturity_date"', conn)
    penitenciar.columns = ['sku', 'site_furnizor', 'denumire', 'brand', 'cod_furnizor', 'ultim_adaos_acceptat',
                           'categorie_de_risc', 'maturity', 'Furnizor', 'only_supplier', 'ctid']
    penitenciar = penitenciar[penitenciar['sku'].apply(lambda x: x.strip()) != '']
    penitenciar['denumire'].fillna('', inplace=True)
    return penitenciar


def cuplare(principal, secundar):
    penitenciar = get_penitenciar()
    rows_secundar = penitenciar[penitenciar['sku'] == secundar]
    rows_secundar['sku'] = principal
    new_row = pd.DataFrame([{'sku': principal, 'cod_furnizor': secundar, 'Furnizor': 'cuplare'}])
    rows_to_insert = pd.concat([new_row, rows_secundar])

    print("\n insert_df_into_table('ProductsPenitentiary', ):")
    print(rows_to_insert)

    print(f'\n-> DELETE FROM "ProductsPenitentiary" WHERE product_code = {secundar}')
    print('✅ Cuplare efectuată!')


def get_lumina_db() -> DB:
    """
    Returns internally cached resource of Lumina DB pool connection
    """

    dotenv.load_dotenv()  # we expect credentials be present in .env

    host = os.getenv('LUMINA_HOST')
    port = os.getenv('LUMINA_PORT')
    user = os.getenv('LUMINA_USER')
    password = os.getenv('LUMINA_PASSWORD')

    return DB(host=host,
              port=port,
              dbname='lumina',
              user=user,
              password=password,
              application_name='test',
              single_connection=True)


def get_orders(limit=10):
    response = requests.get(url=f'http://192.168.2.81:8000/api/orders?limit={limit}&offset=0',
                            auth=('root', 'test123123'))
    return json.loads(response.content)['results']


def get_orders_by_ids(ids):
    response = requests.get('http://192.168.2.81:8000/api/orders/by_order_ids/',
                            json={'order_ids': ids},
                            auth=('root', 'test123123'))
    return pd.DataFrame(response.json())


# region EMAILS
def fetch_email_data(client, msgid):
    response = client.fetch(msgid, ["ENVELOPE", "BODY[TEXT]"])
    return response[msgid]


def get_envelope_field(fetched_data, field):
    envelope = fetched_data[b"ENVELOPE"]
    field = field.lower()

    if field == "date":
        return envelope.date
    elif field == "subject":
        return envelope.subject.decode() if envelope.subject else None
    elif field == "from":
        return _format_address_list(envelope.from_)
    elif field == "to":
        return _format_address_list(envelope.to)
    elif field == "cc":
        return _format_address_list(envelope.cc)
    elif field == "bcc":
        return _format_address_list(envelope.bcc)
    elif field == "in_reply_to":
        return envelope.in_reply_to.decode() if envelope.in_reply_to else None
    elif field == "message_id":
        return envelope.message_id.decode() if envelope.message_id else None
    elif field == "body":
        body = fetched_data[b"BODY[TEXT]"]
        return body.decode("utf-8", errors="replace")
    else:
        raise ValueError(f"Invalid field: {field}. Valid options are 'date', 'subject', 'from', "
                         "'to', 'cc', 'bcc', 'in_reply_to', 'message_id', 'body'.")


def _format_address_list(address_list):
    """
    Helper function to format a list of IMAPClient address tuples.
    """
    if not address_list:
        return None
    formatted_addresses = []
    for address in address_list:
        name = address.name.decode() if address.name else ""
        mailbox = address.mailbox.decode() if address.mailbox else ""
        host = address.host.decode() if address.host else ""
        email = f"{mailbox}@{host}" if mailbox and host else ""
        formatted_addresses.append(f"{name} <{email}>".strip())
    return formatted_addresses if len(formatted_addresses) > 1 else formatted_addresses[0]


@contextmanager
def get_imap_client(username: str, password: str, host: str) -> IMAPClient:
    server = IMAPClient(host=host)
    try:
        server.login(username, password)
        yield server
    finally:
        try:
            server.logout()
        except (IMAPClientError, IMAPClientAbortError):
            server.shutdown()


def get_emails_by_address(email_address):
    COUNTRY = 'RO'
    with get_imap_client(os.getenv(f'WEBMAIL_USER_{COUNTRY}'), os.getenv(f'WEBMAIL_PASS_{COUNTRY}'),
                         os.getenv(f'WEBMAIL_URL_{COUNTRY}')) as client:
        client.select_folder('INBOX')
        message_ids = client.search(["OR", "FROM", email_address, "TO", email_address])
        if not message_ids:
            return []
        emails = []
        for msgid in message_ids:
            fetched_data = fetch_email_data(client, msgid)
            emails.append({
                "Date": get_envelope_field(fetched_data, "date"),
                "Subject": get_envelope_field(fetched_data, "subject"),
                "From": get_envelope_field(fetched_data, "from"),
                "To": get_envelope_field(fetched_data, "to"),
                "Body": get_envelope_field(fetched_data, "body")
            })
        return emails


def add_order_to_db(order):
    with get_lumina_db().cursor() as cursor:
        order['products'] = json.dumps(order['products'])  # todo remove
        columns = list(order.keys())
        values = [order[col] for col in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        column_names = ", ".join(columns)
        query = f'INSERT INTO "SalesHistory" ({column_names}) VALUES ({placeholders})'
        cursor.execute(query, values)
        cursor.connection.commit()


def price_per_gram_protein(pret, greutate_g, procent_proteina_la_100g):
    return f'{round(pret / ((procent_proteina_la_100g / 100) * greutate_g), 2)} lei/1g proteină'


def get_courier_type(awb):
    err = 'Curier nerecunoscut!'
    if any(awb.count(s) == 1 for s in ['EMG', 'ONB']):
        if 15 < len(awb) < 20:
            return 'SAMEDAY'
        return err
    if any(awb.startswith(x) for x in ['PNZW', 'PBZW']):
        if len(awb) == 26:
            return 'POSTAHU'
        return err
    if 'TRP' in awb:
        return 'TRANSILVANIAPOST'
    if 'GBW' in awb or len(awb) == 20:
        return 'GBW'
    if awb.startswith('1') and len(awb) in [10, 13] and all(c.isdigit() for c in awb):
        return 'CARGUS'
    if len(awb) in [13, 15]:
        return 'ECONT'
    if len(awb) == 11:
        return 'MEMEX'
    if len(awb) == 10:
        return 'TAXYDROMIKI'
    if len(awb) == 9:
        return 'TAXYDROMIKI'
    if len(awb) == 8:
        return 'DRAGON'
    return err


def insert_phu_feed_in_db():  # insert df to table
    phu_feed_url = "http://zzz.csv"
    phu = pd.read_csv(phu_feed_url, sep=';', header=0, encoding='utf-8-sig', on_bad_lines='skip')
    print(len(phu))
    phu.replace(np.nan, None, inplace=True)
    placeholders = ", ".join(["%s"] * len(phu.columns))
    col_sql = ", ".join(f'"{c}"' for c in phu.columns)
    query = f'INSERT INTO "PHUStock" ({col_sql}) VALUES ({placeholders})'
    with get_lumina_db().pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, [tuple(x) for x in phu.to_numpy()])
            conn.commit()


########################################################################################################################


print('conflict pe main')
