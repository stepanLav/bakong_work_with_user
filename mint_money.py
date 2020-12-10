import binascii
import sys
import trace
from datetime import datetime

import psycopg2
from iroha import IrohaCrypto
from iroha import Iroha, IrohaGrpc

from db_connection import Connection

IROHA_HOST_ADDR = '127.0.0.1'
IROHA_PORT = '2346'
ADMIN_ACCOUNT_ID = 'admin@nbc'
ADMIN_PRIVATE_KEY = '72a9eb49c0cd469ed64f653e33ffc6dde475a6b9fd8be615086bce7c44b5a8f8'
iroha = Iroha(ADMIN_ACCOUNT_ID)
net = IrohaGrpc('{}:{}'.format(IROHA_HOST_ADDR, IROHA_PORT))
select_from_test_db = "SELECT account.* FROM account LEFT JOIN account_has_asset ON (account_has_asset.account_id=account.account_id) WHERE account.domain_id = 'bank1' and account_has_asset.account_id is null limit 1000;"
update_to_bank_db = "UPDATE fst_iroha_account SET assets = %s WHERE account_id = %s;"
"SELECT table2.* FROM table2 LEFT JOIN table1 ON (table2.id=table1.id) WHERE table1.id IS NULL"
# select_from_test_db = "UPDATE bank1 SET have_money = TRUE, used = TRUE WHERE bank1.id IN (SELECT i.id FROM bank1_values_randomized r INNER JOIN bank1 i ON i.id = r.id WHERE (NOT i.used) and r.have_money=false LIMIT 100) RETURNING account_id;"


def transaction_builder(account_list):
    bath = []
    for account_id in account_list:
        bath.append(iroha.command('TransferAsset', src_account_id=ADMIN_ACCOUNT_ID, dest_account_id=account_id[0],
                                  asset_id='usd#nbc', description='', amount='100000'))
        bath.append(iroha.command('TransferAsset', src_account_id=ADMIN_ACCOUNT_ID, dest_account_id=account_id[0],
                                  asset_id='khr#nbc', description='', amount='100000'))
    return bath


def bath_request_builder(account_list, cursor, assets, query):
    for where_condition in account_list:
        cursor.execute(query, (assets, where_condition))


def send_transaction_and_print_status(transaction):
    hex_hash = binascii.hexlify(IrohaCrypto.hash(transaction))
    print('Transaction hash = {}, creator = {}'.format(
        hex_hash, transaction.payload.reduced_payload.creator_account_id))
    net.send_tx(transaction)
    for status in net.tx_status_stream(transaction):
        print(status)
        if status[1] != 5:
            continue


def transfer_coin_from_admin_to_userone(account_list):
    init_cmds = transaction_builder(account_list=account_list)
    init_tx = iroha.transaction(init_cmds)
    IrohaCrypto.sign_transaction(init_tx, ADMIN_PRIVATE_KEY)
    send_transaction_and_print_status(init_tx)


def make_sql_request(user, password, host, port, database, query, where_account=None, set_data=None):
    try:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
        cursor = connection.cursor()
        if where_account is not None:
            for account in where_account:
                cursor.execute(query, (set_data, account[0]))
            connection.commit()
            print("Transaction " + query + " commited!")
        else:
            cursor.execute(query)
            account_id_list = cursor.fetchall()
            return account_id_list


    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def send():
    print(datetime.now())
    assets = '{"khr#nbc": 10000.00, "usd#nbc": 10000.00}'
    account_list = make_sql_request(database="c_iroha_0_2cb834b_10001", user="postgres",
                                    password="postgres",
                                    host="localhost", port=2349,
                                    query=select_from_test_db)
    print(account_list)
    print(datetime.now())
    transfer_coin_from_admin_to_userone(account_list)
    # make_sql_request(database="tps", user="tps", password="Cig30A2zP8TXdTCpT52JoIvhr981Bc",
    #                  host="localhost", port=2347,
    #                  query=update_to_bank_db, where_account=account_list, set_data=assets)


i = 0
while i < 1:
    send()
    i += 1


    # account_list = make_sql_request(database="load_test_data", user="postgres",
    #                                 password="Zobc1PYo4thTGCdy9j4bOQ9GNEgVo",
    #                                 host="localhost", port=2345,
    #                                 query=select_from_test_db)
