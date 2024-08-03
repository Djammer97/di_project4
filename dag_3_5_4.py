from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.variable import Variable

from sqlalchemy import create_engine
import psycopg2

import pandas as pd
import json

from datetime import datetime, timedelta, date

from my_libs.mongo_connect import MongoConnect
from my_libs.json_work import *


pg_source_conn = BaseHook.get_connection("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
dest_conn = BaseHook.get_connection("PG_WAREHOUSE_CONNECTION")

mongo_cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
mongo_db_user = Variable.get("MONGO_DB_USER")
mongo_db_pw = Variable.get("MONGO_DB_PASSWORD")
mongo_rs = Variable.get("MONGO_DB_REPLICA_SET")
mongo_db = Variable.get("MONGO_DB_DATABASE_NAME")
mongo_host = Variable.get("MONGO_DB_HOST")
mongo_connect = MongoConnect(mongo_cert_path, mongo_db_user, mongo_db_pw, mongo_host, mongo_rs, mongo_db, mongo_db)

amount_rows_per_session = 10000

def download_from_postgres_task(*args, **kwargs):
    src_str = f"postgresql://{pg_source_conn.login}:{pg_source_conn.password}@{pg_source_conn.host}:{pg_source_conn.port}/{pg_source_conn.schema}?sslmode=require&sslcert=C:/Users/user/Desktop/course/CA.pem"
    dst_str = f"postgresql://{dest_conn.login}:{dest_conn.password}@{dest_conn.host}:{dest_conn.port}/{dest_conn.schema}?options=-csearch_path=stg"

    src_engine = create_engine(src_str)
    dst_engine = create_engine(dst_str)

    query = f"SELECT * FROM {kwargs['bd_name_src']}"
    df = pd.read_sql_query(query, src_engine)

    df.to_sql(kwargs['bd_name_dst'], dst_engine, if_exists="replace", index=False)

def download_from_postgres_incremental(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection, \
         psycopg2.connect(dbname=pg_source_conn.schema, user=pg_source_conn.login, password=pg_source_conn.password, host=pg_source_conn.host, port=pg_source_conn.port) as src_connection:

        dest_cursor = dest_connection.cursor()
        source_cursor = src_connection.cursor()

        dest_cursor.execute("SELECT workflow_settings FROM stg.srv_wf_settings WHERE workflow_key='outbox_last_id'")
        last_id = dest_cursor.fetchone()

        if not last_id:
            last_id = -1
        else:
            last_id = last_id[0]['last_id']

        source_cursor.execute("SELECT * FROM outbox WHERE id > %s ORDER BY id", [last_id])
        data = source_cursor.fetchall()

        if data:
            last_id = data[-1][0]

            for row in data:
                dest_cursor.execute("INSERT INTO stg.bonussystem_events VALUES (%s, %s, %s, %s)", row)
                dest_cursor.execute("INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings) VALUES (%s, %s) ON CONFLICT(workflow_key) DO UPDATE SET workflow_settings=%s", ('outbox_last_id', json.dumps({'last_id': row[0]}), json.dumps({'last_id': row[0]})))

        dest_connection.commit()

def download_from_mongo_incremental(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:

        mongo_client = mongo_connect.client()

        dest_cursor = dest_connection.cursor()

        dest_cursor.execute(f"SELECT workflow_settings FROM stg.srv_wf_settings WHERE workflow_key='{kwargs['workflow_key']}'")
        last_date = dest_cursor.fetchone()

        if not last_date:
            last_date = datetime(1900, 1, 1)
        else:
            last_date = datetime.fromisoformat(last_date[0]['last_date'])

        docs = list(mongo_client.get_collection(kwargs['bd_name_src']).find(filter={'update_ts': {'$gt': last_date}}, sort=[('update_ts', 1)], limit=amount_rows_per_session))

        if docs:
            querry = '''
                INSERT INTO stg.{table}(object_id, object_value, update_ts)
                VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                ON CONFLICT (object_id) DO UPDATE
                SET 
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
            '''

            for one_doc in docs:
                dest_cursor.execute(querry.format(table=kwargs['bd_name_dst']), {'object_id': str(one_doc['_id']), 'object_value': json2str(one_doc), 'update_ts': one_doc['update_ts']})

            querry = '''
                INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                VALUES (%(workflow_key)s, %(workflow_settings)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET 
                    workflow_settings = %(setting)s;
            '''

            dest_cursor.execute(querry, 
            {
                'workflow_key': kwargs['workflow_key'], 
                'workflow_settings': json.dumps({'last_date': str(max([one_doc['update_ts'] for one_doc in docs]))}),
                'setting': json.dumps({'last_date': str(max([one_doc['update_ts'] for one_doc in docs]))})
            }
            )

        dest_connection.commit()

def dm_users_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        dest_cursor.execute("SELECT workflow_settings FROM dds.srv_wf_settings WHERE workflow_key='dm_users_last_ts'")

        data = dest_cursor.fetchone()

        if data:
            dest_cursor.execute(
                f"SELECT object_value FROM stg.ordersystem_users WHERE update_ts > \'{data[0]['last_ts']}\'"
            )
        else:
            dest_cursor.execute('SELECT object_value FROM stg.ordersystem_users')

        data = dest_cursor.fetchall()

        for one_data in data:
            temp = str2json(one_data[0])

            query = """
                SELECT *
                FROM dds.dm_users
                WHERE user_id = %(user_id)s
            """
            dest_cursor.execute(query, 
            {
                'user_id':temp['_id']
            }
            )

            if dest_cursor.fetchone():
                query = '''
                    UPDATE dds.dm_users
                    SET user_name=%(user_name)s, user_login=%(user_login)s
                    WHERE user_id = %(user_id)s
                '''

                dest_cursor.execute(query,
                {
                    'user_name': temp['name'],
                    'user_login': temp['login'],
                    'user_id': temp['_id'] 
                }
                )
            else:
                query = '''
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                '''

                dest_cursor.execute(query,
                {
                    'user_id': temp['_id'],
                    'user_name': temp['name'],
                    'user_login': temp['login']
                }
                )

        dest_cursor.execute('SELECT MAX(update_ts) FROM stg.ordersystem_users')
        data = dest_cursor.fetchone()

        query = """
            INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
            VALUES (%(workflow_key)s, %(workflow_settings)s)
            ON CONFLICT (workflow_key)
            DO UPDATE 
            SET workflow_settings = EXCLUDED.workflow_settings
        """
        dest_cursor.execute(query, {
            'workflow_key': 'dm_users_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(data[0])})
        })

        dest_connection.commit()

def dm_restaurants_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        dest_cursor.execute("SELECT workflow_settings FROM dds.srv_wf_settings WHERE workflow_key='dm_restaurants_last_ts'")

        data = dest_cursor.fetchone()

        if data:
            dest_cursor.execute(
                f"SELECT object_value FROM stg.ordersystem_restaurants WHERE update_ts > \'{data[0]['last_ts']}\'"
            )
        else:
            dest_cursor.execute('SELECT object_value FROM stg.ordersystem_restaurants')

        data = dest_cursor.fetchall()

        for one_data in data:
            temp = str2json(one_data[0])

            dest_cursor.execute(
                'SELECT * FROM dds.dm_restaurants WHERE restaurant_id=%s',
                (temp['_id'], )
            )

            if dest_cursor.fetchone():
                dest_cursor.execute(
                    "SELECT * FROM dds.dm_restaurants WHERE restaurant_id=%s AND restaurant_name!=%s AND active_to = '2099-12-31'", (temp['_id'], temp['name'])
                )

                if dest_cursor.fetchone():
                    query = """
                        UPDATE dds.dm_restaurants
                        SET active_to=%(update_ts)s
                        WHERE restaurant_id=%(_id)s AND active_to = '2099-12-31'
                    """
                    dest_cursor.execute(query, (temp['update_ts'], temp['_id']))

                    query = """
                        INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                        VALUES(%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    """

                    dest_cursor.execute(query, {
                        'restaurant_id': temp['_id'],
                        'restaurant_name': temp['name'],
                        'active_from': temp['update_ts'],
                        'active_to': str(datetime(year=2099, month=12, day=31))
                    })
            else:
                query = """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES(%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                """

                dest_cursor.execute(query, {
                    'restaurant_id': temp['_id'],
                    'restaurant_name': temp['name'],
                    'active_from': temp['update_ts'],
                    'active_to': str(datetime(year=2099, month=12, day=31))
                })

        dest_cursor.execute('SELECT MAX(update_ts) FROM stg.ordersystem_restaurants')
        data = dest_cursor.fetchone()

        query = """
                    INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(workflow_key)s, %(workflow_settings)s)
                    ON CONFLICT (workflow_key)
                    DO UPDATE 
                    SET workflow_settings = EXCLUDED.workflow_settings
                """
        dest_cursor.execute(query, {
            'workflow_key': 'dm_restaurants_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(data[0])})
        })

        dest_connection.commit()

def dm_timestamps_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        dest_cursor.execute("SELECT workflow_settings FROM dds.srv_wf_settings WHERE workflow_key='dm_timestamps_last_ts'")

        data = dest_cursor.fetchone()

        if data:
            dest_cursor.execute(
                f"SELECT object_value FROM stg.ordersystem_orders WHERE update_ts > \'{data[0]['last_ts']}\'"
            )
        else:
            dest_cursor.execute('SELECT object_value FROM stg.ordersystem_orders')

        data = dest_cursor.fetchall()

        for one_data in data:
            temp = str2json(one_data[0])

            if temp['final_status'] == 'CLOSED' or temp['final_status'] == 'CANCELLED':
                query = """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES(%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                """

                dest_cursor.execute(query, {
                    'ts': datetime.fromisoformat(temp['date']),
                    'year': datetime.fromisoformat(temp['date']).year,
                    'month': datetime.fromisoformat(temp['date']).month,
                    'day': datetime.fromisoformat(temp['date']).day,
                    'time': datetime.fromisoformat(temp['date']).time(),
                    'date': datetime.fromisoformat(temp['date']).date()
                })

        dest_cursor.execute('SELECT MAX(update_ts) FROM stg.ordersystem_orders')
        data = dest_cursor.fetchone()

        query = """
            INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
            VALUES (%(workflow_key)s, %(workflow_settings)s)
            ON CONFLICT (workflow_key)
            DO UPDATE 
            SET workflow_settings = EXCLUDED.workflow_settings
        """
        dest_cursor.execute(query, {
            'workflow_key': 'dm_timestamps_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(data[0])})
        })
                
        dest_connection.commit()

def dm_products_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        dest_cursor.execute("SELECT workflow_settings FROM dds.srv_wf_settings WHERE workflow_key='dm_products_last_ts'")

        data = dest_cursor.fetchone()

        if data:
            dest_cursor.execute(
                f"SELECT object_value FROM stg.ordersystem_restaurants WHERE update_ts > \'{data[0]['last_ts']}\'"
            )
        else:
            dest_cursor.execute('SELECT object_value FROM stg.ordersystem_restaurants')

        data = dest_cursor.fetchall()

        for one_data in data:
            temp = str2json(one_data[0])

            restaurant_id = temp['_id']

            query = """
                SELECT DISTINCT id
                FROM dds.dm_restaurants
                WHERE restaurant_id = %(restaurant_id)s
            """

            dest_cursor.execute(query, {
                'restaurant_id': restaurant_id
            })

            dm_restaurant_id = dest_cursor.fetchone()[0]

            menu = temp['menu']

            products_id = [one_position['_id'] for one_position in menu]

            query = """
                UPDATE dds.dm_products
                SET active_to = %(active_to)s
                WHERE restaurant_id = %(restaurant_id)s
                    AND active_to = '2099-12-31'
                    AND product_id NOT IN %(products_id)s
            """

            dest_cursor.execute(query, {
                'active_to': temp['update_ts'],
                'restaurant_id': dm_restaurant_id,
                'products_id': tuple(products_id)
            })

            for one_position in menu:
                query = """
                    SELECT *
                    FROM dds.dm_products
                    WHERE 
                    restaurant_id = %(restaurant_id)s
                    AND product_id = %(product_id)s
                """

                dest_cursor.execute(query, {
                    'restaurant_id': dm_restaurant_id,
                    'product_id': one_position['_id']
                })

                if dest_cursor.fetchone():
                    query = """
                        SELECT *
                        FROM dds.dm_products
                        WHERE restaurant_id = %(restaurant_id)s
                            AND product_id = %(product_id)s
                            AND product_name = %(product_name)s
                            AND product_price = %(product_price)s
                            AND active_to = '2099-12-31'
                    """

                    dest_cursor.execute(query, {
                        'restaurant_id': dm_restaurant_id,
                        'product_id': one_position['_id'],
                        'product_name': one_position['name'],
                        'product_price': one_position['price']
                    })

                    if not dest_cursor.fetchone():
                        query = """
                            UPDATE dds.dm_products
                            SET active_to = %(active_to)s
                            WHERE restaurant_id = %(restaurant_id)s
                                AND product_id = %(product_id)s
                                AND active_to = '2099-12-31'
                        """

                        dest_cursor.execute(query, {
                            'active_to': temp['update_ts'],
                            'restaurant_id': dm_restaurant_id,
                            'product_id': one_position['_id']
                        })

                        query = """
                            INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                            VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                        """

                        dest_cursor.execute(query, {
                            'restaurant_id': dm_restaurant_id,
                            'product_id': one_position['_id'],
                            'product_name': one_position['name'],
                            'product_price': one_position['price'],
                            'active_from': temp['update_ts'],
                            'active_to': '2099-12-31'
                        })
                else:
                    query = """
                        INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                        VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    """

                    dest_cursor.execute(query, {
                        'restaurant_id': dm_restaurant_id,
                        'product_id': one_position['_id'],
                        'product_name': one_position['name'],
                        'product_price': one_position['price'],
                        'active_from': temp['update_ts'],
                        'active_to': '2099-12-31'
                    })

        dest_cursor.execute('SELECT MAX(update_ts) FROM stg.ordersystem_restaurants')
        data = dest_cursor.fetchone()

        query = """
                    INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(workflow_key)s, %(workflow_settings)s)
                    ON CONFLICT (workflow_key)
                    DO UPDATE 
                    SET workflow_settings = EXCLUDED.workflow_settings
                """
        dest_cursor.execute(query, {
            'workflow_key': 'dm_products_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(data[0])})
        })

        dest_connection.commit()

def dm_orders_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        dest_cursor.execute(
            "SELECT workflow_settings FROM dds.srv_wf_settings WHERE workflow_key='dm_orders_last_ts'")

        data = dest_cursor.fetchone()

        if data:
            dest_cursor.execute(
                f"SELECT object_value FROM stg.ordersystem_orders WHERE update_ts > \'{data[0]['last_ts']}\'"
            )
        else:
            dest_cursor.execute('SELECT object_value FROM stg.ordersystem_orders')

        data = dest_cursor.fetchall()

        for one_data in data:
            temp = str2json(one_data[0])

            user_id = temp['user']['id']

            query = """
                SELECT DISTINCT id
                FROM dds.dm_users
                WHERE user_id = %(user_id)s
            """
            dest_cursor.execute(query, {
                'user_id': user_id
            })

            dm_user_id = dest_cursor.fetchone()[0]

            restaurant_id = temp['restaurant']['id']

            query = """
                SELECT DISTINCT id
                FROM dds.dm_restaurants
                WHERE restaurant_id = %(restaurant_id)s
            """
            dest_cursor.execute(query, {
                'restaurant_id': restaurant_id
            })

            dm_restaurant_id = dest_cursor.fetchone()[0]

            timestamp_id = temp['update_ts']

            query = """
                SELECT DISTINCT id
                FROM dds.dm_timestamps
                WHERE ts = %(timestamp_id)s
            """
            dest_cursor.execute(query, {
                'timestamp_id': timestamp_id
            })

            dm_timestamp_id = dest_cursor.fetchone()[0]

            order_key = temp['_id']

            order_status = temp['final_status']

            query = """
                INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                VALUES(%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
            """
            dest_cursor.execute(query, {
                'user_id': dm_user_id,
                'restaurant_id': dm_restaurant_id,
                'timestamp_id': dm_timestamp_id,
                'order_key': order_key,
                'order_status': order_status
            })

        dest_cursor.execute('SELECT MAX(update_ts) FROM stg.ordersystem_orders')
        data = dest_cursor.fetchone()

        query = """
            INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
            VALUES (%(workflow_key)s, %(workflow_settings)s)
            ON CONFLICT (workflow_key)
            DO UPDATE 
            SET workflow_settings = EXCLUDED.workflow_settings
        """
        dest_cursor.execute(query, {
            'workflow_key': 'dm_orders_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(data[0])})
        })

        dest_connection.commit()

def fct_product_sales_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        dest_cursor.execute(
            "SELECT workflow_settings FROM dds.srv_wf_settings WHERE workflow_key='fct_product_sales_last_ts'")

        data = dest_cursor.fetchone()

        if data:
            dest_cursor.execute(
                f"SELECT event_value FROM stg.bonussystem_events WHERE event_type = 'bonus_transaction' AND event_ts > \'{data[0]['last_ts']}\'"
            )
        else:
            dest_cursor.execute("SELECT event_value FROM stg.bonussystem_events WHERE event_type = 'bonus_transaction'")

        data = dest_cursor.fetchall()

        for one_data in data:
            temp = str2json(one_data[0])

            user_id_raw = temp['user_id']

            query = """
                SELECT order_user_id
                FROM stg.bonussystem_users
                WHERE id = %(id)s
            """

            dest_cursor.execute(query, {
                'id': user_id_raw
            })

            user_id_raw = dest_cursor.fetchone()[0]

            query = """
                SELECT id
                FROM dds.dm_users
                WHERE user_id = %(user_id)s
            """

            dest_cursor.execute(query, {
                'user_id': user_id_raw
            })

            user_id = dest_cursor.fetchone()[0]

            query = """
                SELECT id, restaurant_id
                FROM dds.dm_orders
                WHERE user_id = %(user_id)s AND order_key = %(order_key)s
            """

            dest_cursor.execute(query, {
                'user_id': user_id,
                'order_key': temp['order_id']
            })

            data = dest_cursor.fetchone()

            try:
                order_id = data[0]
                restaurant_id = data[1]
            except:
                continue

            product_payments = temp['product_payments']

            for product in product_payments:
                product_id_raw = product['product_id']

                query = """
                    SELECT id
                    FROM dds.dm_products
                    WHERE restaurant_id = %(restaurant_id)s AND product_id = %(product_id)s AND active_to = '2099-12-31'
                """

                dest_cursor.execute(query, {
                    'restaurant_id': restaurant_id,
                    'product_id': product_id_raw
                })

                try:
                    product_id = dest_cursor.fetchone()[0]
                except:
                    raise Exception(product, restaurant_id, product_id_raw)

                count = product['quantity']
                price = product['price']
                total_sum = product['product_cost']
                bonus_payment = product['bonus_payment']
                bonus_grant = product['bonus_grant']

                query = """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES(%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """

                dest_cursor.execute(query, {
                    'product_id': product_id,
                    'order_id': order_id, 
                    'count': count, 
                    'price': price, 
                    'total_sum': total_sum, 
                    'bonus_payment': bonus_payment, 
                    'bonus_grant': bonus_grant
                })           

        dest_cursor.execute('SELECT MAX(event_ts) FROM stg.bonussystem_events')
        data = dest_cursor.fetchone()

        query = """
            INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
            VALUES (%(workflow_key)s, %(workflow_settings)s)
            ON CONFLICT (workflow_key)
            DO UPDATE 
            SET workflow_settings = EXCLUDED.workflow_settings
        """
        dest_cursor.execute(query, {
            'workflow_key': 'fct_product_sales_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(data[0])})
        })

        dest_connection.commit()

def dm_settlements_report_fulfill(*args, **kwargs):
    with psycopg2.connect(dbname=dest_conn.schema, user=dest_conn.login, password=dest_conn.password, host=dest_conn.host, port=dest_conn.port) as dest_connection:
        dest_cursor = dest_connection.cursor()

        query = """
            SELECT workflow_settings
            FROM cdm.srv_wf_settings
            WHERE workflow_key = 'dm_settlement_report_last_ts'
        """

        dest_cursor.execute(query)

        ts = dest_cursor.fetchone()

        today = date.today()

        if ts:

            ts = ts[0]['last_ts']
            query = """
                INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, 
                orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                SELECT dmo.restaurant_id, dmr.restaurant_name, dmt."date", COUNT(DISTINCT dmo.id) AS orders_count, SUM(fctps.total_sum) AS orders_total_sum,
                SUM(fctps.bonus_payment) AS orders_bonus_payment_sum, SUM(fctps.bonus_grant) AS orders_bonus_granted_sum, SUM(fctps.total_sum) * 0.25 AS order_processing_fee,
                SUM(fctps.total_sum) - SUM(fctps.total_sum) * 0.25 - SUM(fctps.bonus_payment) AS restaurant_reward_sum 
                FROM dds.fct_product_sales fctps
                LEFT JOIN dds.dm_orders dmo ON fctps.order_id = dmo.id
                LEFT JOIN dds.dm_restaurants dmr ON dmo.restaurant_id = dmr.id 
                LEFT JOIN dds.dm_timestamps dmt ON dmt.id = dmo.timestamp_id
                WHERE dmo.order_status = 'CLOSED' AND dmt.ts >= %(ts)s AND dmt.ts < %(today)s
                GROUP BY dmo.restaurant_id, dmt."date", dmr.restaurant_name 
                ORDER BY dmo.restaurant_id, dmt."date"
                ON CONFLICT (restaurant_id, settlement_date)
                DO UPDATE SET 
                    restaurant_name = EXCLUDED.restaurant_name, 
                    orders_count = cdm.dm_settlement_report.orders_count + EXCLUDED.orders_count,
                    orders_total_sum = cdm.dm_settlement_report.orders_total_sum + EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = cdm.dm_settlement_report.orders_bonus_payment_sum + EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = cdm.dm_settlement_report.orders_bonus_granted_sum + EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = cdm.dm_settlement_report.order_processing_fee + EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = cdm.dm_settlement_report.restaurant_reward_sum + EXCLUDED.restaurant_reward_sum;
            """

            dest_cursor.execute(query, {
                'ts': ts,
                'today': today
            })

        else:
            query = """
                INSERT INTO cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, 
                orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                SELECT dmo.restaurant_id, dmr.restaurant_name, dmt."date", COUNT(DISTINCT dmo.id) AS orders_count, SUM(fctps.total_sum) AS orders_total_sum,
                SUM(fctps.bonus_payment) AS orders_bonus_payment_sum, SUM(fctps.bonus_grant) AS orders_bonus_granted_sum, SUM(fctps.total_sum) * 0.25 AS order_processing_fee,
                SUM(fctps.total_sum) - SUM(fctps.total_sum) * 0.25 - SUM(fctps.bonus_payment) AS restaurant_reward_sum 
                FROM dds.fct_product_sales fctps
                LEFT JOIN dds.dm_orders dmo ON fctps.order_id = dmo.id
                LEFT JOIN dds.dm_restaurants dmr ON dmo.restaurant_id = dmr.id 
                LEFT JOIN dds.dm_timestamps dmt ON dmt.id = dmo.timestamp_id
                WHERE dmo.order_status = 'CLOSED' AND dmt.ts < %(today)s
                GROUP BY dmo.restaurant_id, dmt."date", dmr.restaurant_name 
                ORDER BY dmo.restaurant_id, dmt."date"
                ON CONFLICT (restaurant_id, settlement_date, restaurant_reward_sum)
                DO UPDATE SET 
                    restaurant_name = EXCLUDED.restaurant_name, 
                    orders_count = cdm.dm_settlement_report.orders_count + EXCLUDED.orders_count,
                    orders_total_sum = cdm.dm_settlement_report.orders_total_sum + EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = cdm.dm_settlement_report.orders_bonus_payment_sum + EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = cdm.dm_settlement_report.orders_bonus_granted_sum + EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = cdm.dm_settlement_report.order_processing_fee + EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = cdm.dm_settlement_report.restaurant_reward_sum + EXCLUDED.restaurant_reward_sum;
            """

            dest_cursor.execute(query, {
                'today': today
            })

        query = """
            SELECT workflow_settings
            FROM dds.srv_wf_settings
        """

        dest_cursor.execute(query)

        data = dest_cursor.fetchall()

        ts = max(one_ts[0]['last_ts'] for one_ts in data)

        if datetime.fromisoformat(ts) > datetime(year=today.year, month=today.month, day=today.day):
            ts = datetime(year=today.year, month=today.month, day=today.day)

        query = """
            INSERT INTO cdm.srv_wf_settings(workflow_key, workflow_settings)
            VALUES(%(workflow_key)s, %(workflow_settings)s)
            ON CONFLICT (workflow_key)
            DO UPDATE SET 
                workflow_settings = %(workflow_settings_1)s
        """

        dest_cursor.execute(query, {
            'workflow_key': 'dm_settlement_report_last_ts',
            'workflow_settings': json.dumps({'last_ts': str(ts)}),
            'workflow_settings_1': json.dumps({'last_ts': str(ts)})
        })

        dest_connection.commit()

default_args = {
    'owner': 'Djammer',
    'retries':2,
    'retry_delay':timedelta(seconds=10),
}

dag = DAG(
    dag_id='dag_3_5_4',
    start_date=datetime(2024, 7, 24), 
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    default_args=default_args)

tg1 = TaskGroup(group_id='download_from_postgres', dag=dag)

ranks_download = PythonOperator(
    task_id='rank_load',
    python_callable=download_from_postgres_task,
    task_group=tg1,
    retries=2,
    dag=dag,
    op_kwargs={
        'bd_name_dst': 'bonussystem_ranks',
        'bd_name_src': 'ranks'
        },
)

users_download = PythonOperator(
    task_id='users_load',
    python_callable=download_from_postgres_task,
    task_group=tg1,
    retries=2,
    dag=dag,
    op_kwargs={
        'bd_name_dst': 'bonussystem_users',
        'bd_name_src': 'users'
        },
)

outbox_downloar = PythonOperator(
    task_id='outbox_load',
    python_callable=download_from_postgres_incremental,
    task_group=tg1,
    retries=2,
    dag=dag,
)

tg2 = TaskGroup(group_id='download_from_mongoDB', dag=dag)

restaurant_download = PythonOperator(
    task_id='restaurant_load',
    python_callable=download_from_mongo_incremental,
    task_group=tg2,
    retries=2,
    dag=dag,
    op_kwargs={
        'bd_name_dst': 'ordersystem_restaurants',
        'bd_name_src': 'restaurants',
        'workflow_key': 'restaurants_last_date'
        },
)

order_download = PythonOperator(
    task_id='order_load',
    python_callable=download_from_mongo_incremental,
    task_group=tg2,
    retries=2,
    dag=dag,
    op_kwargs={
        'bd_name_dst': 'ordersystem_orders',
        'bd_name_src': 'orders',
        'workflow_key': 'orders_last_date'
        },
)

restaurant_download = PythonOperator(
    task_id='users_load',
    python_callable=download_from_mongo_incremental,
    task_group=tg2,
    dag=dag,
    op_kwargs={
        'bd_name_dst': 'ordersystem_users',
        'bd_name_src': 'users',
        'workflow_key': 'users_last_date'
        },
)

tg3 = TaskGroup(group_id = 'dds_fulfill_part_1', dag=dag)

users_dm_download = PythonOperator(
    task_id='dm_users_fulfill',
    python_callable=dm_users_fulfill,
    task_group=tg3,
    dag=dag,
)

restaurants_dm_download = PythonOperator(
    task_id='dm_restaurants_fulfill',
    python_callable=dm_restaurants_fulfill,
    task_group=tg3,
    dag=dag,
)

timestamps_dm_download = PythonOperator(
    task_id='dm_timestamps_fulfill',
    python_callable=dm_timestamps_fulfill,
    task_group=tg3,
    dag=dag,
)

tg4 = TaskGroup(group_id = 'dds_fulfill_part_2', dag=dag)

products_dm_download = PythonOperator(
    task_id='dm_products_fulfill',
    python_callable=dm_products_fulfill,
    task_group=tg4,
    dag=dag,
)

orders_dm_download = PythonOperator(
    task_id='dm_orders_fulfill',
    python_callable=dm_orders_fulfill,
    task_group=tg4,
    dag=dag,
)

product_sales_fct_download = PythonOperator(
    task_id='fct_product_sales_fulfill',
    python_callable=fct_product_sales_fulfill,
    dag=dag,
)

settlement_report_dm_download = PythonOperator(
    task_id='dm_settlement_report_fulfill',
    python_callable=dm_settlements_report_fulfill,
    dag=dag
)

[tg1, tg2] >> tg3 >> tg4 >> product_sales_fct_download >> settlement_report_dm_download
