import pika
import psycopg2
import time

connection_rmq = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel_rmq = connection_rmq.channel()

host = "127.0.0.1"
user = "postgres"
password = "0404"
db_name = "postgres"
port = "5432"

connection_psql = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    dbname=db_name,
    port=port
)


def callback(ch, method, properties, body):
    if body != "ne":
        send_message_psql(body)
        print(send_message_psql(body))


def get_body_rmq():
    time.sleep(4)
    path = channel_rmq.basic_get(queue='from_people_extraction_to_face_extraction')
    str_path = str(path[2])
    str_pathe = str_path[2:]
    return str_pathe


def send_message_psql(message):
    if message != "ne":
        time.sleep(4)
        connection_psql.autocommit = True
        try:
            cursor = connection_psql.cursor()
            with connection_psql.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO all_people (path) VALUES (%s) 
                    """, (message.decode(),)
                )
                print("Send is good")

        except Exception as ex:
            print("Error sending", ex)


channel_rmq.basic_consume(queue='from_people_extraction_to_face_extraction', on_message_callback=callback, auto_ack=True)

print('Ожидание сообщений...')
channel_rmq.start_consuming()
