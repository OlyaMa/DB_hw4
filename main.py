import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute('''
        DROP TABLE PhoneNumber;
        DROP TABLE Client
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Client(
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(40),
            last_name VARCHAR(60),
            email VARCHAR(255) UNIQUE
        );      
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS PhoneNumber(
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES Client(client_id),
            number INTEGER
        );
        ''')
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO Client(first_name, last_name, email) VALUES (%s, %s, %s);
        ''', (first_name, last_name, email))
        conn.commit()
        cur.execute('''
        SELECT client_id FROM Client WHERE email=%s
        ''', (email,))
        cl_id = cur.fetchone()[0]
        if phones:
            cur.execute('''
            INSERT INTO PhoneNumber(client_id, number) VALUES (%s, %s);
            ''', (cl_id, phones))
            conn.commit()
        cur.execute("""
        SELECT * FROM Client;
        """)
        print(cur.fetchall())
        cur.execute("""
        SELECT * FROM PhoneNumber;
        """)
        print(cur.fetchall())


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO PhoneNumber(client_id, number) VALUES (%s, %s);
        ''', (client_id, phone))
        conn.commit()
        cur.execute("""
        SELECT * FROM Client;
        """)
        print(cur.fetchall())
        cur.execute("""
        SELECT * FROM PhoneNumber;
        """)
        print(cur.fetchall())


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute('''
            UPDATE Client SET first_name=%s WHERE client_id=%s;
            ''', (first_name, client_id))
        if last_name:
            cur.execute('''
            UPDATE Client SET last_name=%s WHERE client_id=%s;
            ''', (last_name, client_id))
        if email:
            cur.execute('''
            UPDATE Client SET email=%s WHERE client_id=%s;
            ''', (email, client_id))
        if phones:
            cur.execute('''
            INSERT INTO PhoneNumber(client_id, number) VALUES (%s);
            ''', (client_id, phones))
        conn.commit()
        cur.execute("""
        SELECT * FROM Client;
        """)
        print(cur.fetchall())
        cur.execute("""
        SELECT * FROM PhoneNumber;
        """)
        print(cur.fetchall())


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM PhoneNumber WHERE client_id=%s AND number=%s;
        ''', (client_id, phone))
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM PhoneNumber WHERE client_id=%s;
        ''', (client_id,))
        cur.execute('''
        DELETE FROM Client WHERE client_id=%s;
        ''', (client_id,))
        conn.commit()
        cur.execute("""
        SELECT * FROM Client;
        """)
        print(cur.fetchall())
        cur.execute("""
        SELECT * FROM PhoneNumber;
        """)
        print(cur.fetchall())


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        email = email
        number = phone
        first_name = first_name
        last_name = last_name
        where = ''
        if email:
            where += f"AND email = '{email}' "
        if phone:
            where += f"AND number = {phone} "
        if first_name:
            where += f"AND first_name = '{first_name}' "
        if last_name:
            where += f"AND last_name = '{last_name}' "
        where = where[3:]
        if email or phone or last_name or first_name:
            where = 'WHERE' + where
        request_to_exec = f"""
        SELECT c.client_id, c.first_name, c.last_name, c.email, p.number FROM Client c
        FULL JOIN PhoneNumber p ON p.client_id=c.client_id
        {where}
        """
        cur.execute(request_to_exec)
        print(cur.fetchall())


with psycopg2.connect(database="customer_db", user="postgres", password="postgres") as conn:
    create_db(conn)
    add_client(conn, 'Ivan', 'Ivanov', 'iiv@ya.ru', phones=123456789)
    add_client(conn, 'Anna', 'Petrova', 'ap@mail.ru', phones=987654321)
    add_client(conn, 'Petr', 'Sokolov', 'ps@ya.ru')
    add_client(conn, 'Mary', 'Sidorova', 'ms@mail.ru', phones=369258147)
    add_phone(conn, 1, 321654987)
    change_client(conn, 2, last_name='Smirnova')
    change_client(conn, 1, email='iiv@mail.ru')
    delete_phone(conn, 1, 123456789)
    delete_client(conn, 4)
    find_client(conn, email='ps@ya.ru')
    find_client(conn, first_name='Ivan', last_name='Ivanov')
    find_client(conn, phone=987654321)

conn.close()
