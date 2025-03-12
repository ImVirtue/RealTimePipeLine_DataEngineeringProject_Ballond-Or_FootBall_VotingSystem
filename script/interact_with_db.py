import psycopg2

def create_connection():
    conn = psycopg2.connect(
        host='localhost',
        dbname='voting',
        user='postgres',
        password='postgres'
    )

    return conn

def create_table(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            candidate_team VARCHAR(255),
            dob DATE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,  
            voter_user VARCHAR(255) NOT NULL UNIQUE, 
            gender VARCHAR(10),  
            address TEXT,  
            voter_name VARCHAR(255) NOT NULL, 
            voter_email VARCHAR(255) UNIQUE,  
            voter_dob DATE  
        );
    """)

    cur.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                voter_id VARCHAR(255) UNIQUE,
                candidate_id VARCHAR(255),
                voting_time TIMESTAMP,
                vote int DEFAULT 1,
                PRIMARY KEY (voter_id, candidate_id)
            )
        """)

    conn.commit()

def insert_into_vote(conn, data):
    cur = conn.cursor()

    insert_query = """
        INSERT INTO votes (voter_id, candidate_id, voting_time, vote)
        VALUES (%s, %s, %s, %s)
    """

    try:
        cur.execute(insert_query, data)
        print('Insert into vote successfully')
        conn.commit()
    except Exception as e:
        print('An error occurred: ', e)

def insert_into_voter(conn, data):
    cur = conn.cursor()

    insert_query = """
            INSERT INTO voters (voter_id, voter_user, gender, address, voter_name, voter_email, voter_dob)
            VALUES (%s,%s, %s, %s, %s, %s, %s)
        """

    try:
        cur.execute(insert_query, data)
        print('Insert into voter successfully')
        conn.commit()
    except Exception as e:
        print('An error occurred: ', e)

def insert_into_candidate(conn):
    cur = conn.cursor()

    insert_query = """
        INSERT INTO candidates (candidate_id, candidate_name, candidate_team, dob)
        VALUES (%s, %s, %s, %s);
    """

    candidates_data = [
        ('CR7', 'Cristiano Ronaldo', 'Real Madrid FC', '1985-02-05'),
        ('M10', 'Leo Messi', 'Barcelona FC', '1987-06-24'),
        ('N9', 'Neymar JR', 'Barcelona FC', '1992-02-05'),
        ('AI8', 'Andres Iniesta', 'Barcelona FC', '1984-05-11')
    ]

    # Use executemany() instead of execute() for multiple records
    cur.executemany(insert_query, candidates_data)

    conn.commit()
    cur.close()

def check_candidates_data(conn):
    cur = conn.cursor()

    cur.execute("SELECT * FROM candidates")

    rows = cur.fetchall()

    if len(rows) != 0:
        return True

    return False

def delete_tables(conn):
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE voters, votes, candidates")
        print("Drop tables successfully!")
        conn.commit()

    except Exception as e:
        print(e)

if __name__ == '__main__':
    pass

