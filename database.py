import mysql.connector 
from mysql.connector import errorcode

class Database:

    def __init__(self, log):
        self.logger = log

    def create_connection(self):
        try:
            self.conn = mysql.connector.connect(
                # database connection details 
            )  
            self.curr = self.conn.cursor()
            self.logger.info(f'Database is connected successfully ')

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
    
    def store_db(self, item):
        insert_query = '''
        INSERT INTO bloomberg (url, article_title, article_content, Timestamp)
        VALUES (%s, %s, %s, %s)
        '''
        try:
            self.curr.execute(insert_query, (
                item[0],
                item[1],
                item[2],
                item[3]
            ))
            self.conn.commit()
            print("Data inserted successfully.")
            self.logger.info('Data insereted successfully into the table')
            return True

        except Exception as e:
            print('Insert data into table ERROR:', e)
            self.logger.error(f'Insert data into table ERROR: {e}')
            return False

    def fetch_url_ids(self):
        try:
           
            # Execute the query to retrieve url_id
            self.curr.execute("SELECT url_id FROM Config")

            # Fetch all results
            url_ids = self.curr.fetchall()

            # Return the url_ids as a list
            return [url_id[0] for url_id in url_ids]

        except Exception as e:
            print(f"SQL error: {e}")
            return None
    
    def fetch_record_by_url_id(self, url_id):
        query = '''
        SELECT * FROM Config
        WHERE url_id = %s
        '''
        try:
            self.curr.execute(query, (url_id,))
            record = self.curr.fetchone()
            if record:
                print('config details : ', record)
            else:
                print("No record found for seed_url:", url_id)
            return record
        except Exception as e:
            print('Fetch record by url_id ERROR:', e)
            self.logger.error(f'Fetch record by url_id ERROR: e')
            return None


    def close_database(self):
        self.curr.close()
        self.conn.close()
        self.logger.info('Database connection closed')

    
