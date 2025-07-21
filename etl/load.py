import psycopg2


class Loader:
    '''
    To write structured data into the database
    '''

    def __init__(self, **db_config):
        '''
        Establish a connection and a cursor to the database

        Parameter
        db_config (dict) : database credentials

        Returns
        None
        '''
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()


    def close(self):
        '''
        Terminate connections to the database

        Returns
        None
        '''
        self.conn.close()
        self.cur.close()
        

    def create_table(self, format):
        '''
        Create a table in database using the format

        Parameter
        format (dict) : names of the columns mapped to their datatypes as strings

        Returns
        None
        '''
        pass
