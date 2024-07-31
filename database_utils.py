# database_utils.py
import yaml
from sqlalchemy import create_engine, text

class DatabaseConnector:
    def __init__(self, creds_path):
        """
        Initializes the DatabaseConnector with the provided credentials path.

        Args:
            creds_path (str): The file path to the database credentials YAML file.
        """
        
        self.creds_path = creds_path
        self.credentials = self.read_db_creds(self.creds_path)

    def read_db_creds(self,creds_path):
        """
        Reads the database credentials from a YAML file.

        Args:
            creds_path (str): The file path to the database credentials YAML file.

        Returns:
            dict: A dictionary containing the database credentials.
        """

        with open(creds_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds    
    
    def init_db_engine(self):
        """
        Initializes the database engine using the credentials.

        Returns:
            sqlalchemy.engine.Engine: The SQLAlchemy engine object for connecting to the database.
        """

        creds = self.credentials
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        print(engine)
        print('Database connected')
        return engine
    
    def list_db_tables(self):
        """
        Lists all table names in the connected database.

        Returns:
            list: A list of table names available in the database.
        """

        engine = self.init_db_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
            tables = result.fetchall()
            table_list = [table[0] for table in tables]
            return table_list
        
    def upload_to_db(self, df, table_name):
        """
        Uploads a DataFrame to the specified table in the database.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the target table in the database.

        Returns:
            None
        """

        engine = self.init_db_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)    

            


#db_connector = DatabaseConnector(creds_path='db_creds.yaml')

#table_names = db_connector.list_db_tables()
#print(table_names)