
import yaml
from sqlalchemy import create_engine

with open('db_creds.yaml', 'r') as file:
        creds = yaml.safe_load(file)   

#engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
engine = create_engine("postgresql+psycopg2://aicore_admin:AiCore2022@data-handling-project-readonly.cq2e8n0558s.eu-west-1.rds.amazonaws.com:5432/postgres")
print(engine)
print('Database connected')