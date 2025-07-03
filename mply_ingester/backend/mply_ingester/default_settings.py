# Db values correspond to the values in docker-compose
DB_USER = 'ingester'
DB_NAME = 'mply_ingester'
DB_PORT = 5490
DB_PASSWORD = '123'
DB_HOST = 'localhost'

DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
