from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os 
import boto3

load_dotenv()

db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_port = os.getenv("POSTGRES_PORT")
db_host = os.getenv("POSTGRES_HOST")
db_name = os.getenv("POSTGRES_DB")

def get_engine():
    url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    return create_engine(url)

# def test_s3():
#     s3 = boto3.client(
#         "s3",
#         region_name=os.getenv("AWS_REGION")
#     )

#     response = s3.list_buckets()


#     buckets =[b["Name"] for b in response["Buckets"]]
#     print(f"S3 Connection ok - Buckets: {buckets}")

if __name__ == "__main__":
    # test_s3()
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print(result.fetchone())