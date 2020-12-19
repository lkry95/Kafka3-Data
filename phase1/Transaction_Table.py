import sqlalchemy
import os
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
sql_pw = os.environ.get("MySqlPassword")


class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)


if __name__ == "__main__":
    engine = sqlalchemy.create_engine(f'mysql+pymysql://root:{sql_pw}@localhost:3306/zipbank')
    Transaction.__table__.create(engine)