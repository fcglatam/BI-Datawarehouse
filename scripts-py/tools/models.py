from sqlalchemy import Column, String, Integer, Date
from tools.base import Base


class Cars(Base):
    __tablename__ = 'cars'

    id              = Column(Integer, primary_key=True)
    title           = Column(String)
    release_dates   = Column(Date)

    def __init__(self, title, release_date):
        self.title = title
        self.release_date = release_date
        
        
