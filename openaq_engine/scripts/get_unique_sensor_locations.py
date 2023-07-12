import datetime
import pandas as pd
from src.utils.utils import write_to_db, get_data
from setup_environment import get_dbengine
import time

engine = get_dbengine()

df = get_data(engine)
