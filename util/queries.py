from util.databridge import Databridge
from pyspark.sql.functions import column, month, dayofmonth, dayofweek

class CustomQuery():
    def query(databridge : Databridge, modifier : str) -> str: ...

class SampleQuery(CustomQuery):
    def ranges(self, df):
        return [
            df['TMed'].between(0,9.99).alias('T 0-999'),
            df['TMed'].between(10,14.99).alias('T 10-1499'),
            df['TMed'].between(15,19.99).alias('T 15-1999'),
            df['TMed'].between(20,24.99).alias('T 20-2499'),
            df['TMed'].between(25,29.99).alias('T 25-2999'),
            df['TMed'].between(30,34.99).alias('T 30-3499') 
        ]

    def query(self, databridge: Databridge, modifier : str) -> str:
        return modifier if databridge is not None else None