from databridge import Databridge
from pyspark.sql.functions import column, month, dayofmonth, dayofweek

class CustomQuery():
    def query(databridge : Databridge, modifier : str) -> str: ...

class temp_ranges(CustomQuery):
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
        if modifier == 'day_count':
            spark_df = databridge.get_dataframe('weather') 
            return spark_df.groupBy(*self.ranges(spark_df))\
                           .count()\
                           .toPandas().to_csv()
        if modifier == 'expenditure':
            spark_df = databridge.get_dataframe('cards_weather') 
            return spark_df.groupBy('SECTOR', *self.ranges(spark_df))\
                           .sum('IMPORTE')\
                           .orderBy('SECTOR', 'T 0-999', 'T 10-1499', 'T 15-1999', 'T 20-2499','T 25-2999', 'T 30-3499')\
                           .toPandas().to_csv()
        return None

class expense(CustomQuery):
    def query(databridge: Databridge, modifier: str, sector_filter: str) -> str:
        spark_df = databridge.get_dataframe('cards_weather')
        time_range = {
            'by_day':       lambda: column    ('FECHA' ),
            'by_month_day': lambda: dayofmonth('FECHA' ),
            'by_month':     lambda: month     ('FECHA' ),
            'by_sector':    lambda: column    ('SECTOR')
        }
        content_df = spark_df.groupBy(time_range[modifier]().alias('DATE')).sum('IMPORTE').orderBy('DATE')
        result_df  = content_df if not sector_filter else content_df.where('SECTOR == ' + sector_filter)
        return result_df.toPandas().to_csv()

class demand(CustomQuery):
    def tremester(self, tremester):
        tremesters = [
            '',
            'month between  1 and 3',
            'month between  4 and 6',
            'month between  7 and 9',
            'month between 10 and 12'
        ]
        return tremesters[int(tremester)]

    def query(self, databridge: Databridge, modifier: str) -> str:
        spark_df = databridge.get_dataframe('cards_weather')
        return spark_df.groupBy(month('FECHA').alias('month')).sum('IMPORTE').where(self.tremester(modifier)).orderBy('month').toPandas().to_csv()

class sector_spec(CustomQuery):
    def query(databridge: Databridge, modifier: str) -> str:
        spark_df = databridge.get_dataframe('cards_weather')
        if modifier == 'beauty_week':
            return spark_df.groupBy(dayofweek('FECHA'), 'SECTOR', 'Precip').sum('IMPORTE') \
                    .where('SECTOR == "BELLEZA" AND (dayofweek(FECHA) == 6 or dayofweek(FECHA) == 7 or dayofweek(FECHA) == 1) AND Precip > 0') \
                    .orderBy('dayofweek(FECHA)').toPandas().to_csv()
        if modifier == 'food_week':
            return spark_df.groupBy(dayofweek('FECHA'), 'SECTOR').sum('IMPORTE') \
                    .where('SECTOR == "ALIMENTACION"').orderBy('dayofweek(FECHA)').toPandas().to_csv()
        return None