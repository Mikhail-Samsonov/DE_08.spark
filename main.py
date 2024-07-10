from pyspark.sql import SparkSession
import argparse
import os

def init_spark():
  spark = SparkSession.builder\
    .appName("crime-app")\
    .getOrCreate()
  sc = spark.sparkContext
  sc.setLogLevel("WARN")
  return spark,sc

def main(path_to_data, path_to_result):
  file_crime = os.path.join(path_to_data, "crime.csv")
  file_offense = os.path.join(path_to_data, "offense_codes.csv")
  spark,sc = init_spark()

  df_crime = spark.read.load(file_crime,format = "csv", inferSchema="true", sep=",", header="true")
  df_crime.createTempView("crime")

  df_offense = spark.read.load(file_offense,format = "csv", inferSchema="true", sep=",", header="true")
  df_offense.createTempView("offense")

  query = """
    with base as (
      select
        c.incident_number as crime_id
        , c.district
        , o.crime_type
        , c.period_id
        , c.lat
        , c.long
      from (
          select distinct
            incident_number
            , district
            , cast(offense_code as int) as offense_code
            , cast(year as int) * 100 + cast(month as int) as period_id
            , cast(lat as float) as lat
            , cast(long as float) as long
          from crime
        ) as c
        left join (
          select
            code
            , trim(split(name, '-')[0]) as crime_type
            , row_number() over(partition by code order by name) as rn
          from offense
        ) as o
          on c.offense_code = o.code
            and o.rn = 1
      where 1=1
        and c.incident_number is not null
        and c.district is not null
        and c.offense_code is not null
        and c.period_id is not null
        and c.lat is not null
        and c.long is not null
        and abs(42.361145 - c.lat) < 1.0
        and abs(-71.057083 - c.long) < 1.0
    )

    , district as (
      select distinct district
      from base
    )

    , crimes_total as (
      select
        district
        , count(distinct crime_id) as crimes_total
      from base
      group by
        district
    )

    , crimes_monthly as (
      select
        district
        , percentile(crime_count, 0.5) as crimes_monthly
      from (
        select
          district
          , cast(count(distinct crime_id) as bigint) as crime_count
        from base
        group by
          district
          , period_id
      ) t
      group by
        district
    )

    , frequent_crime_types as (
      select
        district
        , concat_ws(', ', collect_set(crime_type)) as frequent_crime_types
      from (
        select
          district
          , crime_type
        from (
          select
            district
            , crime_type
            , rn
          from (
            select
              district
              , crime_type
              , row_number() over(partition by district order by crime_count desc) as rn
            from (
              select
                district
                , crime_type
                , count(distinct crime_id) as crime_count
              from base
              group by
                district
                , crime_type
            ) t
          ) t
          where rn < 4
        ) t
        distribute by
          district
        sort by
          district
          , rn
      ) t
      group by
        district
    )

    , lat as (
      select
        district
        , round(sum(lat*crime_count)/sum(crime_count), 2) as lat
      from (
        select
          district
          , lat
          , count(distinct crime_id) as crime_count
        from base
        group by
          district
          , lat
      ) t
      group by
        district
    )

    , long as (
      select
        district
        , round(sum(long*crime_count)/sum(crime_count), 2) as long
      from (
        select
          district
          , long
          , count(distinct crime_id) as crime_count
        from base
        group by
          district
          , long
      ) t
      group by
        district
    )

    select
      d.district
      , ct.crimes_total
      , cm.crimes_monthly
      , fct.frequent_crime_types
      , lt.lat
      , lg.long
    from district d
      left join crimes_total ct
        on d.district = ct.district
      left join crimes_monthly cm
        on d.district = cm.district
      left join frequent_crime_types fct
        on d.district = fct.district
      left join lat lt
        on d.district = lt.district
      left join long lg
        on d.district = lg.district
  """

  spark.sql(query).write.parquet(path_to_result, mode="overwrite")
  
parser = argparse.ArgumentParser()
parser.add_argument("--path_to_data", help="path to data")
parser.add_argument("--path_to_result", help="path to result")
args = parser.parse_args()
if args.path_to_data:
  path_to_data = args.path_to_data
if args.path_to_result:
  path_to_result = args.path_to_result

if __name__ == '__main__':
  main(path_to_data, path_to_result)
