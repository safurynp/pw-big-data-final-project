import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, col, to_date, expr

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Build DataFrames from the raw data
dyf = glueContext.create_dynamic_frame.from_catalog(database='raw-data-crawler-database', table_name='atp_players')
atp_players_df = dyf.toDF()

dyf = glueContext.create_dynamic_frame.from_catalog(database='raw-data-crawler-database', table_name='wta_players')
wta_players_df = dyf.toDF()

dyf = glueContext.create_dynamic_frame.from_catalog(database='raw-data-crawler-database', table_name='atp_rankings')
atp_rankings_df = dyf.toDF()

dyf = glueContext.create_dynamic_frame.from_catalog(database='raw-data-crawler-database', table_name='wta_rankings')
wta_rankings_df = dyf.toDF()

dyf = glueContext.create_dynamic_frame.from_catalog(database='raw-data-crawler-database', table_name='atp_matches')
atp_matches_df = dyf.toDF()

dyf = glueContext.create_dynamic_frame.from_catalog(database='raw-data-crawler-database', table_name='wta_matches')
# Resovle the 'draw_size' type conflict
dyf = dyf.resolveChoice(specs = [('draw_size', 'cast:long')])
wta_matches_df = dyf.toDF()


# Resolve duplicate player IDs between ATP and WTA data
# Get the maximum player_id from atp_players_df
max_atp_player_id = atp_players_df.agg({"player_id": "max"}).collect()[0][0]

# Create a window function to generate new IDs starting from max_atp_player_id + 1
w = Window.orderBy(F.monotonically_increasing_id())

# Add a new column 'new_player_id' starting from (max_atp_player_id + 1) and rename/drop columns
wta_players_df = wta_players_df.withColumn('new_player_id', F.row_number().over(w) + max_atp_player_id) \
                               .withColumnRenamed('player_id', 'old_player_id') \
                               .withColumnRenamed('new_player_id', 'player_id')

# DataFrame wta_rankings: Change 'player' column to 'player_id'
wta_rankings_df = wta_rankings_df.join(wta_players_df, on=wta_rankings_df.player == wta_players_df.old_player_id, how='left') \
                                 .select(wta_rankings_df['*'], wta_players_df['player_id']) \
                                 .drop('player')

# DataFrame wta_matches: Change 'winner_id' and 'loser_id' columns to follow the new 'player_id'
wta_matches_df = wta_matches_df.join(wta_players_df, on=wta_matches_df.winner_id == wta_players_df.old_player_id, how='left') \
                               .select(wta_matches_df['*'], wta_players_df['player_id']) \
                               .drop('winner_id') \
                               .withColumnRenamed('player_id', 'winner_id')
wta_matches_df = wta_matches_df.join(wta_players_df, on=wta_matches_df.loser_id == wta_players_df.old_player_id, how='left') \
                               .select(wta_matches_df['*'], wta_players_df['player_id']) \
                               .drop('loser_id') \
                               .withColumnRenamed('player_id', 'loser_id')


# Join ATP and WTA data
# Merge ATP and WTA players data (drop 'old_player_id' from WTA data first)
wta_players_df = wta_players_df.drop('old_player_id')
atp_players_df = atp_players_df.withColumn('tour', lit('ATP'))
wta_players_df = wta_players_df.withColumn('tour', lit('WTA'))
players_df = atp_players_df.union(wta_players_df)

# Merge ATP and WTA rankings data (rename 'player' column to 'player_id' and add 'tours' column to ATP data)
atp_rankings_df = atp_rankings_df.withColumnRenamed('player', 'player_id')
atp_rankings_df = atp_rankings_df.withColumn('tours', lit(None).cast('int'))
atp_rankings_df = atp_rankings_df.withColumn('tour', lit('ATP'))
wta_rankings_df = wta_rankings_df.withColumn('tour', lit('WTA'))
rankings_df = atp_rankings_df.union(wta_rankings_df)

# Merge ATP and WTA matches data
atp_matches_df = atp_matches_df.withColumn('tour', lit('ATP'))
wta_matches_df = wta_matches_df.withColumn('tour', lit('WTA'))
matches_df = atp_matches_df.union(wta_matches_df)

# Convert date columns to date type
players_df = players_df.withColumn('dob', to_date(col('dob'), 'yyyyMMdd'))
rankings_df = rankings_df.withColumn('ranking_date', to_date(col('ranking_date'), 'yyyyMMdd'))
matches_df = matches_df.withColumn('tourney_date', to_date(col('tourney_date'), 'yyyyMMdd'))

# Create new columns - winner games and loser games (from 'score')
# Split the 'score' string by empty space and save as an array
matches_df = matches_df.withColumn('split_score', F.split('score', ' '))

# Extract the first character of each element in the array, cast to int, and sum
matches_df = matches_df.withColumn('winner_games', F.expr("aggregate(split_score, 0, (acc, x) -> acc + cast(substr(x, 1, 1) as int))"))

# Extract the last character of each element in the array, cast to int, and sum
matches_df = matches_df.withColumn('loser_games', F.expr("aggregate(split_score, 0, (acc, x) -> acc + cast(substr(x, -1, 1) as int))"))

# Drop the 'split_score' column
matches_df = matches_df.drop('split_score')


# Write the DataFrames to S3 in a parquet format
matches_df.write.mode('overwrite').format("parquet").save("s3://processed-tennis-stats-data/matches")
players_df.write.mode('overwrite').format("parquet").save("s3://processed-tennis-stats-data/players")
rankings_df.write.mode('overwrite').format("parquet").save("s3://processed-tennis-stats-data/rankings")
job.commit()