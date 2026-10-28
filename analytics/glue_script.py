import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["config"])

config = json.loads(args["config"])

table_name = config['table']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


def transactions(name):
    """Read a table from the transactions layer."""
    return spark.table(f"iceberg.transactions.{name}")


def paid_share(total_plays, paid_play_count):
    """Share of plays made on a paid level, or 0.0 when there are no plays."""
    return (
        F.when(F.coalesce(total_plays, F.lit(0)) == 0, F.lit(0.0))
        .otherwise(paid_play_count.cast("double") / total_plays)
    )


def build_artist_facts():
    """One row per artist, including artists with no plays."""
    events = transactions("events")
    artists = transactions("artists")

    # Play statistics for each artist found in the events
    play_aggs = (
        events
        # Ignore events that have no artist
        .where(F.col("artist_id").isNotNull())
        # One group per artist
        .groupBy("artist_id")
        .agg(
            # Number of play events
            F.count("*").alias("total_plays"),
            # Number of different users who played the artist
            F.countDistinct("user_id").alias("distinct_listeners"),
            # Number of different songs played
            F.countDistinct("song_id").alias("distinct_songs_played"),
            # Number of different sessions the artist appeared in
            F.countDistinct("session_id").alias("distinct_sessions"),
            # Earliest play time
            F.min("ts").alias("first_played_at"),
            # Latest play time
            F.max("ts").alias("last_played_at"),
            # Number of plays made on the paid level (1 per paid play, 0 otherwise)
            F.sum(F.when(F.col("level") == "paid", 1).otherwise(0)).alias("paid_play_count"),
        )
    )

    return (
        # Start from every artist so those with no plays are kept
        artists
        .join(play_aggs, on="artist_id", how="left")
        .select(
            "artist_id",
            "artist_name",
            # Artists with no plays get 0 instead of null
            F.coalesce(F.col("total_plays"), F.lit(0)).alias("total_plays"),
            F.coalesce(F.col("distinct_listeners"), F.lit(0)).alias("distinct_listeners"),
            F.coalesce(F.col("distinct_songs_played"), F.lit(0)).alias("distinct_songs_played"),
            F.coalesce(F.col("distinct_sessions"), F.lit(0)).alias("distinct_sessions"),
            "first_played_at",
            "last_played_at",
            # Share of plays that were on the paid level
            paid_share(F.col("total_plays"), F.col("paid_play_count")).alias("paid_play_share"),
        )
    )


def build_song_facts():
    """One row per song across all its versions, including songs with no plays."""
    events = transactions("events")
    songs = transactions("songs")

    # Play statistics for each song found in the events
    play_aggs = (
        events
        # Ignore events that have no song
        .where(F.col("song_id").isNotNull())
        # One group per song
        .groupBy("song_id")
        .agg(
            # Number of play events
            F.count("*").alias("total_plays"),
            # Number of different users who played the song
            F.countDistinct("user_id").alias("distinct_listeners"),
            # Number of different sessions the song appeared in
            F.countDistinct("session_id").alias("distinct_sessions"),
            # Number of different versions of the song that were played
            F.countDistinct("version_id").alias("distinct_versions_played"),
            # Earliest play time
            F.min("ts").alias("first_played_at"),
            # Latest play time
            F.max("ts").alias("last_played_at"),
            # Number of plays made on the paid level (1 per paid play, 0 otherwise)
            F.sum(F.when(F.col("level") == "paid", 1).otherwise(0)).alias("paid_play_count"),
        )
    )

    return (
        # Start from every song so those with no plays are kept
        songs
        .join(play_aggs, on="song_id", how="left")
        .select(
            "song_id",
            "title",
            "artist_id",
            # Songs with no plays get 0 instead of null
            F.coalesce(F.col("total_plays"), F.lit(0)).alias("total_plays"),
            F.coalesce(F.col("distinct_listeners"), F.lit(0)).alias("distinct_listeners"),
            F.coalesce(F.col("distinct_sessions"), F.lit(0)).alias("distinct_sessions"),
            F.coalesce(F.col("distinct_versions_played"), F.lit(0)).alias("distinct_versions_played"),
            "first_played_at",
            "last_played_at",
            # Share of plays that were on the paid level
            paid_share(F.col("total_plays"), F.col("paid_play_count")).alias("paid_play_share"),
        )
    )


def build_session_facts():
    """One row per session, with the level and location of its first event."""
    events = transactions("events")
    # Only the length of each song version is needed from this table
    durations = transactions("song_versions").select("version_id", "duration")

    # Totals for each session
    session_aggs = (
        events
        # Attach the song length to each event, keeping events with no match
        .join(durations, on="version_id", how="left")
        # One group per session and user
        .groupBy("session_id", "user_id")
        .agg(
            # Time of the first event in the session
            F.min("ts").alias("started_at"),
            # Time of the last event in the session
            F.max("ts").alias("ended_at"),
            # Number of plays in the session
            F.count("*").alias("play_count"),
            # Number of different songs played
            F.countDistinct("song_id").alias("distinct_songs"),
            # Number of different artists played
            F.countDistinct("artist_id").alias("distinct_artists"),
            # Total song length played, 0 when no lengths are known
            F.coalesce(F.sum("duration"), F.lit(0)).alias("total_listening_seconds"),
        )
    )

    # Number the events inside each session from the earliest to the latest
    first_event_first = Window.partitionBy("session_id").orderBy(F.col("ts").asc())
    session_context = (
        events
        .withColumn("rn", F.row_number().over(first_event_first))
        # Keep only the first event of each session
        .where(F.col("rn") == 1)
        .select("session_id", "level", "location")
    )

    return (
        # Add the level and location of the first event to each session
        session_aggs
        .join(session_context, on="session_id", how="left")
        .select(
            "session_id",
            "user_id",
            "started_at",
            "ended_at",
            # Session length in seconds
            (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")).alias("duration_seconds"),
            "play_count",
            "distinct_songs",
            "distinct_artists",
            "total_listening_seconds",
            "level",
            "location",
        )
    )


def build_user_facts():
    """One row per user, with listening totals and current subscription status."""
    events = transactions("events")
    users = transactions("users")
    user_levels = transactions("user_levels")
    # Only the length of each song version is needed from this table
    durations = transactions("song_versions").select("version_id", "duration")

    # Listening totals for each user
    event_aggs = (
        events
        # Attach the song length to each event, keeping events with no match
        .join(durations, on="version_id", how="left")
        # One group per user
        .groupBy("user_id")
        .agg(
            # Number of plays
            F.count("*").alias("total_plays"),
            # Number of different sessions
            F.countDistinct("session_id").alias("total_sessions"),
            # Total song length played, 0 when no lengths are known
            F.coalesce(F.sum("duration"), F.lit(0)).alias("total_listening_seconds"),
            # Number of different songs played
            F.countDistinct("song_id").alias("distinct_songs_played"),
            # Number of different artists played
            F.countDistinct("artist_id").alias("distinct_artists_played"),
            # First and last time the user was active
            F.min("ts").alias("first_seen_at"),
            F.max("ts").alias("last_seen_at"),
        )
    )

    # Number each user's level changes from the most recent to the oldest
    latest_change_first = Window.partitionBy("user_id").orderBy(F.col("changed_at").desc())
    current_levels = (
        user_levels
        .withColumn("rn", F.row_number().over(latest_change_first))
        # Keep only the most recent change, which is the current level
        .where(F.col("rn") == 1)
        .select("user_id", F.col("new_level").alias("current_level"))
    )

    # Users who have ever moved to the paid level
    ever_paid = (
        user_levels
        .where((F.col("new_level") == "paid") & (F.col("source") == "transition"))
        .select("user_id")
        .distinct()
        # Mark each of them as converted
        .withColumn("is_converted", F.lit(True))
    )

    return (
        # Start from every user so those with no activity are kept
        users.select("user_id")
        .join(event_aggs, on="user_id", how="left")
        .join(current_levels, on="user_id", how="left")
        .join(ever_paid, on="user_id", how="left")
        .select(
            "user_id",
            # Users with no activity get 0 instead of null
            F.coalesce(F.col("total_plays"), F.lit(0)).alias("total_plays"),
            F.coalesce(F.col("total_sessions"), F.lit(0)).alias("total_sessions"),
            F.coalesce(F.col("total_listening_seconds"), F.lit(0)).alias("total_listening_seconds"),
            F.coalesce(F.col("distinct_songs_played"), F.lit(0)).alias("distinct_songs_played"),
            F.coalesce(F.col("distinct_artists_played"), F.lit(0)).alias("distinct_artists_played"),
            "first_seen_at",
            "last_seen_at",
            "current_level",
            # Users who never moved to paid are not converted
            F.coalesce(F.col("is_converted"), F.lit(False)).alias("is_converted"),
        )
    )


BUILDERS = {
    "artist_facts": build_artist_facts,
    "session_facts": build_session_facts,
    "song_facts": build_song_facts,
    "user_facts": build_user_facts,
}

result_df = BUILDERS[table_name]()

target_table = f"iceberg.analytics.{table_name}"

result_df.writeTo(target_table).using("iceberg").createOrReplace()

print(f"Completed Iceberg overwrite for {table_name}")
