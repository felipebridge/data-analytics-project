import pandas as pd
from sqlalchemy import create_engine, text

RAW_PATH = "data/raw/spotify_history.csv"
DB_URL = "postgresql+psycopg2://spotify:spotify@localhost:5432/spotify_analytics"

SKIP_THRESHOLD_MS = 30_000

def extract(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    df["played_at"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["played_at"])

    df["date_key"] = (df["played_at"].dt.year * 10000 +
                      df["played_at"].dt.month * 100 +
                      df["played_at"].dt.day).astype(int)
    df["hour"] = df["played_at"].dt.hour.astype(int)

    df["ms_played"] = pd.to_numeric(df["ms_played"], errors="coerce").fillna(0).astype(int)
    df["minutes_played"] = (df["ms_played"] / 60000.0).round(4)

    df["skipped"] = df["skipped"].astype(bool)
    df["shuffle"] = df["shuffle"].astype(bool)

    df["is_play"] = df["ms_played"] > 0
    df["is_effective_play"] = (df["ms_played"] >= SKIP_THRESHOLD_MS) & (~df["skipped"])
    df["is_skip"] = df["skipped"] | ((df["ms_played"] > 0) & (df["ms_played"] < SKIP_THRESHOLD_MS))

    for col in ["track_name", "artist_name", "album_name"]:
        df[col] = df[col].astype(str).str.strip()
    df["spotify_track_uri"] = df["spotify_track_uri"].astype(str).str.strip()

    dim_track = (
    df[["spotify_track_uri", "track_name", "artist_name", "album_name"]]
    .rename(columns={"spotify_track_uri": "track_uri"})
    .dropna(subset=["track_uri"])
    .drop_duplicates(subset=["track_uri"], keep="first")
)

    fact = (
        df.rename(columns={"spotify_track_uri": "track_uri"})
          [["played_at","date_key","hour","platform","track_uri","ms_played","minutes_played",
            "skipped","shuffle","reason_start","reason_end","is_play","is_effective_play","is_skip"]]
    )

    return dim_track, fact

def load(dim_track: pd.DataFrame, fact: pd.DataFrame, db_url: str) -> None:
    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(text("create schema if not exists analytics;"))

    dim_track.to_sql("dim_track", engine, schema="analytics", if_exists="append", index=False, method="multi")

    fact.to_sql("fact_stream", engine, schema="analytics", if_exists="append", index=False, method="multi", chunksize=50_000)

if __name__ == "__main__":
    df = extract(RAW_PATH)
    dim_track, fact = transform(df)
    load(dim_track, fact, DB_URL)