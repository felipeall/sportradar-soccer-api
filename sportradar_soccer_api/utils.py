import pandas as pd
import requests
from datetime import datetime

last_updated = datetime.now().strftime("%Y-%m-%d")


def _explode_column(df_to_explode: pd.DataFrame, col_to_explode: str, cols_to_keep:list[str] = None):
    df = df_to_explode.copy()
    if cols_to_keep: df = df.loc[:,cols_to_keep+[col_to_explode]]
    
    df = df.explode(col_to_explode)

    normalized = pd.json_normalize(df[col_to_explode]).add_prefix(f'{col_to_explode}.')
    normalized.index = df.index

    df = df.join(normalized)
    df = df.drop(columns=col_to_explode)

    return df


def _explode_column_period_scores(df_to_explode: pd.DataFrame):

    df = df_to_explode.copy()

    exploded = df.explode('sport_event_status.period_scores')
    normalized = pd.json_normalize(exploded['sport_event_status.period_scores'])
    normalized['score'] = normalized['home_score'].astype('string') + 'x' + normalized['away_score'].astype('string')

    normalized.index = exploded.index
    normalized = normalized.reset_index()

    grouping = normalized.groupby('index').score.apply(lambda x: x.tolist())
    grouped = pd.DataFrame(grouping.tolist(), index=grouping.index)

    grouped[['sport_event_status.period_scores.regular_period.1.home_score', 'sport_event_status.period_scores.regular_period.1.away_score']] = grouped[0].str.split('x', expand=True)
    grouped[['sport_event_status.period_scores.regular_period.2.home_score', 'sport_event_status.period_scores.regular_period.2.away_score']] = grouped[1].str.split('x', expand=True)
    if 2 in grouped.columns: grouped[['sport_event_status.period_scores.overtime.home_score', 'sport_event_status.period_scores.overtime.away_score']] = grouped[2].str.split('x', expand=True)
    if 3 in grouped.columns: grouped[['sport_event_status.period_scores.overtime.home_score', 'sport_event_status.period_scores.overtime.away_score']] = grouped[2].str.split('x', expand=True)
    grouped = grouped.drop(columns=[0,1,2,3], errors='ignore')

    df = df.join(grouped)
    df = df.drop(columns='sport_event_status.period_scores')

    return df


def _parse_columns_dtypes(df: pd.DataFrame, cols_dtypes: dict):
    for col, col_dtype in cols_dtypes.items():
        if col in df.columns:
            df[col] = df[col].astype(col_dtype)
    
    return df


def _format_competitions(response: requests.models.Response):
    competitions = pd.json_normalize(response.json(), "competitions")

    cols_dtypes = {
        'id' : 'string',
        'name' : 'string',
        'gender' : 'string',
        'category.id' : 'string',
        'category.name' : 'string',
        'category.country_code' : 'string',
        'parent_id' : 'string',
    }

    competitions = _parse_columns_dtypes(competitions, cols_dtypes)
    competitions = competitions.assign(last_updated=last_updated)

    return competitions


def _format_seasons(response: requests.models.Response):
    seasons_df = pd.json_normalize(response.json(), "seasons")

    seasons_dtypes = {
        'id': 'string',
        'name': 'string',
        'start_date': 'datetime64[ns]',
        'end_date': 'datetime64[ns]',
        'year': 'string',
        'competition_id': 'string'
    }

    seasons_df = seasons_df.astype(seasons_dtypes)
    seasons_df = seasons_df.assign(last_updated=last_updated)

    return seasons_df


def _format_season_summary(response: requests.models.Response):
    season_summary = pd.json_normalize(response.json(), "summaries")

    if season_summary.empty: 
        return pd.DataFrame({'sport_event.sport_event_context.season.id': [response.url.split("/")[-2]]})

    season_summary = _explode_column_period_scores(season_summary)
    season_summary = _explode_column(season_summary, 'sport_event.sport_event_context.groups')
    season_summary = season_summary.assign(last_updated=last_updated)

    cols_drop = [
        'sport_event.sport_event_conditions.referees',
        'sport_event.competitors',
        'sport_event_status.ball_locations',
        'statistics.totals.competitors',
        'sport_event.channels',
    ]

    season_summary = season_summary.drop(columns=cols_drop, errors='ignore')

    return season_summary


def _format_season_players_statistics(response: requests.models.Response):
    season_players_statistics = pd.json_normalize(response.json(), "summaries")

    if season_players_statistics.empty: 
        return pd.DataFrame({'sport_event.sport_event_context.season.id': [response.url.split("/")[-2]]})

    season_players_statistics = season_players_statistics.loc[season_players_statistics['sport_event_status.match_status'] == 'ended']
    season_players_statistics = _explode_column(season_players_statistics, 'statistics.totals.competitors', ['sport_event.id', 'sport_event.sport_event_context.competition.id', 'sport_event.sport_event_context.season.id'])
    season_players_statistics = _explode_column(season_players_statistics, 'statistics.totals.competitors.players', ['sport_event.id', 'sport_event.sport_event_context.competition.id', 'sport_event.sport_event_context.season.id', 'statistics.totals.competitors.id'])
    season_players_statistics = season_players_statistics.fillna(0)
    season_players_statistics = season_players_statistics.reset_index(drop=True)
    season_players_statistics = season_players_statistics.assign(last_updated=last_updated)

    season_players_statistics.columns = [col.split(".")[-1] if '.players.' in col else ".".join(col.split('.')[-2:]) for col in season_players_statistics.columns]
    season_players_statistics[season_players_statistics.select_dtypes(float).columns] = season_players_statistics.select_dtypes(float).astype('int64')

    return season_players_statistics


def _format_season_competitors_statistics(response: requests.models.Response):
    season_competitors_statistics = pd.json_normalize(response.json(), "summaries")

    if season_competitors_statistics.empty: 
        return pd.DataFrame({'sport_event.sport_event_context.season.id': [response.url.split("/")[-2]]})

    season_competitors_statistics = season_competitors_statistics.loc[season_competitors_statistics['sport_event_status.match_status'] == 'ended']
    season_competitors_statistics = _explode_column(season_competitors_statistics, 'statistics.totals.competitors', ['sport_event.id', 'sport_event.sport_event_context.competition.id', 'sport_event.sport_event_context.season.id'])
    season_competitors_statistics = season_competitors_statistics.drop(columns='statistics.totals.competitors.players')
    season_competitors_statistics = season_competitors_statistics.reset_index(drop=True)
    season_competitors_statistics = season_competitors_statistics.fillna(0)
    season_competitors_statistics = season_competitors_statistics.assign(last_updated=last_updated)

    season_competitors_statistics.columns = [col.split('.')[-1] if col.startswith('statistics') else col.replace('sport_event.sport_event_context.', '') for col in season_competitors_statistics.columns]
    season_competitors_statistics[season_competitors_statistics.select_dtypes(float).columns] = season_competitors_statistics.select_dtypes(float).astype('int64')

    return season_competitors_statistics


def _format_season_referees(response: requests.models.Response):
    season_referees = pd.json_normalize(response.json(), "summaries")

    if season_referees.empty or 'sport_event.sport_event_conditions.referees' not in season_referees.columns: 
        return pd.DataFrame({'sport_event.sport_event_context.season.id': [response.url.split("/")[-2]]})

    season_referees = season_referees.loc[:,['sport_event.id', 'sport_event.sport_event_context.season.id', 'sport_event.sport_event_conditions.referees']]
    season_referees = season_referees.explode('sport_event.sport_event_conditions.referees')

    normalized = pd.json_normalize(season_referees['sport_event.sport_event_conditions.referees'])
    normalized.index = season_referees.index

    season_referees = season_referees.join(normalized)
    season_referees = season_referees.drop(columns='sport_event.sport_event_conditions.referees')
    season_referees = season_referees.rename(columns={'sport_event.sport_event_context.season.id': 'season.id'})
    season_referees = season_referees.reset_index(drop=True)

    season_referees = season_referees.assign(last_updated=last_updated)

    return season_referees


def _format_season_ball_locations(response: requests.models.Response):
    season_ball_locations = pd.json_normalize(response.json(), "summaries")

    if season_ball_locations.empty or 'sport_event_status.ball_locations' not in season_ball_locations.columns: 
        return pd.DataFrame({'sport_event.sport_event_context.season.id': [response.url.split("/")[-2]]})

    season_ball_locations = season_ball_locations.loc[:,['sport_event.id', 'sport_event.sport_event_context.season.id','sport_event_status.ball_locations']]
    season_ball_locations = season_ball_locations.explode('sport_event_status.ball_locations')

    normalized = pd.json_normalize(season_ball_locations['sport_event_status.ball_locations'])
    normalized.index = season_ball_locations.index

    season_ball_locations = season_ball_locations.join(normalized)
    season_ball_locations = season_ball_locations.drop(columns='sport_event_status.ball_locations')
    season_ball_locations = season_ball_locations.rename(columns={'sport_event.sport_event_context.season.id': 'season.id'})
    season_ball_locations = season_ball_locations.reset_index(drop=True)
    
    season_ball_locations = season_ball_locations.assign(last_updated=last_updated)

    return season_ball_locations


def _format_season_channels(response):
    season_channels = pd.json_normalize(response.json(), "summaries")

    if season_channels.empty or 'sport_event.channels' not in season_channels.columns: 
        return pd.DataFrame({'sport_event.sport_event_context.season.id': [response.url.split("/")[-2]]})

    season_channels = season_channels.loc[:,['sport_event.id', 'sport_event.sport_event_context.season.id','sport_event.channels']]
    season_channels = season_channels.explode('sport_event.channels')

    normalized = pd.json_normalize(season_channels['sport_event.channels'])
    normalized.index = season_channels.index

    season_channels = season_channels.join(normalized)
    season_channels = season_channels.drop(columns='sport_event.channels')
    season_channels = season_channels.rename(columns={'sport_event.sport_event_context.season.id': 'season.id'})
    season_channels = season_channels.reset_index(drop=True)
    
    season_channels = season_channels.assign(last_updated=last_updated)

    return season_channels


def _format_player_profile(response: requests.models.Response):
    player_profile = pd.json_normalize(response.json())
    player_profile = player_profile.loc[:,player_profile.columns.str.startswith('player.')]
    player_profile.columns = [col.split(".")[-1] for col in player_profile.columns]

    cols_dtypes = {
        'id' : 'string',
        'name' : 'string',
        'type' : 'string',
        'date_of_birth' : 'datetime64[ns]',
        'nationality' : 'string',
        'country_code' : 'string',
        'height' : 'int',
        'weight' : 'int',
        'jersey_number' : 'string',
        'preferred_foot' : 'string',
        'place_of_birth' : 'string',
        'nickname' : 'string',
        'gender' : 'string',
        'xgender' : 'string',
    }

    player_profile = _parse_columns_dtypes(player_profile, cols_dtypes)
    player_profile = player_profile.assign(last_updated=last_updated)

    return player_profile
