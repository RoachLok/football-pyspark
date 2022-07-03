from pyspark.sql import DataFrame
import util.agg_functions as agf
from pyspark.sql.functions import count, when, col, sum, expr


def club_home_won_matches(
    games_clubs_df   : DataFrame,
    lasts_keys       : list[str],
    sums_keys        : list[str],
    calc_percentage  : bool,
    seasons          : list[int] | None = None,
    min_played_games : int       | None = None,
    order_by = 'home_won_games',
    csv=True,
) -> str:
    """Returns a CSV or JSON string of penalty_cards aggregated based on
    user given parameters.
    Parameters
    ----------
    games_clubs_df :  The dataframe containing the required columns to aggregate
    club wins when home. 'pretty_name', 'away_club_goals', 'home_club_goals',
    'season', 'away_club_id'. For transfermarkt dataset this is a join of
    games and clubs.
        Should be a pyspark.sql DataFrame (:class:`DataFrame`).
    lasts_keys : List of strings contaning column names for the columns you
    want to display as they are. 'pretty_name', 'season'.
    sums_keys : List of strings contaning column names for the columns you
    want to display the sum of. 'home_club_goals', 'away_club_goals', 'season'.
    calc_percentage : Boolean indicating wether to calc home win percentage or not.
    seasons : Optional. List of ints containing seasons to filter results by.
        >>> .where(f'season == {season}')
    min_played_games : Optional. Minimum amount of games played to show on results.
        >>> .where(f'played_games > {min_played_games}')
    order_by : Name of column you want to order the result by.
    Use player_id as order_by column when using json to get an indexed
    json.
    csv : Boolean to determine wether result is in csv or json format.
    If True csv format is returned, if False json is returned instead.
    Examples
    --------
    >>> response_str = club_goals_when_visiting(
    >>>     player_app_df   = data.join_stored('trmkt_games', 'trmkt_clubs', ['home_club_id', 'club_id']),
    >>>     lasts_keys      = ['pretty_name', 'season'],
    >>>     sums_keys       = ['home_club_goals', 'away_club_goals'],
    >>>     calc_percentage = calc_percentage,
    >>>     seasons         = requested_seasons,
    >>>     order_by        = order_by,
    >>>     csv = format_csv
    >>> )
    """

    res_df = games_clubs_df.select(
            'pretty_name',
            'away_club_goals',
            'home_club_goals',
            'season',
            'home_club_id'    
        ).groupBy('home_club_id')\
        .agg(
            *agf.lasts(lasts_keys),
            *agf.sums(sums_keys),
            count('home_club_id').alias('played_games'),
            sum(
                when(
                    col('home_club_goals') > col('away_club_goals'),
                    1
                ).otherwise(0)
            ).alias('home_won_games')
        ).cache()
    
    if calc_percentage:
        res_df = res_df.withColumn(
                'home_win_percent',
                expr('home_won_games / played_games')
            )

    if seasons:
        for year in seasons:
            res_df = res_df.where(f'season == {year}')

    if min_played_games:
        res_df = res_df.where(f'played_games > {min_played_games}')


    return res_df.orderBy(order_by, ascending=False).toPandas().to_csv() if csv \
        else res_df.toPandas().set_index("player_id").to_json(orient="index") if order_by == 'player_id' \
        else '['+','.join(res_df.toJSON().collect())+']' 


def club_goals_when_visiting(
    games_clubs_df   : DataFrame,
    lasts_keys       : list[str],
    sums_keys        : list[str],
    scoring_ratio    : bool,
    seasons          : list[int] | None = None,
    order_by = 'scored_goals',
    csv=True,
) -> str:
    """Returns a CSV or JSON string of penalty_cards aggregated based on
    user given parameters.
    Parameters
    ----------
    games_clubs_df : The dataframe containing the required columns to aggregate
    club goals when visiting. 'pretty_name', 'away_club_goals', 'home_club_goals',
    'season', 'away_club_id'. For transfermarkt dataset this is a join of
    games and clubs.
        Should be a pyspark.sql DataFrame (:class:`DataFrame`).
    lasts_keys : List of strings contaning column names for the columns you
    want to display as they are. 'pretty_name', 'season'.
    sums_keys : List of strings contaning column names for the columns you
    want to display the sum of. 'home_club_goals', 'away_club_goals'.
    scoring_ratio : Boolean indicating wether to scoring ratio or not.
    seasons : Optional. List of ints containing seasons to filter results by.
        >>> .where(f'season == {season}')
    
    order_by : Name of column you want to order the result by.
    Use player_id as order_by column when using json to get an indexed
    json.
    csv : Boolean to determine wether result is in csv or json format.
    If True csv format is returned, if False json is returned instead.
    Examples
    --------
    >>> response_str = club_goals_when_visiting(
    >>>     player_app_df   = data.join_stored('trmkt_games', 'trmkt_clubs', ['away_club_id', 'club_id']),
    >>>     lasts_keys      = ['pretty_name', 'season'],
    >>>     sums_keys       = ['home_club_goals', 'away_club_goals'],
    >>>     scoring_ratio   = scoring_ratio,
    >>>     seasons         = requested_seasons,
    >>>     order_by        = order_by,
    >>>     csv = format_csv
    >>> )
    """

    res_df = games_clubs_df.select(
            'pretty_name',
            'away_club_goals',
            'home_club_goals',
            'season',
            'away_club_id'    
        ).groupBy('away_club_id')\
        .agg(
            *agf.lasts(lasts_keys),
            *agf.sums(sums_keys),
        ).cache()
    
    if scoring_ratio:
        res_df = res_df.withColumn(
                'scoring_ratio',
                expr('(taken_goals + scored_goals) / scored_goals')
            )

    if seasons:
        for year in seasons:
            res_df = res_df.where(f'season == {year}')


    return res_df.orderBy(order_by, ascending=False).toPandas().to_csv() if csv \
        else res_df.toPandas().set_index("player_id").to_json(orient="index") if order_by == 'player_id' \
        else '['+','.join(res_df.toJSON().collect())+']' 