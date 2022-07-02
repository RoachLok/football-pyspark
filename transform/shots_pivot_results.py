from pyspark.sql import DataFrame
from pyspark.sql.functions import count, col, expr


def shots_pivot_results(
    shots_df    : DataFrame,
    situation   : str,
    players_df  : DataFrame | None = None,
    order_by     = 'Goal',
    only_goalers = True, 
    percentage   = True,
    csv=True
) -> str:
    """Returns a CSV or JSON string of penalty_cards aggregated based on
    user given parameters.

    Parameters
    ----------
    shots_df : The dataframe containing the required columns to pivot the 
        shot types. Columns: 'player_id', 'situation', 'shot_result',
        For technika148 dataset this is the shots csv.
            Should be a pyspark.sql DataFrame (:class:`DataFrame`).

    situation : Type of shot. In technika148's dataset it can be 
    [ 'DirectFreekick', 'SetPiece', 'OpenPlay', 'FromCorner', 'Penalty' ].

    players_df : The dataframe containing player names. Can be null. If
        provided return result will contain player names.
            Should be a pyspark.sql DataFrame (:class:`DataFrame`).

    order_by : Name of column you want to order the result by. 
        Default is 'Goal'.
        Use player_id as order_by column when using json to get an indexed
        json.

    only_goalers : Return only players who have scored goals.
        Default is True.

    percentage : Return percentage of shots that scored goals. 
        Default is True.

    csv : Boolean to determine wether result is in csv or json format.
    If True csv format is returned, if False json is returned instead.

    Examples
    --------
    >>> response_str = shots_pivot_results(
    >>>     shots_df        = data.get_dataframe('tech_shots'),
    >>>     situation       = situation,
    >>>     players_df      = names_df,
    >>>     order_by        = order_by,
    >>>     only_goalers    = only_goalers,
    >>>     percentage      = percentage,
    >>>     csv = format_csv
    >>> )
    """
    
    shots = shots_df.select(
            'player_id',
            'situation',
            'shot_result',
        ).where(f'situation == "{situation}"') \
        .groupBy('player_id') \
        .pivot('shot_result').agg(count('*')).cache()

    
    if only_goalers:
        shots = shots.where(col('Goal').isNotNull())

    if percentage:
        shots = shots.withColumn(
                'goal_percentage',
                expr('Goal / (Goal + MissedShots + SavedShot + ShotOnPost)')
            )
    
    if players_df:
        shots = shots.join(players_df, 'player_id') \


    return shots.orderBy(order_by, ascending=False).toPandas().to_csv() if csv \
        else shots.toPandas().set_index("player_id").to_json(orient="index") if order_by == 'player_id' \
        else '['+','.join(shots.orderBy(order_by, ascending=False).toJSON().collect())+']' 
    
     