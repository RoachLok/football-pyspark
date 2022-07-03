from pyspark.sql import DataFrame
import util.agg_functions as agf
from pyspark.sql.functions import avg, count, when, col, sum


def penalty_cards_agg(
    player_app_df   : DataFrame,
    lasts_keys      : list[str],
    sums_keys       : list[str],
    pl_position     : str,
    order_by = 'yellow_cards',
    csv=True,
) -> str:
    """Returns a CSV or JSON string of penalty_cards aggregated based on
    user given parameters.

    Parameters
    ----------
    player_app_df : The dataframe containing the required columns to aggregate
    the penalty cards. 'player_id', 'yellow_cards', 'red_cards',
    'minutes_played', 'position', 'sub_position'. For transfermarkt dataset
    this is a join of appearences and players.
        Should be a pyspark.sql DataFrame (:class:`DataFrame`).

    lasts_keys : List of strings contaning column names for the columns you
    want to display as they are.

    sums_keys : List of strings contaning column names for the columns you
    want to display the sum of.

    pl_position : Position of the player. Defender, Attacker, Goalkeeper 
    or Midfield.

    order_by : Name of column you want to order the result by.
    User player_id as order_by column when using json to get an indexed
    json.

    csv : Boolean to determine wether result is in csv or json format.
    If True csv format is returned, if False json is returned instead.

    Examples
    --------
    >>> penalty_cards_agg(
    >>>     player_app_df   = data.join_stored('trmkt_appearences', 'trmkt_players', 'player_id'),
    >>>     lasts_keys      = ['position', 'sub_position'],
    >>>     sums_keys       = ['yellow_cards', 'red_cards'],
    >>>     pl_position     = player_positon,
    >>>     order_by        = order_by,
    >>>     csv = format_csv
    >>> )
    """

    res_df = player_app_df.select(
            'player_id',
            'yellow_cards',
            'red_cards',
            'minutes_played',
            'position',
            'sub_position',
            'pretty_name'
        ).where(f'position == "{ pl_position }"') \
        .groupBy('player_id') \
        .agg(
            *agf.lasts(lasts_keys),
            *agf.sums(sums_keys),
            avg('minutes_played').alias('avg_minutes_played'),
            count('player_id').alias('n_games'),
            sum(when(col('yellow_cards') == 2, 1).otherwise(0)).alias('double_yellow')
        )


    return res_df.orderBy(order_by, ascending=False).toPandas().to_csv() if csv \
        else res_df.toPandas().set_index("player_id").to_json(orient="index") if order_by == 'player_id' \
        else '['+','.join(res_df.toJSON().collect())+']' 