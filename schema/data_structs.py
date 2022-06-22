from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

trmkt_appearences = StructType([ 
    StructField('player_id'                     , IntegerType()), 
    StructField('game_id'                       , IntegerType()),
    StructField('appearence_id'                 , IntegerType()),
    StructField('competition_id'                , IntegerType()),
    StructField('player_club_id'                , IntegerType()),
    StructField('goals'                         , IntegerType()),
    StructField('assists'                       , IntegerType()),
    StructField('minutes_played'                , IntegerType()),
    StructField('yellow_cards'                  , IntegerType()),
    StructField('red_cards'                     , IntegerType())
])
                        

trmkt_clubs = StructType([
    StructField('club_id'                       , IntegerType()), 
    StructField('name'                          , StringType ()),
    StructField('pretty_name'                   , StringType ()),
    StructField('dcomestic_competition_id'      , StringType ()),
    StructField('total_market_value'            , StringType ()),
    StructField('squad_size'                    , IntegerType()),
    StructField('average_age'                   , StringType ()),
    StructField('foreigners_number'             , IntegerType()),
    StructField('foreigners_percentage'         , FloatType  ()),
    StructField('national_team_players'         , IntegerType()),
    StructField('stadium_name'                  , StringType ()),
    StructField('stadium_seats'                 , IntegerType()),
    StructField('net_transfer_record'           , StringType ()),
    StructField('coach_name'                    , StringType ()),
    StructField('url'                           , StringType ()),
])

trmkt_competitions = StructType([ 
    StructField('competition_id'                , IntegerType()), 
    StructField('name'                          , StringType ()),
    StructField('type'                          , StringType ()),
    StructField('country_id'                    , IntegerType()),
    StructField('country_name'                  , StringType ()),
    StructField('domestic_league_code'          , StringType ()),
    StructField('confederation'                 , StringType ()),
    StructField('url'                           , StringType ()) 
])

trmkt_players = StructType([ 
    StructField('player_id'                     , IntegerType()),
    StructField('current_club_id'               , IntegerType()),
    StructField('name'                          , StringType ()),
    StructField('pretty_name'                   , StringType ()),
    StructField('country_of_birth'              , StringType ()),
    StructField('country_of_citizenship'        , StringType ()),
    StructField('date_of_birth'                 , StringType ()),
    StructField('position'                      , StringType ()),
    StructField('sub_position'                  , StringType ()),
    StructField('foot'                          , StringType ()),
    StructField('height_in_cm'                  , IntegerType()),
    StructField('market_value_in_gbp'           , FloatType  ()),
    StructField('highest_market_value_in_gbp'   , FloatType  ()),
    StructField('url'                           , StringType ())
])

trmkt_games = StructType([
    StructField('game_id'                       , IntegerType()),
    StructField('competition_code'              , StringType ()),
    StructField('season'                        , IntegerType()),
    StructField('round'                         , StringType ()),
    StructField('date'                          , StringType ()),
    StructField('home_club_id'                  , IntegerType()),
    StructField('away_club_id'                  , IntegerType()),
    StructField('home_club_goals'               , IntegerType()),
    StructField('away_club_goals'               , IntegerType()),
    StructField('home_club_position'            , IntegerType()),
    StructField('away_club_position'            , IntegerType()),
    StructField('stadium'                       , StringType ()),
    StructField('attendance'                    , StringType ()),
    StructField('referee'                       , StringType ()),
    StructField('url'                           , StringType ())
])

trmkt_leagues = StructType([ 
    StructField('league_id'                     , StringType ()),
    StructField('name'                          , StringType ()),
    StructField('confederation'                 , StringType ())
])

trmkt_players = StructType([ 
    StructField('game_id'                       , IntegerType()),
    StructField('current_club_id'               , IntegerType()),
    StructField('name'                          , StringType ()),
    StructField('pretty_name'                   , StringType ()),
    StructField('country_of_birth'              , StringType ()),
    StructField('country_of_citizenship'        , StringType ()),
    StructField('date_of_birth'                 , StringType ()),
    StructField('position'                      , StringType ()),
    StructField('sub_position'                  , StringType ()),
    StructField('foot'                          , StringType ()),
    StructField('height_in_cm'                  , IntegerType()),
    StructField('market_value_in_gbp'           , FloatType  ()),
    StructField('highest_market_value_in_gbp'   , FloatType  ()),
    StructField('url'                           , StringType ())
])