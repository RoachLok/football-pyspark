from flask import Flask, make_response, request
from flask_caching import Cache
from util.databridge import Databridge
from util.queries import SampleQuery
import schema.data_structs as schema
import data.paths as data_routes

config = {   
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300,
}

data = Databridge(data_location='local[8]', name='API')
reader = data.get_reader()

data.add_dataframes([
    (
        reader.csv(
            data_routes.tech_appearences,
            header=True,
            schema=schema.tech_appearences
        ),
        'tech_appearences'
    ),
    # (
    #     reader.csv(
    #         data_routes.tech_playervalues,
    #         header=True,
    #         schema=schema.tech_playervalues
    #     ),
    #     'tech_playervalues'
    # ),
    (
        reader.csv(
            data_routes.tech_games,
            header=True,
            schema=schema.tech_games
        ),
        'tech_games'
    ),
    (
        reader.csv(
            data_routes.tech_leagues,
            header=True,
            schema=schema.tech_leagues
        ),
        'tech_leagues'
    ),   
    (
        reader.csv(
            data_routes.tech_players,
            header=True,
            schema=schema.tech_players
        ),
        'tech_players'
    ),
    (
        reader.csv(
            data_routes.tech_shots,
            header=True,
            schema=schema.tech_shots
        ),
        'tech_shots'
    ),
    (
        reader.csv(
            data_routes.tech_teams,
            header=True,
            schema=schema.tech_teams
        ),
        'tech_teams'
    ),
    (
        reader.csv(
            data_routes.tech_teamstats,
            header=True,
            schema=schema.tech_teamstats
        ),
        'tech_teamstats'
    ),
])


#data.add_dataframe(data.join_stored('cards', 'weather', on_join_tag='FECHA'), 'cards_weather')

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)

@app.route('/')
def index():
    return "Hi", 200



"""
Sample query handling

@app.route('/expenditure')
@cache.cached(timeout=300, query_string=True)
def expenditure():
    query_by = request.args.get('query_by') or 'by_day'
    sector_filter = request.args.get('sector_filter') or None

    csv_str = expense.query(data, modifier=query_by, sector_filter=sector_filter)

    if not csv_str:
        return make_response('{"message": "Bad filter"}', 400, {'Content-type' : 'application/json'})

    return make_response(csv_str, 200, {'Content-Disposition' : 'attachment; filename=export.csv', 'Content-type' : 'text/csv'})
"""


if __name__ == '__main__':
    app.run(host='0.0.0.0')



# data.add_dataframes([
#     (
#         reader.csv(
#             data_routes.trmkt_appearences,
#             header=True,
#             schema=schema.trmkt_appearences
#         ),
#         'trmkt_appearences'
#     ),
#     (
#         reader.csv(
#             data_routes.trmkt_clubs,
#             header=True,
#             schema=schema.trmkt_clubs
#         ),
#         'trmkt_clubs'
#     ),
#     (
#         reader.csv(
#             data_routes.trmkt_competitions,
#             header=True,
#             schema=schema.trmkt_competitions
#         ),
#         'trmkt_competitions'
#     ),   
#     (
#         reader.csv(
#             data_routes.trmkt_games,
#             header=True,
#             schema=schema.trmkt_games
#         ),
#         'trmkt_games'
#     ),
#     (
#         reader.csv(
#             data_routes.trmkt_leagues,
#             header=True,
#             schema=schema.trmkt_leagues
#         ),
#         'trmkt_leagues'
#     ),
#     (
#         reader.csv(
#             data_routes.trmkt_players,
#             header=True,
#             schema=schema.trmkt_players
#         ),
#         'trmkt_players'
#     ),
# ])   