from asyncio.windows_events import NULL
from email.mime import base
from itertools import groupby
from turtle import width
import pandas as pd
import streamlit as st
import altair as alt
import matplotlib.pyplot as plt
import plotly.express as px
from io import BytesIO
import requests 
import numpy as np
from streamlit_option_menu import option_menu
# from bokeh.plotting import figure










st.markdown('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">', unsafe_allow_html=True)

st.markdown(
        f"""
<style>
	.main {{
  margin-top: 50px; 
	}}
	.reportview-container .main .block-container{{
		position: fixed;
        max-width: 80%;
        padding-top: 5px;
        padding-right: 25px;
        padding-left: 25px;
        padding-bottom: 25px;
        <!--background-color: red->;
    }}
    img{{
    	max-width:40%;
    }}
</style>
""",
        unsafe_allow_html=True,
    )
def kpi_goles_fueraCasa():
	url = base_url + '/api/club_goals_visiting?order_by=away_club_goals&json=false'
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	df1 = df1.filter(['pretty_name', 'home_club_goals', 'away_club_goals'])
	st.write(df1)

	df = df1.head(50)
	labels = ['Home goals', 'Away goals']
	home_goals = df['home_club_goals'].sum()
	away_goals = df['away_club_goals'].sum()
	# df = df1.filter(['pretty_name', 'home_club_goals' 'away_club_goals'])
	# st.write(away_goals)
	# st.write(home_goals)
	sizes = [home_goals, away_goals]
	explode = (0, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')
	fig1, ax1 = plt.subplots()
	ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
			shadow=True, startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	
	st.write("De los 50 equipos más goleadores fuera de casa, la proporción de goles marcados a goles encajados, siendo visitante")
	st.pyplot(fig1)	

	

def kpi_faltas_directas():
	st.write("Faltas Directas")
	url = base_url + '/api/shots?situation=DirectFreekick&json=false&only_goalers=true&order_by=Goal&player_names=true&percentage=true'
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	st.write(df1)
	
	df = df1.filter(['name', 'Goal', 'goal_percentage'])
	# df = df.query('Goal > 5')
	df = df.query('goal_percentage > 0.01')
	df = df.sort_values(by=['goal_percentage'], ascending=False)
	st.write('Ranking de jugadores los cuales tienen la mayor probabilidad de meter un gol de falta')
	st.write(df)
	players = df['name'].count()
	percentage = df['goal_percentage'].sum()
	# st.write(players)
	percentage = percentage / players
	percentage = percentage * 100
	st.write('Probabilidad de que un jugador marque una falta')
	st.write(percentage)

	labels = 'No-Goal', 'Goal'
	noGoal = 100 - percentage
	sizes = [noGoal, percentage]
	
	explode = (0, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')
	fig1, ax1 = plt.subplots()
	ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
			shadow=True, startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	
	st.write("Probabilidad de que un jugador que ya ha marcado un gol de falta, marque otra falta")
	st.pyplot(fig1)	



def kpi_ganar_casa():
	st.write("Equipos con más partidas ganados en casa")
	url = base_url + '/api/club_wins_home?order_by=home_won_games&calc_percentage=true&json=false'
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	
	st.write(df1)
	
	df1 = df1.head(40)
	won_games = []
	teams = []
	for x in df1['home_won_games']:
		won_games.append(x)
	
	for y in df1['pretty_name']:
		teams.append(y)

	data = pd.DataFrame({
		'index': teams,
		'won_games': won_games,
	}).set_index('index')

	st.write('Mejores equipos ganando en casa, donde en la gráfica se pueden ver los primeros 40')
	st.bar_chart(data)

def kpi_masMin_menosCards():
	url = base_url + '/api/penalty_cards?position=Midfield&json=false&order_by=double_yellow&pl_names=true'
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	# df1 = df1.query('avg_minutes_played > 80')
	# df1 = df1.query('n_games > 100')
	df1 = df1.sort_values(by=['yellow_cards'], ascending=False)
	st.write(df1)
	df = df1.query('avg_minutes_played < 60')
	df = df.query('avg_minutes_played > 45')
	# st.write(df)
	lessthan60 = df['yellow_cards'].sum()
	st.write('Tarjetas amarillas para los jugadores con menos de 60 minutos de media y mas de 45 ')
	st.write(lessthan60)

	df2 = df1.query('avg_minutes_played < 75')
	df2 = df2.query('avg_minutes_played > 60')
	# st.write(df2)
	less80more60 = df2['yellow_cards'].sum()
	st.write('Tarjetas amarillas para los jugadores con menos de 75 minutos de media y mas de 60 ')
	st.write(less80more60)

	df3 = df1.query('avg_minutes_played < 90')
	df3 = df3.query('avg_minutes_played > 75')
	# st.write(df3)

	less90more80 = df3['yellow_cards'].sum()
	st.write('Tarjetas amarillas para los jugadores con menos de 90 minutos de media y mas de 80 ')
	st.write(less90more80)
	labels = ['Avg < 60', 'Avg < 75', 'Avg < 90']
	values = [lessthan60, less80more60, less90more80]
	x = np.arange(len(labels))
	width = 0.35
	fig, ax = plt.subplots()
	rects1 = ax.bar(x - width/2, values, width, label='Yellow cards')
	ax.set_ylabel('Scores')
	ax.set_title('Scores by group and gender')
	ax.set_xticks(x, labels)
	ax.legend()
	ax.bar_label(rects1, padding=3)

	fig.tight_layout()
	st.write('Comparativa entre las tarjetas totales que reciben los jugadores dependiendo de la media de minutos jugados')
	st.write('Permite visualizar enque franja de tiempo se reciben la mayor cantidad de tarjetas amarillas')
	st.pyplot(fig)
	
def kpi_max_goleadores():
	url = base_url + '/api/shots?situation=OpenPlay&json=false&only_goalers=true&order_by=Goal&player_names=true&percentage=true'
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	df1 = df1.filter(['name', 'Goal', 'MissesShots', 'ShotOnPost', 'goal_percentage'])
	df1 = df1.head(3)
	st.write(df1)
	names = []
	for x in df1['name']:
		names.append(x)
	
	



	# st.write(names)
	valores = []
	url1 = base_url + '/api/get_player/'
	for z in names:
		# st.write(z)
		url_valor = url1 + z
		r = requests.get(url_valor)
		value = pd.read_csv(BytesIO(r.content))
		value = value.filter(['Player','Market value'])
		st.write(value)
		valor0 = value['Market value']
		valores.append(value['Market value'])


	jugadores = []
	goles = []
	for x in df1['Goal']:
		goles.append(x)
	
	for y in df1['name']:
	 	jugadores.append(y)

	data1 = pd.DataFrame({
		'index': jugadores,
		'goles': goles,
	}).set_index('index')
	st.write('Goles marcados por los 3 maximos goleadores, a la vez que viendo su valor en el mercado')
	st.bar_chart(data1)





def kpi_tajetas_defensas():
	url = base_url + '/api/penalty_cards?position=Defender&json=false&order_by=double_yellow&pl_names=true'
	#õrdery by tambien puede ser por doble amarillas 
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	df_double = df1
	st.write(df1)
	#Quesitos con todas las tarjetas de cada posicion de center back y right y left back
	labels = 'Centre-Back', 'Left-Back', 'Right-Back'
	centers = df1.groupby(['sub_position']).count()
	st.write(centers['yellow_cards'])
	sizes = centers['yellow_cards']
	# [15, 15, 45, 25]
	explode = (0, 0, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')
	fig1, ax1 = plt.subplots()
	ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
			shadow=True, startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	
	st.write("Proporcion de tarjetas amarillas en base a su posicion siendo defensa")
	st.pyplot(fig1)	

	st.write("Jugadores con mayor cantidad de dobles amarillas")
	
	#df_double.groupby('double_yellow').head(20)
	
	#head_name = df_double.filter(['pretty_name, double_yellow']).head(20)
	# for x in df_double['pretty_name'].head(20):
	df_double = df_double.filter(['pretty_name', 'double_yellow'])
	st.write(df_double.head(40))
	dobles = []
	jugadores = []
	#df_double.sort_values(by=['double_yellow'], inplace=False)
	df_double = df_double.head(40)



	jugadores = []
	dobles = []
	for x in df_double['double_yellow']:
		dobles.append(x)
	
	for y in df_double['pretty_name']:
		jugadores.append(y)

	data = pd.DataFrame({
		'index': jugadores,
		'dobles': dobles,
	}).set_index('index')
	st.bar_chart(data)
	

	
#######################################

# stats_container = st.container()	
#######################################
base_url = 'http://localhost:5000'
#######################################


selected = option_menu(
	menu_title="Proyecto final",
	options=["Home", "KPIs"],
	icons=["house", "book"],
	default_index=0,
	orientation="horizontal",
	styles={
        "container": {"padding": "0!important", "background-color": "#fafafa"},
        "icon": {"color": "blue", "font-size": "25px"}, 
        "nav-link": {"font-size": "20px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        "nav-link-selected": {"background-color": "grey"},
    }
)

if selected == "Home":
	st.title(f"Bienvenido!")
	st.subheader("Este es el proyecto final de Grandes Volumenes de Datos")
	st.write("Ha sido realizado por: **Manuel Salvador, Javier Taborda, Daniel Sabbagh, Alfonso Vega**")
	st.write("Este proyecto representa la práctica final de la asignatura de Grandes Volumenes de Datos.", 
	"En este proyecto se pretende obtener información de valor sobre los jugadores de futbol y su valor en el mercado,", 
	"A lo largo de esta práctica se desarrollan diferentes KPIs los cuales creemos que aporta información muy relevante al usuario sobre los diferentes jugadores y sus valores", 
	"Para esta práctica se han usado los siguientes datasets: ")
	st.write("https://www.kaggle.com/davidcariboo/player-scores")	
	st.write("https://www.kaggle.com/technika148/football-database")
	st.write("En el apartado de KPIs va a poder observar un dropbox con todo los KPIs realizados y podrá escoger el que desee clickando sobre el para observar el resultado del mismo")


if selected == "KPIs":
	st.title(f"Bienvenido!")
	listKpi = ['Tarjetas y minutos jugados siendo defensa','Equipos con mayor cantidad de goles marcados fuera de casa','Mediocentros y la proporcion de tarjetas en base a los minutos jugados','Equipos con mayor probabilidad de ganar en casa','Mayores goleadores por faltas directas','Mayores goleadores en OpenPlay y su valor en el mercado']
	
	kpi = st.selectbox('Selecciona una de las 6 predicciones que tenemos:', listKpi, key='start_station')#Creación de desplegable


	st.write('Has seleccionado el siguiente KPI: ' + str(kpi) + ' el cual se puede ver representado:')

	if str(kpi) == '0':
		st.write('aaa')
	elif str(kpi) == 'Tarjetas y minutos jugados siendo defensa':
		kpi_tajetas_defensas()
	elif str(kpi) == 'Equipos con mayor cantidad de goles marcados fuera de casa':
		kpi_goles_fueraCasa()	
	elif str(kpi) == 'Mediocentros y la proporcion de tarjetas en base a los minutos jugados':
		kpi_masMin_menosCards()
	elif str(kpi) == 'Equipos con mayor probabilidad de ganar en casa':
		kpi_ganar_casa()
	elif str(kpi) == 'Mayores goleadores por faltas directas':
		kpi_faltas_directas()
	elif str(kpi) == 'Mayores goleadores en OpenPlay y su valor en el mercado':
		kpi_max_goleadores()
		





 


# with header_container:
# 	# st.title("Proyecto Grandes Volumenes de Datos")
# 	# st.header("Bienvenido")
# 	# st.subheader("Autores: ")
# 	# st.write("Daniel Sabbagh, Manuel Salvador, Javier Taborda, Alfonso Vega")

# with stats_container:
	
# 	# text_input_container = st.empty()
# 	# t = text_input_container.text_input("Introduce tu Nombre")

# 	# if t != "":
# 	# 	text_input_container.empty()
# 	# 	st.info("Bienvenido "+t )

# 	# data = pd.read_csv(BytesIO(request_temp_ranges().content))
# 	# data = pd.read_csv('group_temp.csv')
# 	#st.write(data)

# 	# start_station_list = ['All'] + data['SECTOR'].unique().tolist()#Lista de Nombres de KPIs
# 	#end_station_list = ['All'] + data['end station name'].unique().tolist()


# 	listKpi = ['GASTO EN SALUD RESPECTO A TEMPERATURA','GASTO EN MODA RESPECTO A TEMPERATURAS NORMALES Y DRÁSTICAS','GASTO EN HOGAR RESPECTO A TEMPERATURAS','DÍA QUE SE GASTA MÁS EN ALIMENTACIÓN','GASTOS TOTALES POR SECTOR','GASTO TOTAL RESPECTO A TEMPERATURA']
	
# 	kpi = st.selectbox('Selecciona una de las 6 predicciones que tenemos:', listKpi, key='start_station')#Creación de desplegable


# 	st.write('Has seleccionado el KPI número: ' + str(kpi) + ' el cual se puede ver representado:')

# 	if str(kpi) == '0':
# 		st.write('aaa')
# 		#display_data = data[data['SECTOR'] == kpi]

# 	elif str(kpi) == 'GASTO EN SALUD RESPECTO A TEMPERATURA':
# 		kpi_salud_temp()
# 		#display_data = data.copy()
# 	elif str(kpi) == 'GASTO EN MODA RESPECTO A TEMPERATURAS NORMALES Y DRÁSTICAS':
# 		kpi_costo_moda_temp()
# 		#display_data = data.copy()
# 	elif str(kpi) == 'GASTO EN HOGAR RESPECTO A TEMPERATURAS':
# 		kpi_costo_hogar_bajas_altas()
# 		#display_data = data.copy()
# 	elif str(kpi) == 'DÍA QUE SE GASTA MÁS EN ALIMENTACIÓN':
# 		kpi_food_dayWeek()
# 		#display_data = data.copy()
# 	elif str(kpi) == 'GASTOS TOTALES POR SECTOR':
# 		kpi_gastos_sector()
# 	elif str(kpi) == 'GASTO TOTAL RESPECTO A TEMPERATURA':
# 		temperatura_gasto()
# 		#display_data = data.copy()

	