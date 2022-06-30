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


# st.markdown("""
# <nav class="navbar fixed-top  navbar-expand-sm navbar-dark" style="background-color: #3498DB, z-index:300  padding-top: 100px">
#   <img src="https://1000marcas.net/wp-content/uploads/2019/12/UEM-Logo.png" alt="uem" width="6%">
#   <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
#     <span class="navbar-toggler-icon"></span>
#   </button>
#   <div class="collapse navbar-collapse" id="navbarNav">
#     <ul class="navbar-nav">
#       <li class="nav-item active">
#         <a class="nav-link disabled" href="#">Home<span class="sr-only">(current)</span></a>
#       </li>
#     </ul>
#   </div>
# </nav>
# """, unsafe_allow_html=True)


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
sectors = ['SALUD', 'ALIMENTACION', 'AUTO', 'BELLEZA', 'HOGAR', 'MODA Y COMPLEMENTOS', 'OCIO Y TIEMPO LIBRE', 'OTROS', 'RESTAURACIÓN', 'TECNOLOGIA']
temp_ranges = ['T 30-3499', 'T 25-2999', 'T 20-2499', 'T 15-1999', 'T 10-1499', 'T 0-999']
def kpi_salud_temp():
	request_temp_ranges()
	display_health_data = []
	importes = []

	#KPI SALUD IMPORTE
	for temp_range in temp_ranges:
		dataframeKPI = data[data[temp_range]==True][[col_name for col_name in data if col_name == 'SECTOR' or col_name == 'sum(IMPORTE)']]
		num = dataframeKPI.loc[dataframeKPI['SECTOR'] == 'SALUD']
		importes.append(num['sum(IMPORTE)'].values[0])
		display_health_data.append((temp_range, num['sum(IMPORTE)'].values[0]))

	
	rangos = ["T 0-9,99", "T 10-14,99", "T 15-19,99", "T 20-24,99", "T 25-29,99", "T 30-34,99"]	
	source = pd.DataFrame({
		'Temperaturas en grados': rangos,
		'Importes': importes
	})
	c = alt.Chart(source).mark_bar().encode(
		x='Temperaturas en grados',
		y='Importes'
	).properties(width=600, height=500)
	st.write('Gasto en salud respecto a la temperatura')
	col1, col2 = st.columns([5, 2])
	col1.altair_chart(c)
	col2.write(source)

def kpi_costo_moda_temp():
	costo_drastico = []
	costo_normal = []
	for temp_range in temp_ranges:
		if temp_range == 'T 30-3499' or temp_range == 'T 25-2999' or temp_range == 'T 10-1499' or temp_range == 'T 0-999':
			dataframeKPI = data[data[temp_range]==True][[col_name for col_name in data if col_name == 'SECTOR' or col_name == 'sum(IMPORTE)']]
			num = dataframeKPI.loc[dataframeKPI['SECTOR'] == 'MODA Y COMPLEMENTOS']
			costo_drastico.append(num['sum(IMPORTE)'].values[0]) 
			
			
		elif temp_range == 'T 20-2499' or temp_range == 'T 15-1999':
			dataframeKPI = data[data[temp_range]==True][[col_name for col_name in data if col_name == 'SECTOR' or col_name == 'sum(IMPORTE)']]
			num = dataframeKPI.loc[dataframeKPI['SECTOR'] == 'MODA Y COMPLEMENTOS']
			costo_normal.append(num['sum(IMPORTE)'].values[0])


	# source = pd.DataFrame({
	# 	'Temperaturas': temp_ranges,
	# 	'Importes': 
	# })
	importe_drastico = sumar_lista(costo_drastico)
	importe_normal = sumar_lista(costo_normal)
	
	st.write('COSTO NORMAL')
	st.write(importe_normal)
	st.write('COSTO DRASTICO')
	st.write(importe_drastico)

	temps_data = {'Temperatura': ['Normal', 'Drastica'], 'Importe': [importe_normal, importe_drastico]}  
	source1 = pd.DataFrame(temps_data)
	#st.write(source1)
	#pie = alt.Chart(source1).mark_arc().encode(
    #theta=alt.Theta(field="Importe", type="quantitative"),
    #color=alt.Color(field="Temperatura", type="nominal")
	#)
	#st.altair_chart(pie)	
	c = alt.Chart(source1).mark_bar().encode(
		x='Temperatura',
		y='Importe'
	)
	st.write('Comparación gasto en moda temperaturas normales y drásticas')
	st.altair_chart(c)



def temperatura_gasto():
	url = base_url + '/expenditure?query_by=by_day'
	r = requests.get(url)
	url2 = base_url + '/temp_range?query_by=by_day'
	x = requests.get(url2)
	#bar_col, pie_col = st.columns(2)

	data1 = pd.read_csv(BytesIO(r.content))
	data2 = pd.read_csv(BytesIO(x.content))
	gasto_diario = []
	tiempo_diario = []

	for importe in data1['sum(IMPORTE)']:
		gasto_diario.append(importe)
	
	for tiempo in data2['sum(TMed)']:
		tiempo_diario.append(tiempo)


	fig, ax1 = plt.subplots()
	t = np.arange(1, 365, 1)
	color = 'tab:red'
	ax1.set_xlabel('Dia') 
	ax1.set_ylabel('Importe total', color=color)
	ax1.plot(t, gasto_diario, color=color)
	ax1.tick_params(axis='y', labelcolor=color)

	ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

	color = 'tab:blue'
	ax2.set_ylabel('Temperatura media del dia', color=color)  # we already handled the x-label with ax1
	ax2.plot(t, tiempo_diario, color=color)
	ax2.tick_params(axis='y', labelcolor=color)

	fig.tight_layout()  # otherwise the right y-label is slightly clipped
	st.pyplot(fig)



def kpi_costo_hogar_bajas_altas():
#ERROR POR LA TEMPERATURA 
#NO SABEMOS EXACTAMENTE QUE ES
	
	temps_bajas= []
	temps_altas = []
	for temp_range in temp_ranges:
		if temp_range == 'T 10-1499' or temp_range == 'T 0-999':
			dataframeKPI = data[data[temp_range]==True][[col_name for col_name in data if col_name == 'SECTOR' or col_name == 'sum(IMPORTE)']]			
			num = dataframeKPI.loc[dataframeKPI['SECTOR'] == 'HOGAR']
			temps_bajas.append(num['sum(IMPORTE)'].values[0])
		elif temp_range == 'T 30-3499' or temp_range == 'T 25-2999':
			dataframeKPI = data[data[temp_range]==True][[col_name for col_name in data if col_name == 'SECTOR' or col_name == 'sum(IMPORTE)']]			
			num = dataframeKPI.loc[dataframeKPI['SECTOR'] == 'HOGAR']
			temps_altas.append(num['sum(IMPORTE)'].values[0])
	
	labels = ['T. Altas', 'T. Bajas']
	gastos = [sumar_lista(temps_altas), sumar_lista(temps_bajas)]
	temps_data = {'Temperatura': labels, 'Gasto': gastos}  

	explode = (0, 0)
	fig1, ax1 = plt.subplots()
	ax1.pie(gastos, explode=explode, labels=labels, autopct='%2.1f%%', radius= 3,
        startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	col1, col2 = st.columns([5, 2])
	col1.pyplot(fig1)
	col2.write(pd.DataFrame(temps_data))
	
def kpi_food_dayWeek():
	data_food = request_food()
	st.write(data_food)
	dias_semana= ['lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo']
	costo_semana = []
	for costo in data_food['sum(IMPORTE)']:
		costo_semana.append(costo)
	st.write(costo_semana)
	st.write('Gasto semanal en alimentacion')
	st.write(costo_semana)

	y = costo_semana

	explode = (0, 0, 0, 0, 0, 0, 0)
	fig1, ax1 = plt.subplots()
	ax1.pie(costo_semana, explode=explode, labels=dias_semana, autopct='%2.1f%%', radius= 3,
        startangle=90)
	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
	st.pyplot(fig1)
	
	
def request_temp_ranges():
	temp_url = base_url + '/temp_range?query_by=expenditure' 
	r = requests.get(temp_url)
	return r
def request_food():
	food_url = base_url + '/sector?query_by=food_week'
	r = requests.get(food_url)
	dataframe = pd.read_csv(BytesIO(r.content))
	return dataframe

def kpi_gastos_sector():
	
	url = base_url + '/expenditure?query_by=by_sector'
	r = requests.get(url)
	df1 = pd.read_csv(BytesIO(r.content))
	st.write(df1)
	#st.write(df1)
	sectores = []
	importes = []
	for x in df1['sum(IMPORTE)']:
		importes.append(x)

	for y in df1['SECTOR']:
		sectores.append(y)

	chart_data = pd.DataFrame(importes, sectores)
	st.bar_chart(chart_data)

def kpi_tajetas_defensas():

	df1 = pd.read_csv('export9.csv')
	st.write(df1)


def sumar_lista(lista):
	suma = 0

	for numero in lista:
		suma += numero
	return suma
	
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
	st.write("Ha sido realizado por: **Manuel Salvador y Javier Taborda**")
	st.write("Este proyecto representa la práctica final de la asignatura de Grandes Volumenes de Datos.", 
	"En este proyecto se pretende obtener información de valor sobre los jugadores de futbol y su valor en el mercado,", 
	"A lo largo de esta práctica se desarrollan diferentes KPIs los cuales creemos que aporta información muy relevante al usuario sobre los diferentes jugadores y sus valores", 
	"Para esta práctica se han usado los siguientes datasets: ")
	st.write("https://www.kaggle.com/davidcariboo/player-scores")

	# st.header("Bienvenido")
	# st.subheader("Autores: ")
if selected == "KPIs":
	st.title(f"Bienvenido!")
	# option = st.selectbox(
    #  'Elija uno de los siguientes KPIs para poder visualizarlos',
    #  ('1', '2', '3', '4', '5', '6', '7'))

	# st.write('You selected:', option)

	# data = pd.read_csv(BytesIO(request_temp_ranges().content))
	# data = "Hola"
	# # pd.read_csv('group_temp.csv')
	# st.write(data)

	# start_station_list = ['All'] + data['SECTOR'].unique().tolist()#Lista de Nombres de KPIs
	# end_station_list = ['All'] + data['end station name'].unique().tolist()
	listKpi = ['Tarjetas y minutos jugados siendo defensa','GASTO EN MODA RESPECTO A TEMPERATURAS NORMALES Y DRÁSTICAS','GASTO EN HOGAR RESPECTO A TEMPERATURAS','DÍA QUE SE GASTA MÁS EN ALIMENTACIÓN','GASTOS TOTALES POR SECTOR','GASTO TOTAL RESPECTO A TEMPERATURA']
	
	kpi = st.selectbox('Selecciona una de las 6 predicciones que tenemos:', listKpi, key='start_station')#Creación de desplegable


	st.write('Has seleccionado el siguiente KPI: ' + str(kpi) + ' el cual se puede ver representado:')

	if str(kpi) == '0':
		st.write('aaa')
		#display_data = data[data['SECTOR'] == kpi]

	elif str(kpi) == 'Tarjetas y minutos jugados siendo defensa':
		kpi_tajetas_defensas()
		# kpi_salud_temp()
		#display_data = data.copy()
	elif str(kpi) == 'GASTO EN MODA RESPECTO A TEMPERATURAS NORMALES Y DRÁSTICAS':
		kpi_tajetas_defensas()
		# kpi_costo_moda_temp()

		#display_data = data.copy()
	elif str(kpi) == 'GASTO EN HOGAR RESPECTO A TEMPERATURAS':
		kpi_tajetas_defensas()
		# kpi_costo_hogar_bajas_altas()
		#display_data = data.copy()
	elif str(kpi) == 'DÍA QUE SE GASTA MÁS EN ALIMENTACIÓN':
		kpi_tajetas_defensas()
		# kpi_food_dayWeek()
		#display_data = data.copy()
	elif str(kpi) == 'GASTOS TOTALES POR SECTOR':
		kpi_tajetas_defensas()
		# kpi_gastos_sector()
	elif str(kpi) == 'GASTO TOTAL RESPECTO A TEMPERATURA':
		kpi_tajetas_defensas()
		# temperatura_gasto()
		#display_data = data.copy()





 


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

	