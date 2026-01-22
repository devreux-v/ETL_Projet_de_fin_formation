import pandas as pd
from shapely.geometry import Point, Polygon
import geopandas as gpd
import xml.etree.ElementTree as ET
import platform

import ETL_Functions
import NeTexToPandas as ntp

#++++++++++++++++++++++++
# datas
#++++++++++++++++++++++++

used_platform = platform.system()
if used_platform == 'Linux':
    file_separator = '/'
else :
    file_separator = '\\'

# xml tree
tree = ET.parse('datasets%sDatas%sepip-tec-bmc-latest.xml'%(file_separator, file_separator))
root = tree.getroot()

# geographical datas
path = 'datasets%sDatas%s201001.geojson'%(file_separator, file_separator)
geography = gpd.read_file(path)

# operating periods
periods = ntp.array_to_pandas(ntp.OperatingPeriod_to_array(root), {0: 'DayType_ID', 1: 'start', 2: 'end', 3: 'validDays'})
periods = ntp.ID_cleaning(periods, 'DayType_ID')

# stops
stops = ntp.array_to_pandas(ntp.stopPlaces_to_array(root), {0: 'Name', 1: 'Coordinatesx', 2: 'Coordinatesy', 3: 'SK_stop', 4: 'type'})
stops = ntp.ID_cleaning(stops,'SK_stop')
stops['Localite'] = stops['Name'].str.split(' ').str[0]

# journey
journey = ntp.array_to_pandas(ntp.journey_to_array(root), {0: 'DayType_ID', 1: 'ArrivalTime', 2: 'DepartureTime', 3: 'Journey_ID', 4: 'StopPoint_ref'})
journey = ntp.ID_cleaning(journey,'DayType_ID')
journey = ntp.ID_cleaning(journey,'Journey_ID')

# asso journey/stops
asso_StopPoint_StopPlace = ntp.array_to_pandas(ntp.asso_StopPoint_StopPlace(root), {0: 'StopPoint_ID', 1: 'SK_stop'})
asso_StopPoint_StopPlace = ntp.ID_cleaning(asso_StopPoint_StopPlace,'StopPoint_ID')
asso_StopPoint_StopPlace = ntp.ID_cleaning(asso_StopPoint_StopPlace,'SK_stop')

# asso journey/line (see below 'lines dimension')
asso_journeyPattern_line = ntp.array_to_pandas(ntp.asso_journeyPattern_line_to_array(root), {0: 'StopPoint_ID', 1: 'SK_lines',2: 'order'})
asso_journeyPattern_line = ntp.ID_cleaning(asso_journeyPattern_line,'StopPoint_ID')
asso_journeyPattern_line = ntp.ID_cleaning(asso_journeyPattern_line,'SK_lines')

#++++++++++++++++++++++++
# stops dimension 
#++++++++++++++++++++++++

geostops = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops.Coordinatesx, stops.Coordinatesy), crs="EPSG:2154")
geostops = geostops.to_crs(crs=4326)
stops_clean = ETL_Functions.generate_DIM_stops(geostops, geography)

stops_json = stops_clean.to_json()
with open('datasets%sDatas%sDIM_stops.geojson'%(file_separator, file_separator), "w") as f:
    f.write(stops_json)

#++++++++++++++++++++++++
# lines dimension
#++++++++++++++++++++++++

lines = ntp.array_to_pandas(ntp.lines_to_array(root), {0: 'Name', 1: 'SK_lines', 2: 'type', 3: 'code'})
lines = ntp.ID_cleaning(lines,'SK_lines')
lines.to_csv('datasets%sDatas%sDIM_lines.csv'%(file_separator, file_separator))

#++++++++++++++++++++++++
# date dimension
#++++++++++++++++++++++++

dates = ETL_Functions.generate_DIM_date (periods)
dates.to_csv('datasets%sDatas%sDIM_date.csv'%(file_separator, file_separator))

#++++++++++++++++++++++++
# geography dimension
#++++++++++++++++++++++++

just_commune = geography.where(geography['type_entite'] == 'Commune')
just_commune = just_commune.dropna(subset=['type_entite'])
just_commune = just_commune[['arr_name_fr', 'prov_name_fr', 'ins', 'entite', 'geometry']]

just_commune_json = just_commune.to_json()
with open('datasets%sDatas%sDIM_geo.geojson'%(file_separator, file_separator), "w") as f:
    f.write(just_commune_json)

#++++++++++++++++++++++++
# population fact
#++++++++++++++++++++++++

path = 'datasets%sDatas%spopulation_wallonie.csv'%(file_separator, file_separator)
population = gpd.read_file(path)
population = population.where(population['Type d\'entité'] == 'Commune')
population = population.dropna(subset=['Type d\'entité'])
population = population[['Code INS', 'Entité administrative', 'Total population']]
population = population.rename (columns={'Code INS': 'ins', 'Entité administrative' : 'entite', 'Total population' : 'population'})
population.to_csv(('datasets%sDatas%sFACT_population.csv'%(file_separator, file_separator)))

#++++++++++++++++++++++++
# asso date StopInJourney fact
#++++++++++++++++++++++++

asso_date_StopInJourney  = ETL_Functions.generate_FACT_asso_date_StopInJourney(periods)
asso_date_StopInJourney.to_csv(('datasets%sDatas%sasso_date_StopInJourney.csv'%(file_separator, file_separator)))

#++++++++++++++++++++++++
# StopInJourney Fact
#++++++++++++++++++++++++

StopInJourney = ETL_Functions.generate_FACT_StopInJourney (periods, journey, asso_StopPoint_StopPlace, asso_journeyPattern_line, stops_clean)
StopInJourney.to_csv(('datasets%sDatas%sStopInJourney.csv'%(file_separator, file_separator)))


