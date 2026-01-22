import pandas as pd
from shapely.geometry import Point, Polygon
import geopandas as gpd

"""
Module that takes Dataframe generated with NeTexToPandas and transorm them into a dimentional model.
"""

def generate_DIM_stops (stops, geography):
    """
    Return a dataframe representing the stops dimention.
    
    Parameter
    ---------
    stops : stops dataframe (Dataframe)
    geography : dataframe from geojson https://www.odwb.be/explore/embed/dataset/201001/table/ (Dataframe)

    Return
    ------
    stopsclean : the stops dimention (dataframe)
    """
    # remove duplicate
    stopsclean = stops.where((stops['type'] == 'bus') | (stops['type'] == 'tram'))
    stopsclean = stopsclean.dropna()

    # sort datas to get only the communes
    just_commune = geography.where(geography['type_entite'] == 'Commune')
    just_commune = just_commune.dropna(subset=['type_entite'])

    # change geographical code to lat/long
    stopsclean = gpd.GeoDataFrame (stopsclean, geometry=gpd.points_from_xy(stopsclean.Coordinatesx, stopsclean.Coordinatesy), crs="EPSG:2154")
    stopsclean = stopsclean.to_crs(crs=4326)

    stopsclean['latitude'] = stopsclean['geometry'].x
    stopsclean['longitude'] = stopsclean['geometry'].y

    stopsclean = stopsclean.sjoin(just_commune)
    return stopsclean[['SK_stop', 'Name', 'type', 'Localite', 'ins', 'latitude', 'longitude', 'geometry']]


def generate_DIM_date (periods):
    """
    Return a dataframe representing the date dimention.
    
    Parameter
    ---------
    periods : periods dataframe (Dataframe)

    Return
    ------
    date_table : the date dimention (dataframe)
    """

    start = pd.to_datetime(periods['start']).min().tz_localize(None)
    end = pd.to_datetime(periods['end']).max().tz_localize(None)

    date_table = pd.DataFrame({"date": pd.date_range(start, end)})
    date_table["week_day"] = date_table.date.dt.dayofweek
    date_table["day_name"] = date_table.date.dt.day_name()
    date_table["day"] = date_table.date.dt.day
    date_table["week"] = date_table.date.dt.isocalendar().week
    date_table["month"] = date_table.date.dt.month
    date_table["month_name"] = date_table.date.dt.month_name()
    date_table["quarter"] = date_table.date.dt.quarter
    date_table["year"] = date_table.date.dt.year
    date_table.insert(0, 'date_id', (date_table.year.astype(str) + date_table.month.astype(str).str.zfill(2) + date_table.day.astype(str).str.zfill(2)).astype(int))

    return date_table

def generate_FACT_asso_date_StopInJourney (periods) :

    """
    Return a dataframe representing the fact many to many from the stop in journey fact and the date dimention.
    
    Parameter
    ---------
    periods : periods dataframe (Dataframe)

    Return
    ------
    oparating_period_df: the fact many to many from the stop in journey fact and the date dimention (dataframe)
    """

    oparating_period_list = []
    for row in periods.iterrows():
        start = pd.to_datetime(row[1]['start']).tz_localize(None)
        end = pd.to_datetime(row[1]['end']).tz_localize(None)
        index = 0

        for day in pd.date_range(start, end):
            if index < len(row[1]['validDays']):
                if row[1]['validDays'][index] == '1':
                    valid_row = row[1].copy()
                    datekey = pd.Series(day,index=['Datekey'])
                    valid_row = pd.concat([valid_row, datekey])
                    oparating_period_list.append(valid_row)

                index += 1
    oparating_period_df = pd.DataFrame(oparating_period_list)
    oparating_period_df = oparating_period_df[['DayType_ID', 'Datekey']]
    print(oparating_period_df)
    return oparating_period_df


def generate_FACT_StopInJourney (periods, journey, asso_StopPoint_StopPlace, asso_journeyPattern_line, stops):
    """
    Return a dataframe representing the fact stop in journey.
    
    Parameter
    ---------
    periods : periods dataframe (Dataframe)
    journey : journey dataframe (Dataframe)
    asso_StopPoint_StopPlace : asso_StopPoint_StopPlace dataframe (Dataframe)
    asso_journeyPattern_line : asso_journeyPattern_line dataframe (Dataframe)
    stops : stops dimention dataframe (Dataframe)

    Return
    ------
    stop in journey : the fact stop in journey (dataframe)
    """

    journey['StopPoint_ID'] = journey['Journey_ID'] + journey['StopPoint_ref']
    # join for stops
    stop_in_journey = journey.merge(asso_StopPoint_StopPlace, how='inner', on='StopPoint_ID')
    stop_in_journey = stop_in_journey.merge(stops, how='inner', on='SK_stop')
    stop_in_journey = stop_in_journey[['StopPoint_ID', 'ArrivalTime', 'SK_stop', 'DayType_ID', 'ins']]
    # join for lines
    stop_in_journey = stop_in_journey.merge(asso_journeyPattern_line, how='inner', on='StopPoint_ID')
    stop_in_journey = stop_in_journey[['StopPoint_ID', 'ArrivalTime', 'SK_stop', 'DayType_ID', 'SK_lines', 'ins']]
    # join for periods 
    stop_in_journey = stop_in_journey.merge(periods, how='inner', on='DayType_ID')
    stop_in_journey = stop_in_journey[['StopPoint_ID', 'ArrivalTime', 'SK_stop', 'DayType_ID', 'SK_lines', 'ins', 'DayType_ID']]

    return stop_in_journey
    

    