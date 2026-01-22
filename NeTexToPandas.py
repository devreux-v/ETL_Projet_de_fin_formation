import pandas as pd

"""
Functions wich create an array (list of lists) from an XML file who uses 
the NeTex norm and can then transform the array in a Pandas DataFrame.

Note
----
This module uses ElementTree from xml.etree, the xml file need to be parsed before.
"""

def stopPlaces_to_array(root):
    """
    Return an array representing the stops.
    
    Parameter
    ---------
    root : root of the tree (Element)

    Return
    ------
    stops : array reprensenting the stops (list[list])

    Notes
    -----
    Each lists represent a column.
    Stucture : [place_name(str), coordinates(list), stop_ID(str), stop_type(str)]
    """
    place_name = []
    coordinatesx = []
    coordinatesy = []
    stop_ID = []
    stop_type = []
    
    for child in root[3][0][4][3][1]:
        place_name.append(child[0].text)
        full_coordinates = child[1][0][0].text.rsplit()
        coordinatesx.append(full_coordinates [0])
        coordinatesy.append(full_coordinates [1])
        stop_ID.append(child.attrib['id'])
        stop_type.append(child[3].text)

    stops = [place_name, coordinatesx, coordinatesy, stop_ID, stop_type]
    return stops


def lines_to_array(root):
    """
    Return an array representing the lines.
    
    Parameter
    ---------
    root : root of the tree (Element)

    Return
    ------
    stops : array reprensenting the lines (list[list])

    Notes
    -----
    Each lists represent a column.
    Stucture : [line_name(str), line_ID(str), line_type(str), line_code(str)]
    """
    line_name = []
    line_ID = []
    line_type = []
    line_code = []

    for child in root[3][0][4][2][1]:
        line_name.append(child[0].text)
        line_ID.append(child.attrib['id'])
        line_type.append(child[1].text)
        line_code.append(child[2].text)

    bus_stops = [line_name, line_ID, line_type, line_code]
    return bus_stops


def journey_to_array (root):
    """
    Return an array representing the journeys.
    
    Parameter
    ---------
    root : root of the tree (Element)

    Return
    ------
    stops : array reprensenting the journeys (list[list])

    Notes
    -----
    Each lists represent a column.
    Each line represent the stop of a transport at a specific time (high volume of datas)
    JourneyPatern_ID + StopPoint_ref = StopPoint_ID
    Stucture : [DayType_ID(str), ArrivalTime(str : hh:mm), DepartureTime(str : hh:mm), JourneyPatern_ID(str), StopPoint_ref(str)]
    """
    DayType_ID = []
    ArrivalTime = []
    DepartureTime = []
    JourneyPatern_ID = []
    StopPoint_ref = []

    for child in root[3][0][4][4][1]:
        ID = child.attrib['id']
        DayType = child[0][0].attrib['ref']
        JourneyPattern = child[1].attrib['ref']
        for timetable in child[3]:
            DayType_ID.append(DayType)
            ArrivalTime.append(timetable[1].text)
            DepartureTime.append(timetable[3].text)
            JourneyPatern_ID.append(JourneyPattern)
            StopPoint_ref.append(timetable[0].attrib['ref'])   
    journey_array = [DayType_ID, ArrivalTime, DepartureTime, JourneyPatern_ID, StopPoint_ref]
    return journey_array


def asso_journeyPattern_line_to_array(root):
    """
    Return an array representing the association between the journey and the lines.
    
    Parameter
    ---------
    root : root of the tree (Element)

    Return
    ------
    stops : array reprensenting the association (list[list])

    Notes
    -----
    Each lists represent a column.
    route_ID is the same as the line_ID with a different prefix (LANGUAGE:route: instead of LANGUAGE:line:)
    Stucture : [StopPoint_ID(str), route_ID(str), order(int)]
    """
    StopPoint_ID = []
    route_ID = []
    order = []

    for child in root[3][0][4][2][4]:
        route = child[1].attrib['ref']
        for pattern in child[2]:
            order.append(pattern.attrib['order'])
            route_ID.append(route)
            StopPoint_ID.append(pattern.attrib['id'])
    journeyPattern_line = [StopPoint_ID, route_ID, order]
    return journeyPattern_line


def OperatingPeriod_to_array(root):
    """
    Return an array representing the different type of days used by the public transport company.
    
    Parameter
    ---------
    root : root of the tree (Element)

    Return
    ------
    stops : array reprensenting the type of days (list[list])

    Notes
    -----
    Each lists represent a column.
    ValidDayBits is a sequence of bits representing each day on an operating period.
    Stucture : [OperatingPeriod_ID, StartDate, EndDate, ValidDayBits]
    """
    OperatingPeriod_ID = []
    StartDate = []
    EndDate = []
    ValidDayBits = []

    for child in root[3][0][4][1][1][3]:
        ValidDayBits.append(child[2].text)
        OperatingPeriod_ID.append(child.attrib['id'])
        StartDate.append(child[0].text)
        EndDate.append(child[1].text)

    OperatingPeriod = [OperatingPeriod_ID, StartDate, EndDate, ValidDayBits]
    return OperatingPeriod


def asso_StopPoint_StopPlace(root):
    """
    Return an array representing the association between the journey and the stops.    
    Parameter
    ---------
    root : root of the tree (Element)

    Return
    ------
    stops : array reprensenting the association (list[list])

    Notes
    -----
    Each lists represent a column.
    Stucture : [StopPoint_ID(str), stop_ID(str)]
    """
    StopPoint_ID = []
    stop_ID = []

    for child in root[3][0][4][2][3]:
        stop_ID.append(child[1].attrib['ref'])
        StopPoint_ID.append(child.attrib['id'])

    asso_StopPoint_StopPlace = [StopPoint_ID, stop_ID]
    return asso_StopPoint_StopPlace


def array_to_pandas(array, columns_names):
    """
    Takes an array generated by this module and make it a Pandas DataFrame.

    Parameter
    ---------
    array : array generated by this module (list[list])
    columns_names : dictionnary representing the columns names (dict)

    Return
    ------
    data_frame : DataFrame with the datas from the array (pandas Dataframe)

    Notes
    -----
    columns_names exemple : {0: 'place_name', 1: 'coordinates', 2: 'stop_ID', 3: 'stop_type'}
    """
    
    data_frame = pd.DataFrame(data=array)
    data_frame = data_frame.T
    data_frame = data_frame.rename(columns=columns_names)

    return data_frame


def ID_cleaning (table, to_clean):
    """
    Clean the IDs of the DAtaframe generated by this module.

    Parameter
    ---------
    table : array generated by this module (pandas Dataframe)
    to_clean : collumn of IDs to clean (str)

    Return
    ------
    data_frame : Dataframe with clean IDs (pandas Dataframe)
    """
    table[to_clean] = table[to_clean].str.split(':').str[2]
    return table

    