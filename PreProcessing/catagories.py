catagories = {
    0: [1, 2, 3],  # carrier
    1: [1, 2, 3, 4, 5, 6, 7, 8],  # origin
    2: [1, 2, 3, 4, 5, 6, 7, 8],  # dest
    3: [1, 2, 3, 4, 5, 6, 7, 8, 9],  # departure delay
    4: [1, 2, 3], # scheduled departure
    5: [1, 2, 3], # scheduled arrival
    6: [1, 2, 3, 4], # origin weather
    7: [1, 2, 3, 4], # destination weather
    8: [1, 2, 3, 4, 5, 6, 7, 8, 9]  # arrival delay
}

time_level_map = {
    '-inf_-60': 1,
    '-59_-30': 2,
    '-29_-10': 3,
    '-9_-2': 4,
    '-1_2': 5,
    '3_10': 6,
    '11_30': 7,
    '31_60': 8,
    '61_inf': 9
}

scheduled_map = {
    'busy': 1,
    'least_busy': 2,
    'most_busy': 3
}

busy_map = {
    '0800_1759': 'most_busy',
    '1800_0559': 'busy',
    '0600_0759': 'least_busy',
}

catagory_map = {
    'carriar': 0,
    'origin': 1,
    'dest': 2,
    'departure_delay': 3,
    'scheduled_departure': 4,
    'scheduled_arrival': 5,
    'origin_weather': 6,
    'dest_weather': 7,
    'arrival_delay': 8
}

weather_map = {
    'Rain': 1,
    'Snow': 2,
    'Windy': 3,
    'Sunny': 4
}

carrier_map = {
    'UA': 'Normal',
    'AA': 'Normal',
    'EV': 'ULLC',
    'B6': 'LCC',
    'DL': 'Normal',
    'OO': 'LCC',
    'F9': 'ULLC',
    'YV': 'ULLC',
    'US': 'Normal',
    'MQ': 'LCC',
    'HA': 'Normal',
    'AS': 'Normal',
    'FL': 'Normal',
    'VX': 'Normal',
    'WN': 'LCC',
    '9E': 'Normal',
    'NK': 'ULLC',
    'XE': 'LCC',
    'CO': 'Normal',
    'NW': 'Normal',
    'OH': 'Normal',
    'G4': 'ULLC',
    'YX': 'Normal'
}

carrier_type_map = {
    'ULCC': 1,
    'LCC': 2,
    'Normal': 3
}

region_map = {
    'NE': 1,
    'MNE': 2,
    'MNW': 3,
    'NW': 4,
    'SE': 5,
    'MSE': 6,
    'MSW': 7,
    'SW': 8
}