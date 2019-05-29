from bokeh.models import Button, Select, TextInput, DatePicker
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.io import curdoc
from bokeh.layouts import column, row
import geo_bd

textInput = TextInput()
date1 = DatePicker()
date2 = DatePicker()
textInput1 = TextInput()
textInput2 = TextInput()
columns_db = ['Phase',
              'Time',
              'Level',
              'Distance',
              'Azimuth',
              'Channel_station',
              'Network_station',
              ]
layout = column()
select_columns = Select(title="column:", value="None", options=["None"] + columns_db)
button_find = Button(label="Find", button_type="success", width=100)


def find_():
    if select_columns.value == 'None':
        return
    if select_columns.value == 'Channel_station' and len(textInput.value) > 0:
        res = geo_bd.search_channel(textInput.value)
        init_table_station(res)
    elif select_columns.value == 'Network_station' and len(textInput.value) > 0:
        res = geo_bd.search_network(textInput.value)
        init_table_station(res)
    elif select_columns.value == 'Phase' and len(textInput.value) > 0:
        res = geo_bd.search_phase(textInput.value)
        init_table(res)
    elif select_columns.value == 'Time':
        res = geo_bd.search_date_interval(date1, date2)
        init_table(res)
    elif select_columns.value == 'Distance' and len(textInput1.value) > 0 and len(textInput2.value) > 0 and int(
            textInput2.value) >= int(textInput1.value):
        res = geo_bd.search_distance_interval(textInput1.value, textInput2.value)
        init_table(res)
    elif select_columns.value == 'Azimuth' and len(textInput1.value) > 0 and len(textInput2.value) > 0 and int(
            textInput2.value) >= int(textInput1.value):
        res = geo_bd.search_azimuth_interval(textInput1.value, textInput2.value)
        init_table(res)
    elif select_columns.value == 'Level' and len(textInput1.value) > 0 and len(textInput2.value) > 0 and int(
            textInput2.value) >= int(textInput1.value):
        res = geo_bd.search_level_interval(textInput1.value, textInput2.value)
        init_table(res)


def main():
    select_columns.on_change('value', lambda attr, old, new: change_select(new))
    button_find.on_click(find_)
    layout.children = [button_find, select_columns]
    curdoc().add_root(layout)


def change_select(new):
    if new == 'None':
        layout.children = [button_find, select_columns]
    if new == 'Channel_station' or new == 'Network_station' or new == 'Phase':
        layout.children = [button_find, select_columns, textInput]
    elif new == 'Time':
        layout.children = [button_find, select_columns, row(date1, date2)]
    else:
        layout.children = [button_find, select_columns, row(textInput1, textInput2)]


def init_table(res):
    phase = [res[i][0] for i in range(len(res))]
    arrival_time = [res[i][1] for i in range(len(res))]
    level = [res[i][2] for i in range(len(res))]
    quality = [res[i][3] for i in range(len(res))]
    distance = [res[i][4] for i in range(len(res))]
    azimuth = [res[i][5] for i in range(len(res))]

    data = dict(
        phase=phase,
        time=arrival_time,
        level=level,
        quality=quality,
        distance=distance,
        azimuth=azimuth
    )
    source = ColumnDataSource(data)

    columns = [
        TableColumn(field="phase", title="Phase"),
        TableColumn(field="time", title="Time"),
        TableColumn(field="quality", title="Quality"),
        TableColumn(field="level", title="Level"),
        TableColumn(field="distance", title="Distance"),
        TableColumn(field="azimuth", title="Azimuth")
    ]
    data_table = DataTable(source=source, columns=columns, width=400, height=280)
    if len(layout.children) == 4:
        layout.children[3] = data_table
    else:
        layout.children += [data_table]


def init_table_station(res):
    channel = [res[i][3] for i in range(len(res))]
    network = [res[i][4] for i in range(len(res))]
    name_station = [res[i][5] for i in range(len(res))]
    update_time = [res[i][7] for i in range(len(res))]
    comment = [res[i][8] for i in range(len(res))]

    data = dict(
        channel=channel,
        network=network,
        name_station=name_station,
        update_time=update_time,
        comment=comment
    )
    source = ColumnDataSource(data)

    columns = [
        TableColumn(field="channel", title="Channel"),
        TableColumn(field="network", title="Network"),
        TableColumn(field="name_station", title="name_station"),
        TableColumn(field="update_time", title="update_time"),
        TableColumn(field="comment", title="Comment")
    ]
    data_table = DataTable(source=source, columns=columns, width=400, height=280)
    layout.children = [button_find, select_columns, textInput, data_table]


main()
