import "strings"
import "regexp"

option task = { 
  name: "powerwall => plus",
  every: 1m,
  start: -5m
}

data = from(bucket: "powerwall")
    |> range(start: task.start)
    |> filter(fn: (r) => r["_measurement"] == "http")
    |> drop(columns: ["month", "year", "url"])
    |> set(key: "_measurement", value: "plus")

inverter = data
    |> filter(fn: (r) =>r["_field"] == "D_Power" or r["_field"] == "C_Power" or r["_field"] == "B_Power" or r["_field"] == "A_Power")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({r with _value: r.A_Power + r.B_Power + r.C_Power + r.D_Power}))
    |> drop(columns: ["A_Power", "B_Power", "C_Power", "D_Power"])
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> set(key: "_field", value: "inverter")

inverter1 = data
    |> filter(fn: (r) => r["_field"] == "D_Power1" or r["_field"] == "C_Power1" or r["_field"] == "B_Power1" or r["_field"] == "A_Power1")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({r with _value: r.A_Power1 + r.B_Power1 + r.C_Power1 + r.D_Power1}))
    |> drop(columns: ["A_Power1", "B_Power1", "C_Power1", "D_Power1"])
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> set(key: "_field", value: "inverter1")

inverter2 = data
    |> filter(fn: (r) => r["_field"] == "D_Power2" or r["_field"] == "C_Power2" or r["_field"] == "B_Power2" or r["_field"] == "A_Power2")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({r with _value: r.A_Power2 + r.B_Power2 + r.C_Power2 + r.D_Power2}))
    |> drop(columns: ["A_Power2", "B_Power2", "C_Power2", "D_Power2"])
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> set(key: "_field", value: "inverter2")

inverter3 = data
    |> filter(fn: (r) => r["_field"] == "D_Power3" or r["_field"] == "C_Power3" or r["_field"] == "B_Power3" or r["_field"] == "A_Power3")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> map(fn: (r) => ({r with _value: r.A_Power3 + r.B_Power3 + r.C_Power3 + r.D_Power3}))
    |> drop(columns: ["A_Power3", "B_Power3", "C_Power3", "D_Power3"])
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> set(key: "_field", value: "inverter3")

inverters = union(tables: [inverter, inverter1, inverter2, inverter3])
    |> to(bucket: "powerwall")

temps = data
    |> filter(fn: (r) => r["_field"] == "PW1_temp" or r["_field"] == "PW2_temp" or r["_field"] == "PW3_temp" or r["_field"] == "PW4_temp" or r["_field"] == "PW5_temp" or r["_field"] == "PW6_temp" or r["_field"] == "PW7_temp" or r["_field"] == "PW8_temp")
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /(PW[1-8])_temp/, t: "temp_$1") }))
    |> to(bucket: "powerwall")

pwFreq = data
    |> filter(fn: (r) => r["_field"] == "PW1_PINV_Fout"  or r["_field"] == "PW2_PINV_Fout"  or r["_field"] == "PW3_PINV_Fout"  or r["_field"] == "PW4_PINV_Fout"  or r["_field"] == "PW5_PINV_Fout"  or r["_field"] == "PW6_PINV_Fout" or r["_field"] == "PW7_PINV_Fout" or r["_field"] == "PW8_PINV_Fout")
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /(PW[1-8])_PINV_Fout/, t: "freq_$1") }))
    |> to(bucket: "powerwall")

homeFreq = data
    |> filter(fn: (r) => r["_field"] == "ISLAND_FreqL1_Load"  or r["_field"] == "ISLAND_FreqL2_Load"  or r["_field"] == "ISLAND_FreqL3_Load")
    |> filter(fn: (r) => r["_value"] > 0)
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /ISLAND_FreqL([1-3])_Load/, t: "freq_home_L$1") }))
    |> to(bucket: "powerwall")

gridFreq = data
    |> filter(fn: (r) => r["_field"] == "ISLAND_FreqL1_Main"  or r["_field"] == "ISLAND_FreqL2_Main"  or r["_field"] == "ISLAND_FreqL3_Main")
    |> filter(fn: (r) => r["_value"] > 0)
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /ISLAND_FreqL([1-3])_Main/, t: "freq_grid_L$1") }))
    |> to(bucket: "powerwall")

pwVolt = data
  |> filter(fn: (r) => r["_field"] == "PW1_PINV_VSplit1"  or r["_field"] == "PW2_PINV_VSplit1"  or r["_field"] == "PW3_PINV_VSplit1"  or r["_field"] == "PW4_PINV_VSplit1"  or r["_field"] == "PW5_PINV_VSplit1"  or r["_field"] == "PW6_PINV_VSplit1" or r["_field"] == "PW7_PINV_VSplit1" or r["_field"] == "PW8_PINV_VSplit1" or r["_field"] == "PW1_PINV_VSplit2"  or r["_field"] == "PW2_PINV_VSplit2" or r["_field"] == "PW3_PINV_VSplit2"  or r["_field"] == "PW4_PINV_VSplit2"  or r["_field"] == "PW5_PINV_VSplit2"  or r["_field"] == "PW6_PINV_VSplit2" or r["_field"] == "PW7_PINV_VSplit2" or r["_field"] == "PW8_PINV_VSplit2" or r["_field"] == "PW1_PINV_VSplit3"  or r["_field"] == "PW2_PINV_VSplit3"  or r["_field"] == "PW3_PINV_VSplit3"  or r["_field"] == "PW4_PINV_VSplit3"  or r["_field"] == "PW5_PINV_VSplit3"  or r["_field"] == "PW6_PINV_VSplit3" or r["_field"] == "PW7_PINV_VSplit3" or r["_field"] == "PW8_PINV_VSplit3")
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: "volt_" + strings.replace(v: r._field, t: "_PINV_VSplit", u: "_L", i: 1) }))
    |> to(bucket: "powerwall")

homeGridVolt = data
    |> filter(fn: (r) => r["_field"] == "ISLAND_VL1N_Load"  or r["_field"] == "ISLAND_VL2N_Load"  or r["_field"] == "ISLAND_VL3N_Load"  or r["_field"] == "METER_X_VL1N"  or r["_field"] == "METER_X_VL2N"  or r["_field"] == "METER_X_VL3N" )
    |> filter(fn: (r) => r["_value"] > 0)
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /ISLAND_VL([1-3])N_Load/, t: "volt_home_L$1") }))
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /METER_X_VL([1-3])N/, t: "volt_grid_L$1") }))
    |> to(bucket: "powerwall")

pwCapacityCharge = data
    |> filter(fn: (r) => r["_field"] == "PW1_POD_nom_full_pack_energy"  or r["_field"] == "PW2_POD_nom_full_pack_energy"  or r["_field"] == "PW3_POD_nom_full_pack_energy"  or r["_field"] == "PW4_POD_nom_full_pack_energy"  or r["_field"] == "PW5_POD_nom_full_pack_energy"  or r["_field"] == "PW6_POD_nom_full_pack_energy" or r["_field"] == "PW7_POD_nom_full_pack_energy" or r["_field"] == "PW8_POD_nom_full_pack_energy")
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /(PW[1-8])_POD_nom_full_pack_energy/, t: "charge_capacity_$1") }))
    |> to(bucket: "powerwall")

pwRemainingCharge = data
    |> filter(fn: (r) => r["_field"] == "PW1_POD_nom_energy_remaining"  or r["_field"] == "PW2_POD_nom_energy_remaining"  or r["_field"] == "PW3_POD_nom_energy_remaining"  or r["_field"] == "PW4_POD_nom_energy_remaining"  or r["_field"] == "PW5_POD_nom_energy_remaining"  or r["_field"] == "PW6_POD_nom_energy_remaining" or r["_field"] == "PW7_POD_nom_energy_remaining" or r["_field"] == "PW8_POD_nom_energy_remaining")
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /(PW[1-8])_POD_nom_energy_remaining/, t: "charge_remaining_$1") }))
    |> to(bucket: "powerwall")

stringVolt = data
    |> filter(fn: (r) => r["_field"] == "D_Voltage" or r["_field"] == "C_Voltage" or r["_field"] == "B_Voltage" or r["_field"] == "A_Voltage" or r["_field"] == "D1_Voltage" or r["_field"] == "C1_Voltage" or r["_field"] == "B1_Voltage" or r["_field"] == "A1_Voltage" or r["_field"] == "D2_Voltage" or r["_field"] == "C2_Voltage" or r["_field"] == "B2_Voltage" or r["_field"] == "A2_Voltage" or r["_field"] == "D3_Voltage" or r["_field"] == "C3_Voltage" or r["_field"] == "B3_Voltage" or r["_field"] == "A3_Voltage")
    |> filter(fn: (r) => r._value > 0)
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /([A-D][1-3]?)_Voltage/, t: "string_volt_$1") }))
    |> to(bucket: "powerwall")

stringCurrent = data
    |> filter(fn: (r) => r["_field"] == "D_Current" or r["_field"] == "C_Current" or r["_field"] == "B_Current" or r["_field"] == "A_Current" or r["_field"] == "D1_Current" or r["_field"] == "C1_Current" or r["_field"] == "B1_Current" or r["_field"] == "A1_Current" or r["_field"] == "D2_Current" or r["_field"] == "C2_Current" or r["_field"] == "B2_Current" or r["_field"] == "A2_Current" or r["_field"] == "D3_Current" or r["_field"] == "C3_Current" or r["_field"] == "B3_Current" or r["_field"] == "A3_Current")
    |> filter(fn: (r) => r._value > 0)
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /([A-D][1-3]?)_Current/, t: "string_current_$1") }))
    |> to(bucket: "powerwall")

stringPower = data
    |> filter(fn: (r) => r["_field"] == "D_Power" or r["_field"] == "C_Power" or r["_field"] == "B_Power" or r["_field"] == "A_Power" or r["_field"] == "D1_Power" or r["_field"] == "C1_Power" or r["_field"] == "B1_Power" or r["_field"] == "A1_Power" or r["_field"] == "D2_Power" or r["_field"] == "C2_Power" or r["_field"] == "B2_Power" or r["_field"] == "A2_Power" or r["_field"] == "D3_Power" or r["_field"] == "C3_Power" or r["_field"] == "B3_Power" or r["_field"] == "A3_Power")
    |> filter(fn: (r) => r._value > 0)
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> map(fn: (r) => ({ r with _field: regexp.replaceAllString(v: r._field, r: /([A-D][1-3]?)_Power/, t: "string_power_$1") }))
    |> to(bucket: "powerwall")

allInverters = inverters
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allTemps = temps
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> max()
    |> set(key: "host", value: "*")

allPwFreq = pwFreq
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> mean()
    |> set(key: "host", value: "*")

allHomeFreq = homeFreq
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> mean()
    |> set(key: "host", value: "*")

allGridFreq = gridFreq
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> mean()
    |> set(key: "host", value: "*")

allPwVolt = pwVolt
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allHomeGridVolt = homeGridVolt
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allPwCapacityCharge = pwCapacityCharge
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allPwRemainingCharge = pwRemainingCharge
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allStringVolt = stringVolt
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allStringCurrent = stringCurrent
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

allStringPower = stringPower
    |> group(columns: ["_time", "_measurement", "_field"], mode: "by")
    |> sum()
    |> set(key: "host", value: "*")

union(tables: [allInverters, allTemps, allPwFreq, allHomeFreq, allGridFreq, allPwVolt, allHomeGridVolt, allPwCapacityCharge, allPwRemainingCharge, allStringVolt, allStringCurrent, allStringPower])
    |> to(bucket: "powerwall")