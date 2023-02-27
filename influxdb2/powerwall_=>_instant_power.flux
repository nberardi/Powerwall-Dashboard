import "math"
import "date"

option task = { 
  name: "powerwall => instant_power",
  every: 1m,
  start: -1h
}

data = from(bucket: "powerwall")
    |> range(start: task.start)
    |> filter(fn: (r) => r["_measurement"] == "http")
    |> filter(fn: (r) => r["_field"] == "battery_instant_power" or r["_field"] == "load_instant_power" or r["_field"] == "site_instant_power" or r["_field"] == "solar_instant_power" or r["_field"] == "percentage" or r["_field"] == "grid_status")
    |> drop(columns: ["month", "year", "url"])
    |> set(key: "_measurement", value: "instant_power")

home = data
    |> filter(fn: (r) => r["_field"] == "load_instant_power")
    |> set(key: "_field", value: "home")

solar =data
    |> filter(fn: (r) => r["_field"] == "solar_instant_power")
    |> set(key: "_field", value: "solar")

grid = data
    |> filter(fn: (r) => r["_field"] == "site_instant_power")
    |> set(key: "_field", value: "grid")

battery = data
    |> filter(fn: (r) => r["_field"] == "battery_instant_power")
    |> set(key: "_field", value: "battery")

toGrid = data
    |> filter(fn: (r) => r._field == "site_instant_power")
    |> map(fn: (r) => ({r with _value: math.abs(x: (1.0 - r._value / math.abs(x: r._value)) * r._value / 2.0) }) )
    |> set(key: "_field", value: "to_grid")

fromGrid = data
    |> filter(fn: (r) => r._field == "site_instant_power")
    |> map(fn: (r) => ({r with _value: math.abs(x: (1.0 + r._value / math.abs(x: r._value)) * r._value / 2.0) }) )
    |> set(key: "_field", value: "from_grid")

toPowerwall = data
    |> filter(fn: (r) => r._field == "battery_instant_power")
    |> map(fn: (r) => ({r with _value: math.abs(x: (1.0 - r._value / math.abs(x: r._value)) * r._value / 2.0) }) )
    |> set(key: "_field", value: "to_powerwall")

fromPowerwall = data
    |> filter(fn: (r) => r._field == "battery_instant_power")
    |> map(fn: (r) => ({r with _value: math.abs(x: (1.0 + r._value / math.abs(x: r._value)) * r._value / 2.0) }) )
    |> set(key: "_field", value: "from_powerwall")

percentage = data
    |> filter(fn: (r) => r["_field"] == "percentage")
    |> set(key: "_field", value: "percentage")

gridStatus = data
    |> filter(fn: (r) => r["_field"] == "grid_status")
    |> set(key: "_field", value: "grid_status")

outputIndividual = union(tables: [home,solar,grid,battery,toGrid,fromGrid,toPowerwall,fromPowerwall,percentage, gridStatus])
    |> aggregateWindow(every: task.every, fn: mean, createEmpty: false)
    |> filter(fn: (r) => not math.isNaN(f: r._value))
    |> to(bucket: "powerwall")

outputAllExceptPercentage = outputIndividual
    |> filter(fn: (r) => r._field != "percentage")
    |> filter(fn: (r) => r._field != "grid_status")
    |> group(columns: ["_time", "_measurement", "_field"], mode:"by")
    |> sum()
    |> set(key: "host", value: "*")

outputAllPercentage = outputIndividual
    |> filter(fn: (r) => r._field == "percentage")
    |> group(columns: ["_time", "_measurement", "_field"], mode:"by")
    |> mean()
    |> set(key: "host", value: "*")

outputAllGridStatus = outputIndividual
    |> filter(fn: (r) => r._field == "grid_status")
    |> group(columns: ["_time", "_measurement", "_field"], mode:"by")
    |> min()
    |> set(key: "host", value: "*")

union(tables: [outputAllExceptPercentage, outputAllPercentage, outputAllGridStatus])
    |> to(bucket: "powerwall")