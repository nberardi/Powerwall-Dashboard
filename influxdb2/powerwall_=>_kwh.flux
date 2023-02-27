import "date"

option task = { 
  name: "powerwall => kwh",
  every: 1m,
}

start = date.truncate(t: now(), unit: 1h)

from(bucket: "powerwall")
  |> range(start: start)
  |> filter(fn: (r) => r["_measurement"] == "instant_power")
  |> integral()
  |> map(fn: (r) => ({r with _time: date.truncate(t: r._start, unit: 1h )}))
  |> map(fn: (r) => ({r with _value: r._value / 1000.0 / 3600.0}))
  |> drop(columns: ["_start", "_stop"])
  |> set(key: "_measurement", value: "kwh")
  |> to(bucket: "powerwall")