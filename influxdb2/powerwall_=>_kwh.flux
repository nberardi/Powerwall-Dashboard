import "date"

option task = { 
  name: "powerwall => kwh",
  every: 1m,
}

currentHourStart = date.truncate(t: now(), unit: 1h)
previousHourStart = date.sub(from: currentHourStart, d: 1h)
previousHourStop = date.sub(from: currentHourStart, d: 1s)

previousHour = from(bucket: "powerwall")
  |> range(start: previousHourStart, stop: previousHourStop)
  |> filter(fn: (r) => r["_measurement"] == "instant_power")
  |> integral()
  |> map(fn: (r) => ({r with _time: date.truncate(t: r._start, unit: 1h )}))

currentHour = from(bucket: "powerwall")
  |> range(start: currentHourStart)
  |> filter(fn: (r) => r["_measurement"] == "instant_power")
  |> integral()
  |> map(fn: (r) => ({r with _time: date.truncate(t: r._start, unit: 1h )}))

union(tables: [previousHour, currentHour])
  |> map(fn: (r) => ({r with _value: r._value / 1000.0 / 3600.0}))
  |> drop(columns: ["_start", "_stop"])
  |> set(key: "_measurement", value: "kwh")
  |> to(bucket: "powerwall")