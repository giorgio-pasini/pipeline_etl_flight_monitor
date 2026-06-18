# Discovering the FlightRadarAPI — Exploration Log

This document retraces the **full process** that took the project from a notebook full of
non-working placeholder snippets to a validated, end-to-end-runnable exploration notebook.
It is written as a journal: every error encountered is reproduced verbatim, explained, and
resolved, so the reasoning behind each fix is auditable.

---

## 1. Starting point

The notebook (`notebook_exploration.ipynb`) shipped with example calls copied from the
library's README, where arguments were stubbed out with the Python ellipsis literal `...`:

```python
flights  = fr_api.get_flights(...)    # Returns a list of Flight objects
airports = fr_api.get_airports(...)    # Returns a list of Airport objects
flight_details = fr_api.get_flight_details(flight)
```

None of these ran as written. The goal was to understand **what the API actually returns**,
fix the calls, and build a coherent data exploration that maps onto the kata's required
indicators.

At this stage I knew nothing about the library beyond its name (`FlightRadarAPI`, v1.5.1).

---

## 2. Methodology

Rather than guessing at signatures, I used two complementary techniques:

1. **Read the installed source** directly. The package is local at
   `D:\Programmi\Python311\Lib\site-packages\FlightRadarAPI\`. Reading the source is the
   ground truth — docstrings and type hints there never lie about the current version.
   - `api.py` — the public methods and their signatures.
   - `entities/flight.py` and `entities/airport.py` — the exact attributes each object exposes.
   - `core.py` — the `Countries` enum and static zone data.
   - `request.py` — to locate the origin of a noisy warning.

2. **Probe the live API** with short throwaway scripts to confirm real return shapes
   (key names, list lengths, enum member names) instead of trusting assumptions.

This "read the code, then verify against live data" loop is what every fix below is built on.

---

## 3. Errors found and how they were resolved

### 3.1 The gzip warning (a red herring)

Every call to `get_flights()` printed:

```
APIRequest.get_content: failed to decode Content-Encoding='gzip' for
https://data-cloud.flightradar24.com/zones/fcgi/feed.js
(Not a gzipped file (b'{"')). Assuming the transport already decompressed and returning raw bytes.
```

**Diagnosis.** This looks alarming but is harmless. The FR24 endpoint advertises
`Content-Encoding: gzip` in its headers, but the HTTP transport (`curl_cffi`) has *already*
decompressed the body. The library tries to gunzip again, fails, and — as the message itself
says — **falls back to the raw bytes**. The data is intact.

I confirmed the source of the message by grepping the package:

```python
# FlightRadarAPI/request.py
_logger = logging.getLogger(__name__)
_logger.warning("APIRequest.get_content: failed to decode Content-Encoding=%r for %s (%s). ...")
```

It is a `logging.warning` on the `FlightRadarAPI.request` logger — **not** an exception.

**Resolution.** Silence the noise at the parent logger so the exploration output stays
readable, while documenting *why* it is safe to ignore:

```python
import logging, warnings
logging.getLogger("FlightRadarAPI").setLevel(logging.ERROR)  # child loggers inherit this
warnings.filterwarnings("ignore")
```

### 3.2 `get_flights(...)` silently returned `[]`

```python
flights = fr_api.get_flights(...)   # -> []
```

**Diagnosis.** `...` is the **Ellipsis singleton**, a real Python object — not "fill me in."
Reading `api.py` showed every parameter of `get_flights` is optional:

```python
def get_flights(self, airline=None, bounds=None, registration=None,
                aircraft_type=None, *, details=False) -> List[Flight]:
```

Passing `Ellipsis` positionally set `airline=...`, which the API treated as a bogus filter and
matched nothing — hence an empty list with no error.

**Resolution.** Call it with no arguments to fetch the global feed:

```python
flights = fr_api.get_flights()
# -> 1500 Flight objects (the API caps the global, unbounded feed at ~1500)
```

### 3.3 `get_airports(...)` → `TypeError: 'ellipsis' object is not iterable`

```
TypeError: 'ellipsis' object is not iterable
  File ".../api.py", line 195, in get_airports
    results = executor.map(_fetch, countries)
```

**Diagnosis.** Same root cause (`...`), but here it failed loudly because the method tries to
**iterate** its argument across a thread pool. `Ellipsis` is not iterable.

**First attempt** — drop the ellipsis:

```python
airports = fr_api.get_airports()
# -> TypeError: get_airports() missing 1 required positional argument: 'countries'
```

So unlike `get_flights`, `countries` is **mandatory**. The signature confirms it:

```python
def get_airports(self, countries: List[Countries]) -> List[Airport]:
```

**Second attempt** — pass country names as strings:

```python
airports = fr_api.get_airports(countries=["United States", "Italy"])
# -> AttributeError: 'str' object has no attribute 'value'
```

The traceback pointed at the internal fetch:

```python
href = Core.airports_data_url + "/" + country.value   # <- expects an enum member
```

So `countries` must be members of the **`Countries` enum**, not plain strings. I enumerated the
enum live to find the correct member names:

```python
from FlightRadarAPI.core import Countries
[c.name for c in Countries if "UNITED" in c.name or "ITAL" in c.name or "FRANCE" in c.name]
# -> ['FRANCE', 'ITALY', 'UNITED_ARAB_EMIRATES', 'UNITED_KINGDOM', 'UNITED_STATES', ...]
# (228 countries total; .value is the URL slug, e.g. Countries.ITALY.value == 'italy')
```

**Resolution.**

```python
from FlightRadarAPI.core import Countries
airports = fr_api.get_airports(countries=[Countries.FRANCE, Countries.ITALY])
# -> 241 Airport objects
```

### 3.4 `NameError: name 'flight' is not defined`

```python
flight_details = fr_api.get_flight_details(flight)   # NameError: 'flight' undefined
```

**Diagnosis.** The cell referenced a `flight` variable that was never created — the snippet
assumed an object that only exists once you pick one out of `get_flights()`.

**Resolution.** Derive it defensively from the flights list, guarding the empty case:

```python
if flights:
    flight = flights[0]
    flight.set_flight_details(fr_api.get_flight_details(flight))
    print("Flying to", flight.destination_airport_name)
```

---

## 4. What the source reading revealed about the data model

Reading the entity classes told me exactly which fields exist — essential for shaping the
DataFrames and for mapping the kata indicators.

**`Flight` (from `get_flights`)** — positional fields decoded from the raw FR24 array. The
useful ones: `latitude`, `longitude`, `altitude`, `ground_speed`, `heading`, `on_ground`,
`aircraft_code`, `registration`, `airline_icao`, `airline_iata`, `origin_airport_iata`,
`destination_airport_iata`, `number`, `callsign`.

> **Key limitation discovered here:** the real-time feed gives only **IATA codes**, not the
> country/continent or the aircraft model. Those require enrichment (next point).

**`Flight` after `set_flight_details(...)`** — `get_flight_details()` populates ~50 extra
attributes, including the ones the indicators actually need: `aircraft_model`, `airline_name`,
`origin_airport_country_name`, `destination_airport_country_name`, and origin/destination
lat-lon (for trajectory length). The cost: **one HTTP request per flight** (the library can
parallelize via `get_flights(..., details=True)`).

**`Airport`** — `name`, `iata`, `icao`, `country`, `latitude`, `longitude`, `altitude`.

**`get_airlines()`** — probed live; returns 2062 dicts with keys
`Name`, `ICAO`, `IATA`, `n_aircrafts`.

**`get_zones()` / `get_bounds()`** — probed live; 9 top-level zones, each a bounding box
(`tl_y, tl_x, br_y, br_x`) plus optional `subzones`. `get_bounds(zone)` turns a zone into the
`"y1,y2,x1,x2"` string that `get_flights(bounds=...)` accepts — the mechanism for collecting
**region by region** to get past the 1500-flight global cap.

---

## 5. Data-quality observations (live run)

Flattening the 1500 flights into a DataFrame and profiling it surfaced what the pipeline's
cleaning phase must handle:

- **~23%** of flights have **no destination IATA**, **~22%** no flight number, **~22%** no
  `airline_icao`.
- A meaningful share are **on the ground** (`on_ground == 1`) — to be excluded from
  "flights in progress" indicators.
- `get_flight_details` can return `N/A` for the destination of a ground/stationary object
  (e.g. the first item came back as a `GRND` "Beacon").

These confirm that filtering (`on_ground`, missing routes) and an UNKNOWN-vs-exclude policy
for missing codes are mandatory before computing any indicator.

---

## 6. Validation

The notebook is not "fixed" until it runs clean from a cold kernel. I executed it
end-to-end and asserted there were zero error outputs:

```bash
python -m jupyter nbconvert --to notebook --execute --inplace \
       --ExecutePreprocessor.timeout=180 notebook_exploration.ipynb
```

Then a programmatic check of the resulting `.ipynb`:

```
Total cells: 24 | code errors: 0
```

Sample live outputs after the run:

```
API prête — 9 zones disponibles
Total flights: 1500
(1500, 16)              # flights DataFrame shape
Total aéroports (FR + IT) : 241
(2062, 4)               # airlines DataFrame shape
Vols Emirates B77W au-dessus de l'Amérique du Nord : 1
```

---

## 7. Summary of fixes

| # | Symptom | Root cause | Fix |
|---|---------|-----------|-----|
| 1 | `gzip` decode warning on every call | Endpoint double-declares gzip; transport already decompressed | Harmless — silence `FlightRadarAPI` logger, document why |
| 2 | `get_flights(...)` returns `[]` | `...` (Ellipsis) passed as the `airline` filter | Call `get_flights()` with no args |
| 3 | `get_airports(...)` → `'ellipsis' object is not iterable` | `...` passed where an iterable is required | — |
| 3b | `get_airports()` → missing arg `countries` | `countries` is mandatory | Pass a list |
| 3c | `get_airports([...strings...])` → `'str' object has no attribute 'value'` | Expects `Countries` **enum** members, not strings | `[Countries.FRANCE, Countries.ITALY]` |
| 4 | `get_flight_details(flight)` → `NameError` | `flight` never defined | Derive `flights[0]` with an empty-list guard |

---

## 8. Takeaways

- **`...` is not a placeholder** in runnable code — it is the Ellipsis object and will either
  silently misbehave (case 2) or crash (case 3).
- **The installed source is the fastest, most reliable documentation.** Reading `api.py` and the
  entity classes answered every signature/field question without trial and error.
- **A warning is not an error.** The gzip message was the scariest-looking output and the least
  important; confirming its origin in `request.py` settled it in minutes.
- **Verify return shapes against the live API.** Enum member names, dict keys, and zone
  structure were all confirmed with tiny probe scripts before being relied on in the notebook.
