# abssa-calendar

This script fetches (ABSSA's fixtures data)[http://www.abssa.org/championnat] and output it as ics files which can then be imported in your favorite calendar.

## Quickstart

```
git clone git@github.com:simonpicard/abssa-calendar.git
cd abssa-calendar
python3 -m venv .env
. .env/bin/activate
(.env) pip install -r requirements.txt
(.env) python src/scrapper.py -d ./test_directory
```

## Pre scrapped calendar

See folder `./ics`