# myCalendar
A Rest API for time management and scheduling service.
## Setup
- Download the Australia suburb dataset [here](https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-australia-state-suburb/exports/csv?lang=en&timezone=Australia%2FSydney&use_labels=true&delimiter=%3B) (file too big for GitHub)
- Install any libraries needed from `requirement.txt`

## Running
- Run `python3 myCalendar.py georef-australia-state-suburb.csv au.csv` 
- Navigate to `localhost:5000`, this should display a Swagger document.
- See the Swagger document produced for more details.

## Misc
- The `myCalendar.db` file given is a default database, feel free to delete it and run the code again to create a new one. A new empty database will be generated automatically, with the same name by default.
