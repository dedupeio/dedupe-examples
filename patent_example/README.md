Patent Example
=================

This example shows `dedupe` being used to disambiguate data on
inventors from the PATSTAT international patent data file.

The example illustrates a few more elaborate features of PATSTAT:

1. Set and class comparisons, useful for records where sets are a
record attribute (e.g., for voters, "candidates donated to")
2. Geographic distance comparison, via a Haversine distance
3. Set and geographic distance blocking predicates


Data 
-----------

- The `patstat_input.csv` file contains data on Dutch
  innovators. Fields are:
  - Person: the numerical identifier assigned by the Dutch patent
  office. This is not guaranteed to be a unique identifier for all
  instances of a single inventor
  - Name: the inventor name
  - Coauthors: coauthors listed on patents assigned to this inventor
  - Class: the 4-digit IPC technical codes listed on patents assigned
  to this inventor
  - Lat, Lng: the latitude and longitude, geocoded from the inventor's address. 0.0
  indicates no geocode-able address was found
- The `patstat_reference.csv` file contains reference data provided by
  KU Leuven, mapping the Person data to a manually-checked unique
  identifier. Fields are:
  - `person_id`: the PATSTAT unique identifier, equivalent to Person above
  - `leuven_id`: the hand-checked unique identifier; there is a 1:many
  leuven_id:person_id relationship
  - `person_name`: the raw person name matching this person_id

Running the example
-------------------

```python

# To run the disambiguation itself:

python patent_example.py 

# To check the precision-recall relative to the provided reference
# data:

python patent_evaluation.py

```

