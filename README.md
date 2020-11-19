# Transform
Python code for implementing transforms on data extracted from DCs

## Transform dictionary
A transform dictionary expresses the transformations needed.

- Each level of the dictionary is a set of keys corresponding to the origin
  field names.
- A field that will be discarded in the destination is simply omitted from the
  dictionary. 
- Each field has at least one entry. This entry is called "CDA-X" and is
  a list of transform functions that have to be applied to the field. It can be
  an empty list. Each item in that list is a transform object, detailed below.
- A field that has a struct as it's contents, contains a second entry called
  "CHILD". The child is another dictionary defined as above.
- No special notation is needed for a field that can contain a list of objects.

**A template transforms dictionary can be constructed from a dataset by running
the script `schema2transform.py` on the dataset. The generated YML can be then
hand edited to add in transforms that are needed.**

## Transform object

Each transform object is a dictionary of the form

```
CDA-X:
    field:
        - F: function_name
          P:
            p1: param1
            p2: param2
        ...
    value:
        - F: function_name
          P:
            p1: ...
            p2: ...
    ...
```

- A function in `field` changes the name of the field
- A function in `value` changes the value
- Any parameters are passed to the function as keyword arguments
