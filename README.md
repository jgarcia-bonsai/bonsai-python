Bonsai SDK
==========
A python library for integrating data sources with Bonsai BRAIN.

Installation
------------
```
$ pip install bonsai-python
```

Install the latest stable from PyPI:
```
$ pip install bonsai-python
```

Install the latest in-development version:
```
$ pip install https://github.com/BonsaiAI/bonsai-python
```

Usage
-----
Subclass either `bonsai.Simulator` or `bonsai.Generator`, implementing
the necessary required methods.
```
class MySimulator(bonsai.Simulator):
    # Simulator methods implementations...
```
Run your simulator with the helper method from `bonsai.BrainServerConnection`.
```
$ python3
> import bonsai
> bonsai.run_for_training_or_prediction(
.     "my_simulator", MySimulator())
```
Note that the schema used by your simulator (or generator), as well as the
name used to identify it, should match the schemas and identifier in the
corresponding inkling file.
