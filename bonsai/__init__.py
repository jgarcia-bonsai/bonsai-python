
"""The bonsai package contains the code necessary for users to author
and connect their own custom generators and simulators with the bonsai
brain system.
"""

# The following classes are imported from their respective modules
# so that they are available at the 'bonsai' package level.
from bonsai.brain_server_connection import BrainServerConnection
from bonsai.brain_server_connection import parse_base_arguments
from bonsai.brain_server_connection import run_for_training_or_prediction
from bonsai.brain_server_connection import run_with_url
from bonsai.generator import Generator
from bonsai.simulator import Simulator
