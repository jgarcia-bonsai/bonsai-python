"""
This file contains the class BrainServerConnection, which exposes
functionality intended to make it easy for clients to author simulators
and generators that communicate with BRAIN backend.
"""
import argparse
import asyncio
import logging
import os
from collections import namedtuple
from urllib.parse import urlparse

import websockets

from bonsai.common.state_to_proto import convert_state_to_proto
from bonsai.common.message_builder import MessageBuilder
from bonsai.generator import Generator
from bonsai.simulator import Simulator
from bonsai.proto.generator_simulator_api_pb2 import (
    SimulatorToServer, ServerToSimulator)
from bonsai_config import BonsaiConfig

# TODO: Once we support fully dynamic schemas for generators, we can
# remove this.
from bonsai.proto.curve_generator_pb2 import MNIST_training_data_schema


log = logging.getLogger(__name__)


class BrainServerConnection:

    def __init__(self, brain_api_url, simulator_name, simulator):
        self._current_reward_name = None

        parse_result = urlparse(brain_api_url)
        path_parts = parse_result.path.strip("/").split("/")
        # Simulators connecting to a brain for training should have
        # a path that has five components:
        # /v1/<username>/<brainname>/sims/ws
        if len(path_parts) == 5:
            self.is_training = True
        # Simulators connecting to a brain for prediction should have
        # a path that has six components:
        # /v1/<username>/<brainname>/<version>/predictions/ws
        elif len(path_parts) == 6:
            self.is_training = False
        # If the split path doesn't have 4 or 5 components, continue
        # anyway assuming prediction, but output a log warning.
        else:
            log.warning(
                "The input brain server API URL does not look "
                "correct. If your simulator does not connect, please "
                "double check your URL.")
            self.is_training = False

        self.brain_api_url = brain_api_url

        # Simulator names should conform to inkling rules.
        # TODO: Enforce full inkling rules.
        if not simulator_name:
            raise TypeError("Simulator name cannot be none or empty")
        if " " in simulator_name:
            raise ValueError("Simulator names cannot contain spaces")
        self.simulator_name = simulator_name

        # Ensure the simulator argument has the correct type
        if isinstance(simulator, Simulator):
            self.is_generator = False
        elif isinstance(simulator, Generator):
            self.is_generator = True
        else:
            raise TypeError(
                "Argument 'simulator' must be an object of type "
                "bonsai.Generator or bonsai.Simulator")
        self.simulator = simulator

    def handle_set_properties(self, set_properties_data):
        log.debug("Received set_properties message")

        # Parse request_data into a properties message.
        properties_message = self.properties_schema()
        properties_message.ParseFromString(
            set_properties_data.dynamic_properties)

        # Create a dictionary of the property names to values.
        properties = {}
        for field in properties_message.DESCRIPTOR.fields:
            properties[field.name] = getattr(properties_message, field.name)

        # Call set_properties on the simulator.
        self.simulator.set_properties(**properties)

        # Set current reward name.
        self._current_reward_name = set_properties_data.reward_name

        # Set the predictions schema
        self.prediction_schema = MessageBuilder().reconstitute(
            set_properties_data.prediction_schema)

    def handle_prediction(self, prediction_data):
        log.debug("Received prediction message")

        # Parse request_data into a properties message.
        predictions_msg = self.prediction_schema()
        predictions_msg.ParseFromString(
            prediction_data.dynamic_prediction)

        # Create a dictionary of the property names to values.
        predictions = {}
        for field in predictions_msg.DESCRIPTOR.fields:
            predictions[field.name] = getattr(predictions_msg, field.name)

        self.simulator.notify_prediction_received(predictions)

    def get_state_message(self):
        state = self.simulator.get_state()

        if self._current_reward_name:
            reward = getattr(self.simulator, self._current_reward_name)()
        else:
            reward = 0.0

        terminal = self.simulator.get_terminal()
        state_message = self.output_schema()
        convert_state_to_proto(state_message, state)

        to_server = SimulatorToServer()
        to_server.message_type = SimulatorToServer.STATE
        to_server.state_data.state = state_message.SerializeToString()
        to_server.state_data.reward = reward
        to_server.state_data.terminal = terminal

        # add action taken
        last_action = self.simulator.get_last_action()
        if last_action is not None:
            actions_msg = self.prediction_schema()
            convert_state_to_proto(actions_msg, last_action)
            to_server.state_data.action_taken = actions_msg.SerializeToString()
        return to_server

    @asyncio.coroutine
    def send_register(self, websocket):
        register = SimulatorToServer()
        register.message_type = SimulatorToServer.REGISTER
        register.register_data.simulator_name = self.simulator_name
        yield from websocket.send(register.SerializeToString())

    @asyncio.coroutine
    def recv_acknowledge_register(self, websocket):
        from_server_bytes = yield from websocket.recv()
        from_server = ServerToSimulator()
        from_server.ParseFromString(from_server_bytes)

        if from_server.message_type != ServerToSimulator.ACKNOWLEDGE_REGISTER:
            raise RuntimeError(
                "Expected to receive an ACKNOWLEDGE_REGISTER message, but "
                "instead received message of type {}".format(
                    from_server.message_type))

        if not from_server.HasField("acknowledge_register_data"):
            raise RuntimeError(
                "Received an ACKNOWLEDGE_REGISTER message that did "
                "not contain acknowledge_register_data.")

        # Reconstitute the simulator schemas.
        self.properties_schema = MessageBuilder().reconstitute(
            from_server.acknowledge_register_data.properties_schema)
        self.output_schema = MessageBuilder().reconstitute(
            from_server.acknowledge_register_data.output_schema)
        self.prediction_schema = MessageBuilder().reconstitute(
            from_server.acknowledge_register_data.prediction_schema)

    @asyncio.coroutine
    def send_ready(self, websocket):
        ready = SimulatorToServer()
        ready.message_type = SimulatorToServer.READY
        yield from websocket.send(ready.SerializeToString())

    @asyncio.coroutine
    def handle_from_server(self, websocket, from_server):
        if from_server.message_type == ServerToSimulator.SET_PROPERTIES:
            if not from_server.HasField("set_properties_data"):
                raise RuntimeError(
                    "Received a SET_PROPERTIES message that did "
                    "not contain set_properties_data.")
            self.handle_set_properties(from_server.set_properties_data)
            yield from self.send_ready(websocket)

        elif from_server.message_type == ServerToSimulator.START:
            self.simulator.start()
            to_server = self.get_state_message()
            yield from websocket.send(to_server.SerializeToString())

        elif from_server.message_type == ServerToSimulator.STOP:
            self.simulator.stop()
            yield from self.send_ready(websocket)

        elif from_server.message_type == ServerToSimulator.PREDICTION:
            if not from_server.HasField("prediction_data"):
                raise RuntimeError(
                    "Received a PREDICTION message that did "
                    "not contain prediction_data.")

            self.handle_prediction(from_server.prediction_data)
            to_server = self.get_state_message()
            yield from websocket.send(to_server.SerializeToString())

        elif from_server.message_type == ServerToSimulator.RESET:
            self.simulator_info.simulator.reset()
            yield from self.send_ready(websocket)

        else:
            raise RuntimeError(
                "Cannot handle ServerToSimulator message with type {}".format(
                    from_server.message_type))

    @asyncio.coroutine
    def run_simulator_for_training(self, websocket):
        # Start by sending a ready message to the server
        # TODO: T365: Exchange should start with register first
        yield from self.send_ready(websocket)

        message_count = 0
        while True:
            # Get a message from the server
            from_server_bytes = yield from websocket.recv()
            from_server = ServerToSimulator()
            from_server.ParseFromString(from_server_bytes)

            # Exit if it is a FINISHED message
            if from_server.message_type == ServerToSimulator.FINISHED:
                log.info("Training is finished!")
                return

            # Otherwise handle the message
            yield from self.handle_from_server(websocket, from_server)

            message_count += 1
            if message_count % 250 == 0:
                log.info("Handled %i messages from the server so far",
                         message_count)

    @asyncio.coroutine
    def run_simulator_for_prediction(self, websocket):
        num_predictions = 0
        while True:

            # Send state to the server
            to_server = self.get_state_message()
            yield from websocket.send(to_server.SerializeToString())

            # Get a prediction back from the server
            from_server_bytes = yield from websocket.recv()
            from_server = ServerToSimulator()
            from_server.ParseFromString(from_server_bytes)
            self.handle_prediction(from_server.prediction_data)

            num_predictions += 1
            if num_predictions % 250 == 0:
                log.info("Recieved %i predictions", num_predictions)

    def get_next_data_message(self):
        if not self.is_generator:
            raise RuntimeError(
                "Method get_next_data_message should only be called when a "
                "generator is being used.")

        next_data = self.simulator.next_data()

        # TODO: We don't support fully dynamic schemas for
        # generators yet. All of our current generators
        # currently use the same schema, so we hardcode it here.
        # It's also harcoded in learnerd in
        # BatchTrainer._collate_batch_data().
        next_data_message = MNIST_training_data_schema()
        next_data_message.label = next_data["label"]
        next_data_message.image.width = next_data["image"].width
        next_data_message.image.height = next_data["image"].height
        next_data_message.image.pixels = next_data["image"].pixels
        return next_data_message

    @asyncio.coroutine
    def run_generator_for_training(self, websocket):
        if not self.is_generator:
            raise RuntimeError(
                "Method run_generator_for_training should only be called "
                "when a generator is being used.")

        message_count = 0
        while True:

            # Generators should just always send next data messages
            to_server = self.get_next_data_message()
            yield from websocket.send(to_server.SerializeToString())

            # Get a message from the server
            from_server_bytes = yield from websocket.recv()
            from_server = ServerToSimulator()
            from_server.ParseFromString(from_server_bytes)

            # Handle FINISHED and SET_PROPERTIES messages, otherwise
            # ignore the message.
            if (from_server.message_type ==
                    ServerToSimulator.SET_PROPERTIES):
                if not from_server.HasField("set_properties_data"):
                    raise RuntimeError(
                        "Received a SET_PROPERTIES message that did "
                        "not contain set_properties_data.")
                self.handle_set_properties(from_server.set_properties_data)
            elif from_server.message_type == ServerToSimulator.FINISHED:
                log.info("Training is finished!")
                return

            message_count += 1
            if message_count % 250 == 0:
                log.info("Handled %i messages from the server so far",
                         message_count)

    @asyncio.coroutine
    def run_until_complete(self):
        if self.is_training and self.is_generator:
            log.info("Running generator %s for training",
                     self.simulator_name)
            run_coro = self.run_generator_for_training
        elif self.is_training and not self.is_generator:
            log.info("Running simulator %s for training",
                     self.simulator_name)
            run_coro = self.run_simulator_for_training
        elif not self.is_training and not self.is_generator:
            log.info("Running simulator %s for prediction",
                     self.simulator_name)
            run_coro = self.run_simulator_for_prediction
        else:
            log.error("Nothing to run!")
            return

        log.info("About to connect to %s", self.brain_api_url)
        websocket = yield from websockets.connect(self.brain_api_url)

        try:

            # The first step in all modes is to send a register message
            # and receive an aknowledge register message.
            yield from self.send_register(websocket)
            yield from self.recv_acknowledge_register(websocket)

            # Run the mode specific coroutine
            yield from run_coro(websocket)

        except websockets.exceptions.ConnectionClosed as e:
            log.error("Connection to '%s' is closed, code='%s', reason='%s'",
                      self.brain_api_url, e.code, e.reason)

        finally:
            yield from websocket.close()


_BaseArguments = namedtuple('BaseArguments', ['brain_url', 'headless'])


def parse_base_arguments():
    parser = argparse.ArgumentParser(
        description="Command line interface for running a simulator")

    train_brain_help = "The name of the BRAIN to connect to for training."
    predict_brain_help = (
        "The name of the BRAIN to connect to for predictions. If you "
        "use this flag, you must also specify the --predict-version flag.")
    predict_version_help = (
        "The version of the BRAIN to connect to for predictions. This flag "
        "must be specified when --predict-brain is used. This flag will "
        "be ignored if it is specified along with --train-brain or "
        "--brain-url.")
    brain_url_help = (
        "The full URL of the BRAIN to connect to. The URL should be of "
        "the form ws://api.bons.ai/v1/<username>/<brainname>/sims/ws "
        "when training, and of the form ws://api.bons.ai/v1/"
        "<username>/<brainname>/<version>/predictions/ws when predicting.")
    headless_help = (
        "The simulator can be run with or without the graphical environment."
        "By default the graphical environment is shown. Using --headless "
        "will run the simulator without graphical output.")

    brain_group = parser.add_mutually_exclusive_group(required=True)
    brain_group.add_argument("--train-brain", help=train_brain_help)
    brain_group.add_argument("--predict-brain", help=predict_brain_help)
    brain_group.add_argument("--brain-url", help=brain_url_help)
    parser.add_argument("--predict-version", help=predict_version_help)
    parser.add_argument("--headless", help=headless_help, action="store_true")

    args = parser.parse_args()

    config = BonsaiConfig()
    partial_url = "ws://{host}:{port}/v1/{user}".format(
            host=config.host(),
            port=config.port(),
            user=config.username())

    # If the --brain_url flag was specified, use its value literally
    # for connecting to the BRAIN server. Otherwise, compose the url
    # to connect to from the other possible flags.
    if args.brain_url:
        brain_url = args.brain_url
    elif args.train_brain:
        brain_url = "{base}/{brain}/sims/ws".format(
            base=partial_url, brain=args.train_brain)
    elif args.predict_brain:
        if not args.predict_version:
            log.error("Flag --predict-version must be specified when flag "
                      "--predict-brain is used.")
            return
        brain_url = "{base}/{brain}/{version}/predictions/ws".format(
            base=partial_url,
            brain=args.predict_brain,
            version=args.predict_version)
    else:
        log.error("One of --brain-url, --predict-brain or --train-brain "
                  "must be specified.")
        return

    return _BaseArguments(brain_url, args.headless)


def run_with_url(simulator_name, simulator, brain_url):
    # Create a connection to the brain server
    server = BrainServerConnection(brain_url, simulator_name, simulator)

    # Run until complete
    asyncio.get_event_loop().run_until_complete(server.run_until_complete())


def run_for_training_or_prediction(simulator_name, simulator):
    """
    Helper function for client implemented simulators that exposes the
    appropriate command line arguments necessary for running a
    simulator with BrainServerConnection for training or prediction.
    """
    # Initialize logging.
    logging.basicConfig(level=logging.INFO)

    base_arguments = parse_base_arguments()
    if base_arguments:
        run_with_url(simulator_name, simulator, base_arguments.brain_url)
