
class Simulator:
    """
    Interface for client implemented Simulators using
    BrainServerConnection.

    Simulators must implement set_prediction(), get_state(),
    get_reward() and get_terminal().

    Implementing start(), stop() and reset() is optional.

    Simulators must also add methods who's names correspond to the
    objectives declared in inkling.
    Note: This Simulator class assumes synchronous action-state transitions.
    This means that the action takes place before the next state is sent. If
    this is not a safe assumption, use AsynchronousSimulator
    """
    def __init__(self):
        self.properties = {}
        self._last_actions = None

    def set_properties(self, **kwargs):
        self.properties = kwargs

    def set_prediction(self, **kwargs):
        raise NotImplementedError()

    def get_state(self):
        raise NotImplementedError()

    def get_reward(self):
        raise NotImplementedError()

    def get_terminal(self):
        raise NotImplementedError()

    def start(self):
        pass

    def stop(self):
        pass

    def reset(self):
        pass

    def get_last_action(self):
        """ when sending states to the server, this function determines which
        corresponding action to send """
        return self._last_actions

    def notify_prediction_received(self, predictions):
        """ When receiving new predictions, save off a copy before reporting
        to simulator """
        self._last_actions = predictions
        self.set_prediction(**predictions)


class AsynchronousSimulator(Simulator):
    """ This simulator interface is to be used wtih asynchronous simiulator
    actions, where the action's effect is not guaranteed to be immediate.
    Like all simulators,
    AsynchronousSimulators must implement set_prediction(), get_state(),
    get_reward() and get_terminal().
    In addition to this, AsynchronousSimulators must be told when an action
    has been taken, and register_action_taken() must be called regularly by
    the simulator.
    """
    def __init__(self):
        super().__init__()

    def notify_prediction_received(self, predictions):
        """ When receiving new predictions, immediately send to simulator,
        and make no assumption about whether the action has affected the state
        """
        self.set_prediction(**predictions)

    def register_action_taken(self, predictions):
        """ This function is for the simulator to notify when an action has
        been taken, and is safe to report to the server as affecting the most
        recent state """
        self._last_actions = predictions
