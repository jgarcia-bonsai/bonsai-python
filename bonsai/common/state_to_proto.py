
import logging


log = logging.getLogger(__name__)


def build_luminance_from_state(field_name, proto_msg, luminance):
    """ This function sets a luminance datum onto a protobuf message """
    lum_attr = getattr(proto_msg, field_name)
    lum_attr.width = luminance.width
    lum_attr.height = luminance.height
    lum_attr.pixels = luminance.pixels


# inkling_type_tensor_handler maps inkling types by name to handlers built
inkling_type_proto_handler = {
    "bonsai.inkling_types.proto.Luminance": build_luminance_from_state}


def build_proto_from_embedded_type(message_type, field_name, field_data,
                                   proto_msg):
    """ This function builds a protobuf representation for complex inkling
    types. For successful conversion, typenames and handling functions must
    be registered in the mapping inkling_type_proto_handler
    """
    if message_type is not None:
        log.debug("Converting %s type into tensor form",
                  str(message_type.full_name))
        handler = inkling_type_proto_handler[message_type.full_name]
        handler(field_name, proto_msg, field_data)
    else:
        log.error("build_proto_from_embedded_type unable to process"
                  " message with empty message_type")


def is_proto_type_embedded_message(field):
    """ This function tests whether a particular field is a simple type or a
    complex type that needs explicit handling
    """
    return field.type == field.TYPE_MESSAGE


def convert_state_to_proto(state_msg, state):
    for field in state_msg.DESCRIPTOR.fields:
        # If the field is a message, assume it is Luminance.
        if is_proto_type_embedded_message(field):
            build_proto_from_embedded_type(
                field.message_type, field.name, state[field.name], state_msg)
        else:
            setattr(state_msg, field.name, state[field.name])
