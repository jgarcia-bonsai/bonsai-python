"""Defines a class for building dynamic protobuf messages.
"""
from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.descriptor_pb2 import DescriptorProto
from google.protobuf.descriptor_pb2 import FieldDescriptorProto
from google.protobuf.descriptor import MakeDescriptor, FieldDescriptor
from google.protobuf.message_factory import MessageFactory

from bonsai.common.utils import generate_guid
from bonsai.proto import inkling_types_pb2


class MessageBuilder:
    """
    Class used to build protobuf dynamic messages appropriate for
    Python. This builder is intended to be used while traversing
    an Inkling AST, allowing users to set parameters as they are
    discovered in the tree rather than caching names and values
    then calling a single add_field(name,type) operation. For
    testing and ad-hoc purposes, the builder can be used in the
    standard GoF Builder pattern-style popular with Java frameworks.
    >>> x = MessageBuilder('Test')
    >>> Test = (x.with_name('a').with_type(brain_pb2.uint8Type).add_field()
    >>>          .with_name('b').with_type(brain_pb2.stringType).add_field()
    >>>          .with_name('c').with_type(brain_pb2.doubleType).add_field()
    >>>          .build())
    >>> tests = Test()
    >>> tests.a = 42
    >>> tests.b = 'Bonsai Rules!!!!'
    >>> tests.c = 3.14159
    >>> assert(tests.a == 42)
    >>> assert(tests.b == 'Bonsai Rules!!!!')
    >>> assert(tests.c == 3.14159)
    """

    def __init__(self, name=None):
        """
        Creates a message builder that will create a named or anonymous
        message.
        Args:
            name: The name of the message to create. If not provided or
                  set to None, the name is set to
                  'anonymous_message_XXXXXXXX_XXXX_XXXX_XXXX_XXXXXXXXXXXX',
                  where each X is a random hex digit.
        Returns:
            nothing
        """
        self._name = name or 'anonymous_message_{}'.format(generate_guid())
        self._file_descriptor_name = 'schema_containing_{}'.format(self._name)
        self._package = 'bonsai.proto'
        self._full_name = '{}.{}'.format(self._package,
                                         self._name)
        self._fields = {}
        self._current_field_name = ''
        self._current_field_type = None
        self._current_field_is_array = False
        self._factory = MessageFactory()
        inkling_file_descriptor = FileDescriptorProto()
        inkling_types_pb2.DESCRIPTOR.CopyToProto(inkling_file_descriptor)
        self._factory.pool.Add(inkling_file_descriptor)
        self._fields_to_resolve = {}

    def as_array(self):
        """
        Marks the current field being added as an array. In Protobuf,
        the field will be a REPEATED field.
        Returns:
            self
        """
        self._current_field_is_array = True
        return self

    def _resolve_composite_schemas(self, descriptor):
        """
        The DescriptorPool in MessageFactory doesn't resolve message
        types for composite schemas (i.e. a Luminance or Matrix schema
        type field in the message). build(), reconstitute(), and
        reconstitute_file() each flag fields in the descriptor marked
        as a TYPE_MESSAGE and caches the type names those fields are
        assigned. Then, after the Descriptor is created, it goes back
        and associates the appropriate structure with those fields.
        Args:
            descriptor: The Descriptor object for the message that
                        needs resolving.
        Returns:
            nothing.
        """
        for field in descriptor.fields:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                type_name = self._fields_to_resolve[field.name]
                type = self._factory.pool.FindMessageTypeByName(type_name)
                field.message_type = type

    def reconstitute_from_bytes(self, descriptor_proto_bytes):
        """
        Reconstitutes a Python protobuf class from a byte stream. The
        intended purpose of this function is to create a Protobuf
        Python class from a byte stream sent from another service. This
        way, services can define arbitrary data types and send schemas
        for those types to other services.
        Args:
            descriptor_proto_bytes: Serialized protocol buffer describing
                                    a single class
        Returns:
            A Python class for the message encoded in
            descriptor_proto_bytes.
        """
        descriptor_proto = DescriptorProto()
        descriptor_proto.ParseFromString(descriptor_proto_bytes)
        return self.reconstitute(descriptor_proto)

    def reconstitute(self, descriptor_proto):
        """
        Reconstitutes a Python protobuf class from a DescriptorProto
        message. Use this instead of reconstitute_from_bytes if you've
        already got a DescriptorProto message.
        """
        for field in descriptor_proto.field:
            if field.type == FieldDescriptorProto.TYPE_MESSAGE:
                self._fields_to_resolve[field.name] = field.type_name
        descriptor = MakeDescriptor(descriptor_proto, self._package)
        self._resolve_composite_schemas(descriptor)
        return self._factory.GetPrototype(descriptor)

    def reconstitute_file_from_bytes(self, file_descriptor_proto_bytes):
        """
        Reconstitutes one or more Python protobuf classes from a byte
        stream. The intended purpose of this function is to create a
        set of Protobuf Python classes from a byte stream file sent
        from another service. This way, services can define arbitrary
        data types and send schemas for those types to other services.
        Args:
            file_descriptor_proto_bytes: Serialized protocol buffer file
                                         containing one or more messages.

        Returns:
            An array containing each class contained in
            file_descriptor_proto_bytes.
        """
        file_descriptor_proto = FileDescriptorProto()
        file_descriptor_proto.ParseFromString(file_descriptor_proto_bytes)
        return self.reconstitute_file(file_descriptor_proto)

    def reconstitute_file(self, file_descriptor_proto):
        """
        Reconstitutes one or more Python protobuf classes from a
        FileDescriptorProto message. Use this instead of
        reconstitute_file_from_bytes if you've already got a
        FileDescriptorProto message.
        """
        classes = []
        for message_proto in file_descriptor_proto.message_type:
            for field in message_proto.field:
                if field.type == FieldDescriptorProto.TYPE_MESSAGE:
                    self._fields_to_resolve[field.name] = field.type_name
            descriptor = MakeDescriptor(message_proto, self._package)
            self._resolve_composite_schemas(descriptor)
            message_type = self._factory.GetPrototype(descriptor)
            classes.append(message_type)
        return classes
