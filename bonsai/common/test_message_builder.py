import unittest

from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.descriptor_pb2 import FieldDescriptorProto

from bonsai.common.message_builder import MessageBuilder


class BasicMessageBuilding(unittest.TestCase):
    def test_create_named_message_builder(self):
        builder = MessageBuilder('my_message')
        self.assertEqual('my_message', builder._name)

    def test_create_anonymous_message_builder(self):
        builder = MessageBuilder()
        self.assertEqual('anonymous_message_', builder._name[0:18])
        self.assertEqual(
            len('anonymous_message_XXXXXXXX_XXXX_XXXX_XXXX_XXXXXXXXXXXX'),
            len(builder._name))

    def test_anonymous_message_builders_are_unique(self):
        builder1 = MessageBuilder()
        builder2 = MessageBuilder()
        self.assertNotEqual(builder1._name, builder2._name)

    def test_reconstitute_single_schema(self):
        fdp = FileDescriptorProto()
        fdp.name = 'test_schemas'
        mt = fdp.message_type.add()
        mt.name = 'tests'
        f1 = mt.field.add()
        f1.name = 'a'
        f1.number = 1
        f1.type = FieldDescriptorProto.TYPE_UINT32
        f1.label = FieldDescriptorProto.LABEL_OPTIONAL
        bytes = mt.SerializeToString()
        x = MessageBuilder()
        Test = x.reconstitute_from_bytes(bytes)
        test = Test()
        test.a = 42
        self.assertEqual(42, test.a)

    def test_reconstitute_multiple_schemas(self):
        fdp = FileDescriptorProto()
        fdp.name = 'test_schemas'
        mt1 = fdp.message_type.add()
        mt1.name = 'test1'
        f1 = mt1.field.add()
        f1.name = 'a'
        f1.number = 1
        f1.type = FieldDescriptorProto.TYPE_UINT32
        f1.label = FieldDescriptorProto.LABEL_OPTIONAL
        mt2 = fdp.message_type.add()
        mt2.name = 'test2'
        f2 = mt2.field.add()
        f2.name = 'b'
        f2.number = 1
        f2.type = FieldDescriptorProto.TYPE_STRING
        f2.label = FieldDescriptorProto.LABEL_OPTIONAL
        bytes = fdp.SerializeToString()
        x = MessageBuilder()
        classes = x.reconstitute_file_from_bytes(bytes)
        Test1 = classes[0]
        Test2 = classes[1]
        test1 = Test1()
        test1.a = 42
        test2 = Test2()
        test2.b = 'Bonsai Rules!!!'
        self.assertEqual(42, test1.a)
        self.assertEqual('Bonsai Rules!!!', test2.b)

    def test_reconstitute_composite_schema_with_luminance(self):
        fdp = FileDescriptorProto()
        fdp.name = 'test_schemas'
        mt = fdp.message_type.add()
        mt.name = 'tests'
        f1 = mt.field.add()
        f1.name = 'a'
        f1.number = 1
        f1.type = FieldDescriptorProto.TYPE_MESSAGE
        f1.label = FieldDescriptorProto.LABEL_OPTIONAL
        f1.type_name = 'bonsai.inkling_types.proto.Luminance'
        bytes = mt.SerializeToString()
        x = MessageBuilder()
        Test = x.reconstitute_from_bytes(bytes)
        test = Test()
        test.a.width = 42
        self.assertEqual(42, test.a.width)


# PyCharm uses the below lines to allow running unit tests with its own
# unit testing engine. The lines below are added by the PyCharm Python
# unit tests template.
if __name__ == '__main__':
    unittest.main()
