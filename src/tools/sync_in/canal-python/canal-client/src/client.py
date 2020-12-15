import time
from connector import Connector
from protocol import CanalProtocol_pb2
from protocol import EntryProtocol_pb2


class Client:

    def __init__(self):
        self.connector = Connector()

    def connect(self, host='127.0.0.1', port=11111):
        self.connector.connect(host, port)
        data = self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)
        if packet.type != CanalProtocol_pb2.PacketType.HANDSHAKE:
            raise Exception('connect error')
        print('connected to %s:%s' % (host, port))

    def disconnect(self):
        self.connector.disconnect()

    def check_valid(self, username=b'', password=b''):
        client_auth = CanalProtocol_pb2.ClientAuth()
        client_auth.username = username
        client_auth.password = password

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.CLIENTAUTHENTICATION
        packet.body = client_auth.SerializeToString()

        self.connector.write_with_header(packet.SerializeToString())

        data = self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)
        if packet.type != CanalProtocol_pb2.PacketType.ACK:
            raise Exception('Auth error')

        ack = CanalProtocol_pb2.Ack()
        ack.MergeFromString(packet.body)
        if ack.error_code > 0:
            raise Exception('something goes wrong when doing authentication. error code:%s, error message:%s' % (
            ack.error_code, ack.error_message))
        print('Auth succed')

    def subscribe(self, client_id=b'1001', destination=b'example', filter=b'.*\\..*'):
        self.client_id = client_id
        self.destination = destination

        self.rollback(0)

        sub = CanalProtocol_pb2.Sub()
        sub.destination = destination
        sub.client_id = client_id
        sub.filter = filter

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.SUBSCRIPTION
        packet.body = sub.SerializeToString()

        self.connector.write_with_header(packet.SerializeToString())

        data = self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)
        if packet.type != CanalProtocol_pb2.PacketType.ACK:
            raise Exception('Subscribe error.')

        ack = CanalProtocol_pb2.Ack()
        ack.MergeFromString(packet.body)
        if ack.error_code > 0:
            raise Exception(
                'Failed to subscribe. error code:%s, error message:%s' % (ack.error_code, ack.error_message))
        print('Subscribe succed')

    def unsubscribe(self):
        pass

    def get(self, size=100):
        message = self.get_without_ack(size)
        self.ack(message['id'])
        return message

    def get_without_ack(self, batch_size=10, timeout=-1, unit=-1):
        get = CanalProtocol_pb2.Get()
        get.client_id = self.client_id
        get.destination = self.destination
        get.auto_ack = False
        get.fetch_size = batch_size
        get.timeout = timeout
        get.unit = unit

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.GET
        packet.body = get.SerializeToString()

        self.connector.write_with_header(packet.SerializeToString())

        data = self.connector.read_next_packet()
        packet = CanalProtocol_pb2.Packet()
        packet.MergeFromString(data)

        message = dict(id=0, entries=[])
        if packet.type == CanalProtocol_pb2.PacketType.MESSAGES:
            messages = CanalProtocol_pb2.Messages()
            messages.MergeFromString(packet.body)
            if messages.batch_id > 0:
                message['id'] = messages.batch_id
                for item in messages.messages:
                    entry = EntryProtocol_pb2.Entry()
                    entry.MergeFromString(item)
                    message['entries'].append(entry)
        elif packet.type == CanalProtocol_pb2.PacketType.ACK:
            ack = CanalProtocol_pb2.PacketType.Ack()
            ack.MergeFromString(packet.body)
            if ack.error_code > 0:
                raise Exception('get data error. error code:%s, error message:%s' % (ack.error_code, ack.error_message))
        else:
            raise Exception('unexpected packet type:%s' % (packet.type))

        return message

    def ack(self, message_id):
        if message_id:
            clientack = CanalProtocol_pb2.ClientAck()
            clientack.destination = self.destination
            clientack.client_id = self.client_id
            clientack.batch_id = message_id

            packet = CanalProtocol_pb2.Packet()
            packet.type = CanalProtocol_pb2.PacketType.CLIENTACK
            packet.body = clientack.SerializeToString()

            self.connector.write_with_header(packet.SerializeToString())

    def rollback(self, batch_id):
        cb = CanalProtocol_pb2.ClientRollback()
        cb.batch_id = batch_id
        cb.client_id = self.client_id
        cb.destination = self.destination

        packet = CanalProtocol_pb2.Packet()
        packet.type = CanalProtocol_pb2.PacketType.CLIENTROLLBACK
        packet.body = cb.SerializeToString()

        self.connector.write_with_header(packet.SerializeToString())


if __name__ == "__main__":
    client = Client()
    client.connect(host='172.12.0.13')
    client.check_valid()
    client.subscribe()

    while True:
        message = client.get(100)
        entries = message['entries']
        for entry in entries:
            entry_type = entry.entryType
            if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN, EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                continue
            row_change = EntryProtocol_pb2.RowChange()
            row_change.MergeFromString(entry.storeValue)
            event_type = row_change.eventType
            header = entry.header
            database = header.schemaName
            table = header.tableName
            event_type = header.eventType
            for row in row_change.rowDatas:
                format_data = dict()
                if event_type == EntryProtocol_pb2.EventType.DELETE:
                    for column in row.beforeColumns:
                        format_data = {
                            column.name: column.value
                        }
                elif event_type == EntryProtocol_pb2.EventType.INSERT:
                    for column in row.afterColumns:
                        format_data = {
                            column.name: column.value
                        }
                else:
                    format_data['before'] = format_data['after'] = dict()
                    for column in row.beforeColumns:
                        format_data['before'][column.name] = column.value
                    for column in row.afterColumns:
                        format_data['after'][column.name] = column.value
                data = dict(
                    db=database,
                    table=table,
                    event_type=event_type,
                    data=format_data,
                )
                print(data)
        time.sleep(1)

    client.disconnect()
