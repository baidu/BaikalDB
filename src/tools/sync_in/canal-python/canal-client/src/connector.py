import socket
import struct


class Connector:
    sock = None
    packet_len = 4

    def connect(self, host, port, timeout=10):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(timeout)
            self.sock.connect((host, port))
            self.sock.settimeout(None)
        except socket.error as e:
            print('Connect to server error: %s' % e)
            self.sock.close()

    def disconnect(self):
        self.sock.close()

    def read(self, length):
        recv = b''
        while True:
            buf = self.sock.recv(length)
            if not buf:
                raise Exception('TSocket: Could not read bytes from server')
            read_len = len(buf)
            if read_len < length:
                recv = recv + buf
                length = length - read_len
            else:
                return recv + buf

    def write(self, buf):
        self.sock.sendall(buf)

    def read_next_packet(self):
        data = self.read(self.packet_len)
        data_len = struct.unpack('>i', data)
        return self.read(data_len[0])

    def write_with_header(self, data):
        self.write(struct.pack('>i', len(data)))
        self.write(data)
