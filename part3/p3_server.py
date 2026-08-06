import socket
import time
import argparse
import json
import math

MSS = 1400
INITIAL_WINDOW_SIZE = 20
DUP_ACK_THRESHOLD = 3
FILE_PATH = "random_100MB.txt"
TIMEOUT = 1.0


CUBIC_C = 0.4
CUBIC_BETA = 0.5
MIN_CWND = 2
MAX_CWND = 1000

import logging
logging.basicConfig(filename='./Logs/server_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s-%(message)s')

class CubicCongestionControl:
    def __init__(self):
        self.cwnd = INITIAL_WINDOW_SIZE  
        # self.ssthresh = float('inf')     
        self.w_max = 0                   
        self.k = 0                       
        self.epoch_start = 0             
        self.tcp_friendliness = True     

    def calculate_k(self):
        """Calculate K parameter for CUBIC function"""
        return math.pow((self.w_max * CUBIC_BETA) / CUBIC_C, 1/3)

    def cubic_window(self, t):
        """Calculate the CUBIC window size for time t"""
        return CUBIC_C * math.pow(t - self.k, 3) + self.w_max

    def update_on_ack(self, current_time):
        """Update window size on receiving an ACK"""
        # if self.cwnd < self.ssthresh:
            
        #     self.cwnd += 1
        #     logging.info(f"Slow start: increased cwnd to {self.cwnd}")
        # else:
            
        t = current_time - self.epoch_start
        target = self.cubic_window(t)

        if target > self.cwnd:
            self.cwnd = min(target, MAX_CWND)
        logging.info(f"CUBIC: updated cwnd to {self.cwnd}")

    def handle_congestion_event(self, current_time):
        """Handle congestion event (triple duplicate ACK or timeout)"""
        
        self.w_max = self.cwnd ## DO CWND / 2
        self.cwnd = max(MIN_CWND, int(self.cwnd * CUBIC_BETA)) ###
        # self.ssthresh = self.cwnd ###
        
        self.epoch_start = current_time
        self.k = self.calculate_k()
        
        logging.info(f"Congestion event: reduced cwnd to {self.cwnd}")

def send_file(server_ip, server_port, enable_fast_recovery = True):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((server_ip, server_port))
    logging.info(f"Server listening on {server_ip}:{server_port}")
    
    
    cubic = CubicCongestionControl()
    
    
    rto = 1.0  
    srtt = None
    rttvar = None
    K = 4
    G = 0

    server_socket.settimeout(2)
    client_address = None

    while not client_address:
        logging.info("Waiting for client connection...")
        try:
            data, client_address = server_socket.recvfrom(1024)
            if data.decode('utf-8') == "START":
                logging.info(f"Connection established with client {client_address}")
            else:
                client_address = None
        except socket.timeout:
            continue

    with open(FILE_PATH, 'rb') as file:
        seq_num = 0
        window_base = 0
        unacked_packets = {}
        duplicate_ack_count = 0
        last_ack_received = 0
        timer = None

        while True:
            
            while seq_num < window_base + int(cubic.cwnd * MSS):
                chunk = file.read(MSS)
                if not chunk:
                    if not unacked_packets:
                        packet = json.dumps({'seq_num': seq_num, 'num_bytes': 0, 'data': 'EOF'}).encode('utf-8')
                        server_socket.sendto(packet, client_address)
                        logging.info("Transfer complete")
                        return
                    break

                packet = create_packet(seq_num, chunk)
                server_socket.sendto(packet, client_address)
                unacked_packets[seq_num] = (packet, time.time())
                logging.info(f"Sent packet {seq_num}, current cwnd: {cubic.cwnd}")
                seq_num += MSS

            if timer is None:
                timer = time.time() + rto

            try:
                socket_timer = max(0.1, timer - time.time())
                server_socket.settimeout(socket_timer)
                ack_packet, _ = server_socket.recvfrom(1024)
                ack_seq_num = get_seq_no_from_ack_pkt(ack_packet)

                if ack_seq_num > last_ack_received:
                    
                    if ack_seq_num - MSS in unacked_packets:
                        current_rtt = time.time() - unacked_packets[ack_seq_num - MSS][1]
                        if srtt is None:
                            srtt = current_rtt
                            rttvar = current_rtt / 2
                        else:
                            rttvar = (1 - 0.25) * rttvar + 0.25 * abs(srtt - current_rtt)
                            srtt = (1 - 0.125) * srtt + 0.125 * current_rtt
                        rto = srtt + max(G, K * rttvar)

                    
                    cubic.update_on_ack(time.time())
                    
                    
                    last_ack_received = ack_seq_num
                    window_base = ack_seq_num
                    unacked_packets = {k: v for k, v in unacked_packets.items() if k >= ack_seq_num}
                    duplicate_ack_count = 0
                    timer = None

                elif ack_seq_num == last_ack_received:
                    duplicate_ack_count += 1
                    if enable_fast_recovery and duplicate_ack_count >= DUP_ACK_THRESHOLD:
                        cubic.handle_congestion_event(time.time())
                        duplicate_ack_count = 0
                        fast_recovery(server_socket, client_address, unacked_packets, ack_seq_num)

            except socket.timeout:
                logging.info("Timeout occurred")
                cubic.handle_congestion_event(time.time())
                retransmit_unacked_packets(server_socket, client_address, unacked_packets)
                timer = None


def create_packet(seq_num, data):
    num_bytes = len(data)
    packet_dict = {
        'seq_num': seq_num,
        'num_bytes': num_bytes,
        'data': data.decode('latin-1')
    }
    return json.dumps(packet_dict).encode('utf-8')

def get_seq_no_from_ack_pkt(ack_packet):
    ack_data = json.loads(ack_packet.decode('utf-8'))
    return ack_data['ack_num']

def retransmit_unacked_packets(server_socket, client_address, unacked_packets):
    for seq_num, (packet, _) in unacked_packets.items():
        unacked_packets[seq_num] = (packet, time.time())
        server_socket.sendto(packet, client_address)
        logging.info(f"Retransmitted packet {seq_num}")

def fast_recovery(server_socket, client_address, unacked_packets, ack_seq_num):
    if unacked_packets:
        earliest_seq_num = min(unacked_packets.keys())
        packet, _ = unacked_packets[earliest_seq_num]
        unacked_packets[earliest_seq_num] = (packet, time.time())
        server_socket.sendto(packet, client_address)
        logging.info(f"Fast recovery: retransmitted packet {earliest_seq_num}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reliable file transfer server over UDP with TCP CUBIC.')
    parser.add_argument('server_ip', help='IP address of the server')
    parser.add_argument('server_port', type=int, help='Port number of the server')
    # parser.add_argument('fast_recovery', type=bool, help='Enable fast recovery (1 to enable, 0 to disable)')

    args = parser.parse_args()
    send_file(args.server_ip, args.server_port)