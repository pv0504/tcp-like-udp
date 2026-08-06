import socket
import time
import argparse
import json
FILE_PATH = "random_100MB.txt"  
MSS = 1400
INITAL_WINDOW = 20
INITAL_SSTHRESH = MSS
BETA = 0.5  
DUP_ACK_THRESHOLD = 3
TIMEOUT = 1.0  


import logging
logging.basicConfig(filename='./Logs/server_output.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s-%(message)s')


def update_congestion_window_new_ack(cwnd, ssthresh, state):
    if state == "slow_start":
        cwnd += MSS
        if cwnd >= ssthresh:
            state = "congestion_avoidance"
    
    elif state == "congestion_avoidance":
        cwnd += (MSS * MSS) // cwnd

    elif state == "fast_recovery":
        cwnd = ssthresh
        state = "congestion_avoidance"

    return cwnd,state

def handle_triple_duplicate_ack(cwnd, ssthresh):
    ssthresh = max(cwnd // 2, 2 * MSS)  
    cwnd = ssthresh + 3 * MSS  
    return cwnd, ssthresh, "fast_recovery"

def handle_timeout(cwnd, ssthresh):
    ssthresh = max(cwnd // 2, 2 * MSS)  
    cwnd = MSS  
    return cwnd, ssthresh, "slow_start"
    

def send_file(server_ip, server_port):

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((server_ip, server_port))
    logging.info(f"Server listening on {server_ip}:{server_port}")

    cwnd = INITAL_WINDOW
    ssthresh = INITAL_SSTHRESH
    congestion_state = "slow_start"
    client_address = None
    completed_sending = False
    timer = None

    rto = 1.0  # Initial rto of 1 second
    srtt = None  # Smoothed RTT
    rttvar = None  # RTT Variation
    K = 4
    G = 0  # Minimum threshold for rto (can be adjusted





    

    # while not client_address:
    #     logging.info("Waiting for client connection...")
    #     data, client_address = server_socket.recvfrom(1024)
    #     if data.decode('utf-8') == "START":
    #         logging.info(f"Connection established with client {client_address}")
    #     else:
    #         logging.info("Unexpected message from client, expecting START")

    server_socket.settimeout(2)  # 2 seconds timeout for receiving data
    client_address = None

    while not client_address:
        logging.info("Waiting for client connection...")
        try:
            data, client_address = server_socket.recvfrom(1024)
            logging.info(f"Got this as client_address {client_address}")
            if data.decode('utf-8') == "START":
                logging.info(f"Connection established with client {client_address}")
            else:
                logging.warning("Unexpected message from client, expecting START")
                client_address = None  # Reset if message is incorrect
        except socket.timeout:
            logging.info("No data received, still waiting...")

    
    with open(FILE_PATH, 'rb') as file:
        seq_num = 0
        window_base = 0
        unacked_packets = {}
        duplicate_ack_count = 0
        last_ack_received = -1

        while True:

            present_cwnd = max(0, cwnd - len(unacked_packets)*MSS)
            while seq_num < window_base + cwnd:
                chunk = file.read(MSS)
                if not chunk:
                    if unacked_packets:
                        break
                    else:
                        packet = json.dumps({'seq_num': seq_num, 'num_bytes': 0, 'data': 'EOF'}).encode('utf-8')
                        server_socket.sendto(packet,client_address)
                        logging.info(f"Successfully sent EOF. Final cwnd: {cwnd}, state: {congestion_state}")
                        completed_sending = True
                        break
                
                packet = create_packet(seq_num,chunk)
                server_socket.sendto(packet,client_address)
                unacked_packets[seq_num] = (packet, time.time())
                seq_num += MSS

                logging.info(f"Sent packet {seq_num-MSS}, cwnd: {cwnd//MSS} packets, state: {congestion_state}, unacked_packets : {len(unacked_packets)}")

            # if completed_sending:
            #     logging.info(f"YES NO I AM BREAKING IT")
            #     break

            if timer == None:
                timer = time.time() + rto
            
            try:
                socket_timer=max(0.5,timer-time.time())
                server_socket.settimeout(socket_timer)
                ack_packet,_ = server_socket.recvfrom(1024)
                ack_seq_num = get_seq_no_from_ack_pkt(ack_packet)

                if ack_seq_num > last_ack_received:
                    
                    if ack_seq_num - MSS not in unacked_packets:
                        logging.info(f"No timestamp found for packet {ack_seq_num - MSS}. Resetting rto to 1 second.")
                        rto = 1.0
                    else:
                        # Calculate RTT and update rto
                        current_rtt = time.time() - unacked_packets[ack_seq_num - MSS][1]

                        if srtt is None:
                            srtt = current_rtt
                            rttvar = current_rtt / 2
                        else:
                            rttvar = (1 - 0.25) * rttvar + 0.25 * abs(srtt - current_rtt)
                            srtt = (1 - 0.125) * srtt + 0.125 * current_rtt

                        rto = srtt + max(G, K * rttvar)
                        # rto = max(rto, 1.0)
                    logging.info(f"the current rto was {rto}")
                        

                    logging.info(f"Received new ACK for packet {ack_seq_num}")
                    last_ack_received = ack_seq_num
                    window_base = ack_seq_num
                    unacked_packets = {k: v for k, v in unacked_packets.items() if k >= ack_seq_num}
                    duplicate_ack_count = 0
                    timer = None
                    #RETRANSMIT NEW SEGMENT
                    cwnd, congestion_state = update_congestion_window_new_ack(cwnd, ssthresh, congestion_state)
                
                    
                elif ack_seq_num == last_ack_received:
                    
                    duplicate_ack_count += 1

                    #-------------------------------------------------------------
                    if congestion_state == "fast_recovery":
                        # duplicate_ack_count -= 1
                        cwnd += MSS
                        #HERE TRANSMIT THE NEW SEGMENT IF CWND IS COMPATIBLE
                    #--------------------------------


                    logging.info(f"Duplicate ACK {ack_seq_num}, count={duplicate_ack_count}, state={congestion_state}")
                    
                    if duplicate_ack_count == DUP_ACK_THRESHOLD and congestion_state != "fast_recovery":

                        #-------- I THINK I SHOULD NOT MAKE DUPLICATE ACK = 0 HERE

                        cwnd, ssthresh, congestion_state = handle_triple_duplicate_ack(cwnd, ssthresh)
                        fast_recovery(server_socket, client_address, unacked_packets, ack_seq_num)
                        # RETRANSMIT MISSING SEGMENT
                        logging.info(f"Entering fast recovery. cwnd={cwnd}, ssthresh={ssthresh}, congestion_state = {congestion_state}")
                    
                else:
                    # duplicate_ack_count += 1
                    # do i need to change duplicate here
                    continue
                    

            except socket.timeout:
                
                logging.info("Timeout occurred, entering slow start")
                cwnd, ssthresh, congestion_state = handle_timeout(cwnd, ssthresh)

                #--- DO I NEED TO TRANSMIT ONLY UNACKED_PACKETS ?
                retransmit_unacked_packets(server_socket, client_address, unacked_packets)
                timer = None


def create_packet(seq_num, data):

    num_bytes = len(data)  
    packet_dict = {
        'seq_num': seq_num,
        'num_bytes': num_bytes,
        'data': data.decode('latin-1')  
    }
    packet = json.dumps(packet_dict).encode('utf-8')
    return packet


# def get_seq_no_from_ack_pkt(ack_packet):
#     """
#     Extract the sequence number from the ACK packet.
#     """
#     ack_data = json.loads(ack_packet.decode('utf-8'))
#     return ack_data['ack_num']

def get_seq_no_from_ack_pkt(ack_packet):
    """
    Extract the sequence number from the ACK packet.
    Returns -1 if decoding fails or if the packet is empty.
    """
    try:
        if not ack_packet:  # Check if ack_packet is empty
            return -1
        ack_data = json.loads(ack_packet.decode('utf-8'))
        return ack_data['ack_num']
    except (json.JSONDecodeError, KeyError) as e:
        logging.info("GOT JSON ERROr")
        return -1



def retransmit_unacked_packets(server_socket, client_address, unacked_packets):
    """
    Retransmit all unacknowledged packets.
    """
    for seq_num, (packet, _) in unacked_packets.items():
        unacked_packets[seq_num] = (packet, time.time())
        server_socket.sendto(packet, client_address)
        logging.info(f"Retransmitted packet {seq_num}")


def fast_recovery(server_socket, client_address, unacked_packets,ack_seq_num):
    """
    Retransmit the earliest unacknowledged packet (fast recovery).
    """
    logging.info(ack_seq_num,unacked_packets)
    if unacked_packets:
        earliest_seq_num = min(unacked_packets.keys())
        packet, _ = unacked_packets[earliest_seq_num]
        unacked_packets[earliest_seq_num] = (packet, time.time())
        server_socket.sendto(packet, client_address)
        logging.info(f"Fast recovery: retransmitted packet {earliest_seq_num}")


parser = argparse.ArgumentParser(description='Reliable file transfer server over UDP.')
parser.add_argument('server_ip', help='IP address of the server')
parser.add_argument('server_port', type=int, help='Port number of the server')
args = parser.parse_args()
send_file(args.server_ip, args.server_port)
