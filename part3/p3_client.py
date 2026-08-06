
import socket
import argparse
import heapq
import json
import logging
# Constants
MSS = 1400  # Maximum Segment Size
buffer = []

logging.basicConfig(filename='./Logs/client_output.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s-%(message)s')

def insert_packet(seq_num, data):
    # Push the (seq_num, data) tuple onto the heap
    heapq.heappush(buffer, (seq_num, data))

def get_next_packet():
    # Pop the smallest (seq_num, data) tuple from the heap
    return heapq.heappop(buffer) if buffer else None

import time

def receive_file(server_ip, server_port, pref_outfile, interval=1):
    """
    Receive the file from the server with reliability, handling packet loss
    and reordering, and log throughput over time.
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(2)  # Set timeout for server response

    server_address = (server_ip, server_port)
    expected_seq_num = 0
    output_file_path = f"Logs/{pref_outfile}received_file.txt"
    throughput_log_path = f"Logs/{pref_outfile}throughput_log.txt"

    client_socket.sendto(b"START", server_address)
    is_started = False
    count_timeouted = 0

    total_bytes_received = 0
    interval_start_time = time.time()
    intial_time = time.time()

    with open(output_file_path, 'wb') as file, open(throughput_log_path, 'w') as throughput_log:
        while True:
            try:
                packet, _ = client_socket.recvfrom(MSS + 100)

                # Parse the packet to extract sequence number and data
                seq_num, data = parse_packet(packet)
                count_timeouted = 0
                if data is not None:
                    is_started = True

                if data == 'EOF':
                    print("Received EOF, file transfer complete")
                    break

                total_bytes_received += len(data)


                # If the packet is in order, write it to the file
                if seq_num == expected_seq_num:
                    file.write(data)
                    print(f"Received packet {seq_num}, writing to file")

                    # Update expected sequence number
                    expected_seq_num += len(data)
                    

                    # Write buffered packets in order
                    while buffer and buffer[0][0] == expected_seq_num:
                        next_packet = get_next_packet()
                        file.write(next_packet[1])
                        print(f"Writing packet number {next_packet[0]} from buffer to file")
                        expected_seq_num += len(next_packet[1])
                        # total_bytes_received += len(next_packet[1])

                    # Send cumulative ACK for the received packet
                    send_ack(client_socket, server_address, expected_seq_num)

                elif seq_num < expected_seq_num:
                    print("Received old packet")
                    send_ack(client_socket, server_address, expected_seq_num)

                else:
                    insert_packet(seq_num, data)
                    send_ack(client_socket, server_address, expected_seq_num)

                # Calculate and log throughput for each interval
                current_time = time.time()
                if current_time - interval_start_time >= interval:
                    throughput = total_bytes_received / ((current_time - interval_start_time)*1024*1024)  # bytes per second
                    throughput_log.write(f"{current_time}, {throughput}\n")
                    print(f"Throughput: {throughput} bytes/sec")
                    interval_start_time = current_time
                    total_bytes_received = 0

            except socket.timeout:
                print("Timeout waiting for data")
                count_timeouted += 1
                if count_timeouted >= 5 and is_started:
                    print("Transfer halted due to repeated timeouts")
                    break

                if not is_started:
                    print("Resending START message")
                    client_socket.sendto(b"START", server_address)

    client_socket.close()


def parse_packet(packet):

    packet_dict = json.loads(packet)
    seq_num = packet_dict['seq_num']
    num_bytes = packet_dict['num_bytes']
    data = packet_dict['data']
    if data == 'EOF':
        return seq_num, 'EOF'  # Returning None to indicate EOF
    data = packet_dict['data'].encode('latin-1')  # Decode the data back to bytes
    return seq_num, data

def send_ack(client_socket, server_address, seq_num):
    """
    Send a cumulative acknowledgment for the received packet.
    """
    ack_packet = json.dumps({'ack_num': seq_num}).encode('utf-8')
    client_socket.sendto(ack_packet, server_address)
    print(f"Sent cumulative ACK for packet {seq_num}")

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Reliable file receiver over UDP.')
parser.add_argument('server_ip', help='IP address of the server')
parser.add_argument('server_port', type=int, help='Port number of the server')
parser.add_argument('--pref_outfile', default='', help='Prefix for the output file')


args = parser.parse_args()

# Run the client
receive_file(args.server_ip, args.server_port,args.pref_outfile)
