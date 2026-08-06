
import socket
import argparse
import heapq
import json
import logging
# Constants
MSS = 1400  # Maximum Segment Size

logging.basicConfig(filename='./client_output.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s-%(message)s')

def insert_packet(seq_num, data,buffer):
    # Push the (seq_num, data) tuple onto the heap
    heapq.heappush(buffer, (seq_num, data))

def get_next_packet(buffer):
    # Pop the smallest (seq_num, data) tuple from the heap
    return heapq.heappop(buffer) if buffer else None

def receive_file(server_ip, server_port):
    """
    Receive the file from the server with reliability, handling packet loss
    and reordering.
    """
    # Initialize UDP socket
    buffer = []
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(2)  # Set timeout for server response

    server_address = (server_ip, server_port)
    expected_seq_num = 0
    output_file_path = f"./received_file.txt"  # Default file name

    # Send initial connection request to server
    client_socket.sendto(b"START", server_address)
    is_started = False
    count_timeouted = 0

    with open(output_file_path, 'wb') as file:
        while True:
            try:

                # Receive the packet
                packet, _ = client_socket.recvfrom(MSS + 100)  # Allow room for headers

                # Parse the packet to extract sequence number and data
                seq_num, data = parse_packet(packet)
                count_timeouted = 0
                if data is not None:
                    is_started = True

                if data == 'EOF':
                    logging.info("Received EOF, file transfer complete")
                    break

                # If the packet is in order, write it to the file
                if seq_num == expected_seq_num:
                    file.write(data)
                    logging.info(f"Received packet {seq_num}, writing to file")

                    # Update expected sequence number
                    expected_seq_num += len(data)

                    # Write buffered packets in order
                    while buffer and buffer[0][0] == expected_seq_num:
                        next_packet = get_next_packet(buffer)
                        file.write(next_packet[1])
                        logging.info(f"writing packet number {next_packet[0]} through buffer, writing to file")
                        expected_seq_num += len(next_packet[1])

                    # Send cumulative ACK for the received packet
                    send_ack(client_socket, server_address, expected_seq_num)

                elif seq_num < expected_seq_num:
                    logging.info("got old packet................")
                    send_ack(client_socket, server_address, expected_seq_num)

                else:
                    insert_packet(seq_num, data, buffer)
                    send_ack(client_socket, server_address, expected_seq_num)  # ACK for last contiguous packet

            except socket.timeout:
                logging.info(f"Timeout waiting for data {count_timeouted}")
                count_timeouted += 1
                # if count_timeouted >= 7 and is_started:
                #     logging.info("IT WAS BROKE THROUGH TIMEOUTEED")
                #     count_timeouted = 0
                #     break

                if not is_started:
                    logging.info("I AM AGAIN SENDING START MESSAGE")
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
    logging.info(f"Sent cumulative ACK for packet {seq_num}")

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Reliable file receiver over UDP.')
parser.add_argument('server_ip', help='IP address of the server')
parser.add_argument('server_port', type=int, help='Port number of the server')
# parser.add_argument('counter')


args = parser.parse_args()

# Run the client
receive_file(args.server_ip, args.server_port)
