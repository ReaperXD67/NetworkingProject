# Multi-threaded HTTP Server Implementation
import socket
import threading
import json
import os
import time
import sys
import re
from datetime import datetime, timezone
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor
import queue
import hashlib
import random
import string

class HTTPServer:
    def __init__(self, host='127.0.0.1', port=8080, max_threads=10):
        self.host = host
        self.port = port
        self.max_threads = max_threads
        self.server_socket = None
        self.thread_pool = None
        self.connection_queue = queue.Queue()
        self.active_connections = 0
        self.lock = threading.Lock()
        
        # Create directories if they don't exist
        if not os.path.exists('resources'):
            os.makedirs('resources')
        if not os.path.exists('resources/uploads'):
            os.makedirs('resources/uploads')
    
    def log(self, message, thread_name=None):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if thread_name:
            print(f"[{timestamp}] [{thread_name}] {message}")
        else:
            print(f"[{timestamp}] {message}")
    
    def start_server(self):
        try:
            # Create socket
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind and listen
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(50)
            
            # Create thread pool
            self.thread_pool = ThreadPoolExecutor(max_workers=self.max_threads)
            
            self.log(f"HTTP Server started on http://{self.host}:{self.port}")
            self.log(f"Thread pool size: {self.max_threads}")
            self.log("Serving files from 'resources' directory")
            self.log("Press Ctrl+C to stop the server")
            
            while True:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    
                    # Submit to thread pool
                    future = self.thread_pool.submit(self.handle_client, client_socket, client_address)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self.log(f"Error accepting connection: {e}")
                    
        except Exception as e:
            self.log(f"Server error: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        self.log("Shutting down server...")
        if self.server_socket:
            
            self.server_socket.close()
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
    
    def handle_client(self, client_socket, client_address):
        thread_name = threading.current_thread().name
        self.log(f"Connection from {client_address[0]}:{client_address[1]}", thread_name)
        
        try:
            # Set socket timeout for persistent connections
            client_socket.settimeout(30)
            
            request_count = 0
            max_requests = 100
            
            while request_count < max_requests:
                try:
                    # Receive request
                    request_data = client_socket.recv(8192)
                    if not request_data:
                        break
                    
                    request_count += 1
                    
                    # Parse HTTP request
                    request = self.parse_http_request(request_data.decode('utf-8', errors='ignore'))
                    if not request:
                        self.send_error_response(client_socket, 400, "Bad Request")
                        break
                    
                    self.log(f"Request: {request['method']} {request['path']} HTTP/{request['version']}", thread_name)
                    
                    # Validate Host header
                    if not self.validate_host_header(request):
                        self.log("Host validation failed", thread_name)
                        self.send_error_response(client_socket, 403, "Forbidden")
                        break
                    
                    self.log(f"Host validation: {request['headers'].get('host', 'N/A')} ✓", thread_name)
                    
                    # Handle request
                    keep_alive = self.handle_request(client_socket, request, thread_name)
                    
                    if not keep_alive:
                        break
                        
                except socket.timeout:
                    self.log("Connection timeout", thread_name)
                    break
                except Exception as e:
                    self.log(f"Error handling request: {e}", thread_name)
                    break
                    
        finally:
            client_socket.close()
            self.log(f"Connection closed", thread_name)
    
    def parse_http_request(self, request_data):
        try:
            lines = request_data.split('\r\n')
            if not lines:
                return None
            
            # Parse request line
            request_line = lines[0]
            parts = request_line.split(' ')
            if len(parts) != 3:
                return None
            
            method, path, version = parts
            version = version.replace('HTTP/', '')
            
            # Parse headers
            headers = {}
            body_start = 0
            for i, line in enumerate(lines[1:], 1):
                if line == '':
                    body_start = i + 1
                    break
                if ':' in line:
                    key, value = line.split(':', 1)
                    headers[key.strip().lower()] = value.strip()
            
            # Get body
            body = '\r\n'.join(lines[body_start:]) if body_start < len(lines) else ''
            
            return {
                'method': method,
                'path': unquote(path),
                'version': version,
                'headers': headers,
                'body': body
            }
            
        except Exception:
            return None
    
    def validate_host_header(self, request):
        host = request['headers'].get('host')
        if not host:
            return False
        
        # Valid hosts
        valid_hosts = [
            f'localhost:{self.port}',
            f'127.0.0.1:{self.port}',
            'localhost',
            '127.0.0.1'
        ]
        
        return host in valid_hosts
    
    def validate_path(self, path):
        # Security: Prevent path traversal
        if '..' in path or '//' in path or path.startswith('/'):
            if path != '/':  # Allow root path
                return None
        
        # Normalize path
        if path == '/':
            path = '/index.html'
        
        # Remove leading slash
        if path.startswith('/'):
            path = path[1:]
        
        # Check if path tries to escape resources directory
        full_path = os.path.join('resources', path)
        if not full_path.startswith('resources'):
            return None
        
        return full_path
    
    def handle_request(self, client_socket, request, thread_name):
        method = request['method']
        path = request['path']
        version = request['version']
        headers = request['headers']
        
        # Determine connection type
        connection = headers.get('connection', '').lower()
        keep_alive = (version == '1.1' and connection != 'close') or connection == 'keep-alive'
        
        try:
            if method == 'GET':
                return self.handle_get_request(client_socket, path, keep_alive, thread_name)
            elif method == 'POST':
                return self.handle_post_request(client_socket, request, keep_alive, thread_name)
            else:
                self.send_error_response(client_socket, 405, "Method Not Allowed")
                return False
                
        except Exception as e:
            self.log(f"Error handling request: {e}", thread_name)
            self.send_error_response(client_socket, 500, "Internal Server Error")
            return False
    
    def handle_get_request(self, client_socket, path, keep_alive, thread_name):
        # Validate and get file path
        file_path = self.validate_path(path)
        if not file_path:
            self.log("Path validation failed - Forbidden", thread_name)
            self.send_error_response(client_socket, 403, "Forbidden")
            return False
        
        if not os.path.exists(file_path):
            self.log(f"File not found: {file_path}", thread_name)
            self.send_error_response(client_socket, 404, "Not Found")
            return False
        
        # Get file extension
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        # Determine content type
        if ext == '.html':
            content_type = 'text/html; charset=utf-8'
            binary_mode = False
        elif ext in ['.txt', '.png', '.jpg', '.jpeg']:
            content_type = 'application/octet-stream'
            binary_mode = True
        else:
            self.send_error_response(client_socket, 415, "Unsupported Media Type")
            return False
        
        try:
            # Read file
            mode = 'rb' if binary_mode else 'r'
            encoding = None if binary_mode else 'utf-8'
            
            with open(file_path, mode, encoding=encoding) as f:
                file_content = f.read()
            
            if binary_mode:
                self.log(f"Sending binary file: {os.path.basename(file_path)} ({len(file_content)} bytes)", thread_name)
                self.send_binary_response(client_socket, file_content, content_type, os.path.basename(file_path), keep_alive)
            else:
                file_size = len(file_content.encode('utf-8'))
                self.log(f"Sending HTML file: {os.path.basename(file_path)} ({file_size} bytes)", thread_name)
                self.send_html_response(client_socket, file_content, keep_alive)
            
            self.log(f"Response: 200 OK ({len(file_content)} bytes transferred)", thread_name)
            self.log(f"Connection: {'keep-alive' if keep_alive else 'close'}", thread_name)
            
            return keep_alive
            
        except Exception as e:
            self.log(f"Error reading file {file_path}: {e}", thread_name)
            self.send_error_response(client_socket, 500, "Internal Server Error")
            return False
    
    def handle_post_request(self, client_socket, request, keep_alive, thread_name):
        headers = request['headers']
        content_type = headers.get('content-type', '')
        
        # Check content type
        if content_type != 'application/json':
            self.send_error_response(client_socket, 415, "Unsupported Media Type")
            return False
        
        try:
            # Parse JSON
            json_data = json.loads(request['body'])
        except json.JSONDecodeError:
            self.send_error_response(client_socket, 400, "Bad Request")
            return False
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        filename = f"upload_{timestamp}_{random_id}.json"
        file_path = os.path.join('resources', 'uploads', filename)
        
        try:
            # Save JSON to file
            with open(file_path, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            # Create response
            response_data = {
                "status": "success",
                "message": "File created successfully",
                "filepath": f"/uploads/{filename}"
            }
            
            self.send_json_response(client_socket, response_data, 201, keep_alive)
            self.log(f"JSON file created: {filename}", thread_name)
            
            return keep_alive
            
        except Exception as e:
            self.log(f"Error saving JSON file: {e}", thread_name)
            self.send_error_response(client_socket, 500, "Internal Server Error")
            return False
    
    def get_http_date(self):
        return datetime.now(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')
    
    def send_html_response(self, client_socket, content, keep_alive=True):
        content_bytes = content.encode('utf-8')
        
        headers = [
            'HTTP/1.1 200 OK',
            'Content-Type: text/html; charset=utf-8',
            f'Content-Length: {len(content_bytes)}',
            f'Date: {self.get_http_date()}',
            'Server: Multi-threaded HTTP Server',
            f'Connection: {"keep-alive" if keep_alive else "close"}'
        ]
        
        if keep_alive:
            headers.append('Keep-Alive: timeout=30, max=100')
        
        response = '\r\n'.join(headers) + '\r\n\r\n'
        client_socket.sendall(response.encode('utf-8') + content_bytes)
    
    def send_binary_response(self, client_socket, content, content_type, filename, keep_alive=True):
        headers = [
            'HTTP/1.1 200 OK',
            f'Content-Type: {content_type}',
            f'Content-Length: {len(content)}',
            f'Content-Disposition: attachment; filename="{filename}"',
            f'Date: {self.get_http_date()}',
            'Server: Multi-threaded HTTP Server',
            f'Connection: {"keep-alive" if keep_alive else "close"}'
        ]
        
        if keep_alive:
            headers.append('Keep-Alive: timeout=30, max=100')
        
        response = '\r\n'.join(headers) + '\r\n\r\n'
        client_socket.sendall(response.encode('utf-8'))
        client_socket.sendall(content)
    
    def send_json_response(self, client_socket, data, status_code=200, keep_alive=True):
        status_messages = {
            200: 'OK',
            201: 'Created',
            400: 'Bad Request',
            403: 'Forbidden',
            404: 'Not Found',
            405: 'Method Not Allowed',
            415: 'Unsupported Media Type',
            500: 'Internal Server Error'
        }
        
        content = json.dumps(data)
        content_bytes = content.encode('utf-8')
        
        headers = [
            f'HTTP/1.1 {status_code} {status_messages.get(status_code, "Unknown")}',
            'Content-Type: application/json',
            f'Content-Length: {len(content_bytes)}',
            f'Date: {self.get_http_date()}',
            'Server: Multi-threaded HTTP Server',
            f'Connection: {"keep-alive" if keep_alive else "close"}'
        ]
        
        if keep_alive:
            headers.append('Keep-Alive: timeout=30, max=100')
        
        response = '\r\n'.join(headers) + '\r\n\r\n'
        client_socket.sendall(response.encode('utf-8') + content_bytes)
    
    def send_error_response(self, client_socket, status_code, message):
        status_messages = {
            400: 'Bad Request',
            403: 'Forbidden',
            404: 'Not Found',
            405: 'Method Not Allowed',
            415: 'Unsupported Media Type',
            500: 'Internal Server Error',
            503: 'Service Unavailable'
        }
        
        content = f"<html><body><h1>{status_code} {message}</h1></body></html>"
        content_bytes = content.encode('utf-8')
        
        headers = [
            f'HTTP/1.1 {status_code} {status_messages.get(status_code, message)}',
            'Content-Type: text/html; charset=utf-8',
            f'Content-Length: {len(content_bytes)}',
            f'Date: {self.get_http_date()}',
            'Server: Multi-threaded HTTP Server',
            'Connection: close'
        ]
        
        if status_code == 503:
            headers.append('Retry-After: 60')
        
        response = '\r\n'.join(headers) + '\r\n\r\n'
        try:
            client_socket.sendall(response.encode('utf-8') + content_bytes)
        except:
            pass

def main():
    # Parse command line arguments
    host = '127.0.0.1'
    port = 8080
    max_threads = 10
    
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    if len(sys.argv) > 2:
        host = sys.argv[2]
    if len(sys.argv) > 3:
        max_threads = int(sys.argv[3])
    
    # Create and start server
    server = HTTPServer(host, port, max_threads)
    
    try:
        server.start_server()
    except KeyboardInterrupt:
        server.log("Server stopped by user")
    finally:
        server.shutdown()

if __name__ == '__main__':
    main()

print("Server code generated successfully!")