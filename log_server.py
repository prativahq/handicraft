import http.server
import base64
from io import BytesIO

# Credentials (username and password)
USERNAME = 'admin'
PASSWORD = 'abcd1234'

# File path to serve
FILE_PATH = 'script.log'

# Basic Authentication header format
def check_auth(request_headers):
    """Check if the authorization header matches the credentials."""
    if 'Authorization' not in request_headers:
        return False

    auth_type, auth_string = request_headers['Authorization'].split(' ', 1)
    if auth_type.lower() != 'basic':
        return False

    # Decode the base64 string and check credentials
    decoded = base64.b64decode(auth_string).decode('utf-8')
    user, password = decoded.split(':', 1)

    return user == USERNAME and password == PASSWORD

class MyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # Check if the user is authenticated
        if not check_auth(self.headers):
            # If not authenticated, send a 401 Unauthorized response
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="My Protected Area"')
            self.end_headers()
            self.wfile.write(b'Authentication required\n')
            return

        # If authenticated, read the file and send its contents
        try:
            with open(FILE_PATH, 'r') as file:
                file_content = file.read()
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(file_content.encode('utf-8'))
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'File not found\n')

def run(server_class=http.server.HTTPServer, handler_class=MyHandler, port=8080):
    server_address = ('0.0.0.0', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run(port=8080)
