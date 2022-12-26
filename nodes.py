class Node():
    """Node class abstraction for a machine in ring topology"""
    def __init__(self, host, port, username, password, name = None) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._name = f'{self._host}:{self._port}'
        if name:
            self._name = name
    
    @property
    def host(self):
        return self._host
    
    @property
    def port(self):
        return self._port
    
    @property
    def name(self):
        return self._name
    
    @property
    def unique_name(self):
        return f'{self.host}:{self.port}'
    
    @property
    def username(self):
        return self._username
    
    @property
    def password(self):
        return self._password
