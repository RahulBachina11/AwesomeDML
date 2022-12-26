from typing import final, List
from nodes import Node

M: final = 3

PING_TIMEOOUT: final = 2

PING_DURATION: final = 2.5

CLEANUP_TIME: final = 10

# H1: final = Node('127.0.0.1', 8001, 'H1')
# H2: final = Node('127.0.0.1', 8002, 'H2')
# H3: final = Node('127.0.0.1', 8003, 'H3')
# H4: final = Node('127.0.0.1', 8004, 'H4')
# H5: final = Node('127.0.0.1', 8005, 'H5')
# H6: final = Node('127.0.0.1', 8006, 'H6')
# H7: final = Node('127.0.0.1', 8007, 'H7')
# H8: final = Node('127.0.0.1', 8008, 'H8')
# H9: final = Node('127.0.0.1', 8009, 'H9')
# H10: final = Node('127.0.0.1', 8010, 'H10')

# Global nodes configuration
# ADD new nodes here...
H1: final = Node('fa22-cs425-6901.cs.illinois.edu', 8000, 'H1')
H2: final = Node('fa22-cs425-6902.cs.illinois.edu', 8000, 'H2')
H3: final = Node('fa22-cs425-6903.cs.illinois.edu', 8000, 'H3')
H4: final = Node('fa22-cs425-6904.cs.illinois.edu', 8000, 'H4')
H5: final = Node('fa22-cs425-6905.cs.illinois.edu', 8000, 'H5')
H6: final = Node('fa22-cs425-6906.cs.illinois.edu', 8000, 'H6')
H7: final = Node('fa22-cs425-6907.cs.illinois.edu', 8000, 'H7')
H8: final = Node('fa22-cs425-6908.cs.illinois.edu', 8000, 'H8')
H9: final = Node('fa22-cs425-6909.cs.illinois.edu', 8000, 'H9')
H10: final = Node('fa22-cs425-6910.cs.illinois.edu', 8000, 'H10')

# GLOBAL_RING_TOPOLOGY: dict = {

#     H1: [H2, H10, H5, H8],

#     H2: [H3, H1, H6, H9],

#     H3: [H4, H2, H7, H10],

#     H4: [H5, H3, H8, H1],

#     H5: [H6, H4, H9, H2],

#     H6: [H7, H5, H10, H3],

#     H7: [H8, H6, H1, H4],

#     H8: [H9, H7, H2, H5],

#     H9: [H10, H8, H3, H6],

#     H10: [H1, H9, H4, H7]

# }

# Current Ring topology
# Edit the ring as needed
GLOBAL_RING_TOPOLOGY: dict = {

    H1: [H2, H10, H5],

    H2: [H3, H1, H6],

    H3: [H4, H2, H7],

    H4: [H5, H3, H8],

    H5: [H6, H4, H9],

    H6: [H7, H5, H10],

    H7: [H8, H6, H1],

    H8: [H9, H7, H2],

    H9: [H10, H8, H3],

    H10: [H1, H9, H4]

}


class Config:
    """
    Config class which takes current hostname, port, introducer and testing flags from cmdline args. 
    """

    def __init__(self, hostname: str, port: int) -> None:

        self.node: Node = Node(hostname, port)
        # self.introducer = '127.0.0.1:8001'
        self.introducer = 'fa22-cs425-6901.cs.illinois.edu:8000'