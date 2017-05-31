"""
Example receiving a message using the SimpleQueue interface.
"""

from kombu import Connection

import traceback

#: Create connection
#: If hostname, userid, password and virtual_host is not specified
#: the values below are the default, but listed here so it can
#: be easily changed.
with Connection('redis://127.0.0.1:6379/0') as conn:

    #: SimpleQueue mimics the interface of the Python Queue module.
    #: First argument can either be a queue name or a kombu.Queue object.
    #: If a name, then the queue will be declared with the name as the queue
    #: name, exchange name and routing key.
    with conn.SimpleQueue('kombu_demo') as queue:
        message = queue.get(block=True, timeout=10)
        message.ack()
        print(message.payload)
        print traceback.print_stack()

####
#: If you don't use the with statement then you must aways
# remember to close objects after use:
#   queue.close()
#   connection.close()
