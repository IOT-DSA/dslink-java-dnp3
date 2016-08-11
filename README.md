# dslink-java-dnp3
A DSLink for communication over the dnp3 protocol

## Usage
Connect to a DNP3 outstation using the "add serial outstation" and "add ip outstation" actions. Note that "Master Address" can be set to pretty much anything, although the device may need to be configured to accept commands from that address. 

DNP3 allows two types of queries for data: static and event. A query for static data will return the values of all of the outstation's points. A query for event data will return the values of only those points that have triggered an event since the last such query. Usually, a point triggers an event when its value is changed, however this depends on how the device is configured. In this DSLink, you can specify both how often the outstation is polled for event data, and how often it is polled for static data (as an integrity poll).

The "discover" action simply triggers a static data query, and the results are used to create nodes for all of the outstation's
data points. After this, the outstation will be polled according to the specified polling intervals as long as at least one of these nodes is subscribed to.

Analog Output and Binary Output values can be written to by setting their nodes' values.
