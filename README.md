# ros_to_avro
Saves ROS messages to Avro format, binned by time and paritioned by topic.

## NOTE: THIS IS STILL WIP

## Setup
To run following dependencies need to be installed
```sh
sudo apt-get install rapidjson-dev libsnappy-dev
```
Note: `libsnappy-dev` is optional, and not needed if you only need

To install Avro, you can build it from source. See [their github page for releases](https://github.com/apache/avro/releases/).
