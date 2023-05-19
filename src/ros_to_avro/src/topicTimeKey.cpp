#include "../include/ros_to_avro/topicTimeKey.h"
#include <boost/functional/hash.hpp>


TopicTimeKey::TopicTimeKey(std::string _topic_name, std::string _md5sum, u_long _time_bin) {
    topic_name = _topic_name;
    md5sum = _md5sum;
    time_bin = _time_bin;
}

bool TopicTimeKey::operator==(const TopicTimeKey other) const {
    return other.topic_name==this->topic_name && other.time_bin==this->time_bin && other.md5sum == this->md5sum;
}

std::size_t std::hash<TopicTimeKey>::operator()(const TopicTimeKey& c) const
{
    std::size_t result = 0;
    boost::hash_combine(result, c.topic_name);
    boost::hash_combine(result, c.md5sum);
    boost::hash_combine(result, c.time_bin);
    return result;
}
