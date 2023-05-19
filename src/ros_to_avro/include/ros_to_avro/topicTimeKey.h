#include <string>


struct TopicTimeKey {
    std::string topic_name;
    std::string md5sum;
    unsigned long time_bin;

    TopicTimeKey(std::string _topic_name, std::string _md5sum, u_long _time_bin);

    bool operator==(const TopicTimeKey other) const;
};

template<>
struct std::hash<TopicTimeKey> {
    std::size_t operator()(const TopicTimeKey& c) const;
};