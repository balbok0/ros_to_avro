#include <ros/ros.h>
#include <ros/master.h>

#include <ros_babel_fish/babel_fish.h>
#include <ros_babel_fish/babel_fish_message.h>
#include <ros_babel_fish/message_types.h>
#include <ros_babel_fish/messages/array_message.h>
#include <ros_babel_fish/messages/compound_message.h>
#include <ros_babel_fish/messages/value_message.h>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"
#include "avro/ValidSchema.hh"

#include "../include/ros_to_avro/topicTimeKey.h"
#include "../include/ros_to_avro/babel_message_parser.h"
#include "../include/ros_to_avro/shared_datafile_writer.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

ros_babel_fish::BabelFish *fish;

typedef std::shared_ptr<avro::OutputStream> SharedOutStream;
typedef std::shared_ptr<SharedDataFileWriter<ros_babel_fish::BabelFishMessage::ConstPtr&>> TypedSharedWriter;

void topicCallback(
    const ros_babel_fish::BabelFishMessage::ConstPtr& msg,
    std::string topic_name,
    std::unordered_map<std::string, avro::ValidSchema>& types_to_avro,
    std::unordered_map<TopicTimeKey, SharedOutStream>& topic_file_handles,
    std::unordered_map<TopicTimeKey, avro::EncoderPtr>& topic_encoders,
    std::unordered_map<TopicTimeKey, avro::EncoderPtr>& topic_encoders,
) {
    // TODO: Make configurable
    auto time_bin = ros::Time::now().toNSec() / (300000000000uL); // Every 5 minutes
    auto md5sum = msg->md5Sum();
    TopicTimeKey key(topic_name, md5sum, time_bin);

    avro::EncoderPtr encoder;
    auto encoder_find_result = topic_encoders.find(key);

    if (encoder_find_result == topic_encoders.end()) {
        TopicTimeKey prev_key(topic_name, md5sum, time_bin);
        if (topic_encoders.find(prev_key) != topic_encoders.end()) {
            topic_encoders[prev_key]->flush();
            topic_file_handles[prev_key]->flush();

            topic_encoders.erase(prev_key);
            topic_file_handles.erase(prev_key);
        }

        // Create file writer
        std::stringstream file_path;
        file_path << "/tmp/ros_to_avro/timeBin=" << time_bin << "/";
        std::string time_bin_folder = file_path.str();
        boost::filesystem::create_directories(time_bin_folder);
        file_path << boost::replace_all_copy(topic_name, "/", ".");
        file_path << ".avrobin";
        std::string file_path_str = file_path.str();

        std::printf("Creating new file @ path %s\n", file_path_str.c_str());
        // Write schema first.
        // Since DataFileWriter is noncopyable, and there is a relatively big overhead of persisiting it in memory over extended amount of time.
        // As such we only use it in the beginning to write schema. Then we use FileOutputStream directly.
        auto schema = types_to_avro[msg->dataType()];
        avro::DataFileWriter<ros_babel_fish::Message::Ptr> writer(file_path_str.c_str(), schema);
        writer.flush();
        writer.close();

        // Create a values writer
        auto file_out_uniq = avro::fileOutputStream(file_path_str.c_str());
        SharedOutStream file_out = std::move(file_out_uniq);

        encoder = avro::binaryEncoder();
        encoder->init(*file_out);

        topic_encoders.insert({key, encoder});
        topic_file_handles.insert(std::make_pair(key, file_out));
    } else {
        encoder = encoder_find_result->second;
    }

    ros_babel_fish::TranslatedMessage::Ptr outer_translated_msg = fish->translateMessage(msg);
    ros_babel_fish::Message::Ptr translated_msg = outer_translated_msg->translated_message;
    babel_message_parser::parse_babel_fish_message(*translated_msg, *encoder);

    // Write to file
    encoder->flush();
    topic_file_handles[key]->flush();
}

void populateTopics(
    ros::NodeHandle& nh,
    std::set<std::string>& known_topics,
    std::vector<std::string>& known_topics_vec,
    std::vector<ros::Subscriber>& subscribers,
    std::unordered_map<std::string, rapidjson::Value>& typesToJson,
    std::unordered_map<std::string, avro::ValidSchema>& typesToAvro,
    std::unordered_map<TopicTimeKey, SharedOutStream>& topicFileHandles,
    std::unordered_map<TopicTimeKey, avro::EncoderPtr>& topicEncoders
) {
    printf("Looping though topics\n");
    // Get topics
    ros::master::V_TopicInfo topic_infos;
    ros::master::getTopics(topic_infos);

    std::string topic_name;
    for(auto topic: topic_infos) {
        topic_name = topic.name;
        if (known_topics.find(topic.name) == known_topics.end()) {
            ROS_INFO("Topic: %s Datatype %s\n", topic_name.c_str(), topic.datatype.c_str());

            // Create avro encoder
            int topic_idx = known_topics.size();
            known_topics.emplace(topic_name);
            known_topics_vec.push_back(topic_name);
            auto message_description = fish->descriptionProvider()->getMessageDescription(topic.datatype);

            auto avro_schema_json = babel_message_parser::message_description_into_json(*message_description);
            avro::ValidSchema avro_schema;
            printf("Json Schema: %s\n", avro_schema_json.c_str());
            avro::compileJsonSchemaFromString(avro_schema_json);
            typesToAvro.insert({topic_name, avro_schema});

            // Alternative


            // Subscribe
            boost::function<void(const ros_babel_fish::BabelFishMessage::ConstPtr&)> callback;
            callback = [&known_topics_vec, topic_idx, &typesToAvro, &topicFileHandles, &topicEncoders](const ros_babel_fish::BabelFishMessage::ConstPtr& msg) -> void {
                printf("topic inside callback: %s\n", known_topics_vec[topic_idx].c_str());
                topicCallback(msg, known_topics_vec[topic_idx], typesToAvro, topicFileHandles, topicEncoders);
            };
            subscribers.push_back(nh.subscribe<ros_babel_fish::BabelFishMessage>(topic_name, 10, callback));

        }
    }
}

int main(int argc, char** argv)
{
    ros::init(argc, argv, "ros_to_avro");
    ros::NodeHandle nh;

    int bin_size_sec = nh.param<int>("/bin_size_sec", 600);
    float topic_lookup_sec = nh.param<float>("/topic_lookup_sec", 1);

    fish = new ros_babel_fish::BabelFish;

    // Topics objects
    std::set<std::string> all_topics;
    std::vector<std::string> all_topics_vec;
    std::vector<ros::Subscriber> subscribers;

    // Avro handles
    std::unordered_map<std::string, rapidjson::Value> typesToJson; // Allows for much faster transcribing of ROS message types
    std::unordered_map<std::string, avro::ValidSchema> typesToAvroSchema;
    std::unordered_map<TopicTimeKey, SharedOutStream> topicFileHandles;
    std::unordered_map<TopicTimeKey, TypedSharedWriter> topicFileWriters;
    // std::unordered_map<TopicTimeKey, avro::DataFileWriter<ros_babel_fish::BabelFishMessage>> topicFileWriters;
    std::unordered_map<TopicTimeKey, avro::EncoderPtr> topicEncoders;

    // RosMsgParser::ParsersCollection parsers;

    std::string topic_name;

    boost::function<void(const ros::TimerEvent&)> timerCallback;
    timerCallback = [
        &nh, &all_topics, &all_topics_vec, &subscribers, &typesToJson, &typesToAvroSchema, &topicFileHandles, &topicEncoders
    ](const ros::TimerEvent& _event) -> void {
        populateTopics(nh, all_topics, all_topics_vec, subscribers, typesToJson, typesToAvroSchema, topicFileHandles, topicEncoders);
    };
    ros::Timer timer = nh.createTimer(ros::Duration(topic_lookup_sec), timerCallback);
    timer.start();

    ros::spin();
    return 0;
}