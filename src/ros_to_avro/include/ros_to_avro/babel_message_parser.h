#include <ros_babel_fish/messages/array_message.h>
#include <ros_babel_fish/message.h>
#include <ros_babel_fish/message_description.h>

#include <rapidjson/document.h>

#include <avro/Specific.hh>


namespace babel_message_parser {
    void parse_babel_fish_message(const ros_babel_fish::Message& msg, avro::Encoder& encoder);
    void parse_array_message(const ros_babel_fish::ArrayMessageBase& msg, avro::Encoder& encoder);

    std::string message_description_into_json(const ros_babel_fish::MessageDescription& msg_description);

    void parse_message_template(
        const ros_babel_fish::MessageTemplate& msg_template,
        rapidjson::Document& json_doc,
        rapidjson::Value& json_value,
        rapidjson::Value& type_key,
        bool optional
    );
};

template<>
struct avro::codec_traits<ros_babel_fish::Message> {
    static void encode(avro::Encoder& e, const ros_babel_fish::Message& msg) {
        babel_message_parser::parse_babel_fish_message(msg, e);
    }

    static void decode(avro::Decoder& d, ros_babel_fish::Message& msg) {
        std::logic_error("Not Implemented");
    }
};