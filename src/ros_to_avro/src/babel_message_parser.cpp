#include <ros/ros.h>
#include <ros_babel_fish/messages/compound_message.h>

#include "avro/DataFile.hh"
#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"

#include <boost/algorithm/string.hpp>

#include "../include/ros_to_avro/babel_message_parser.h"

const rapidjson::Value DEFAULT_TYPE_KEY = rapidjson::Value("type");
const rapidjson::Value ARRAY_TYPE_KEY = rapidjson::Value("items");

void babel_message_parser::parse_babel_fish_message(
    const ros_babel_fish::Message& msg,
    avro::Encoder& encoder
) {
    switch (msg.type()) {
        case ros_babel_fish::MessageTypes::Bool:
            encoder.encodeUnionIndex(0);
            encoder.encodeBool(msg.value<bool>());
            break;
        case ros_babel_fish::MessageTypes::Float32:
            encoder.encodeUnionIndex(0);
            encoder.encodeFloat(msg.value<float>());
            break;
        case ros_babel_fish::MessageTypes::Float64:
            encoder.encodeUnionIndex(0);
            encoder.encodeDouble(msg.value<double>());
            break;
        case ros_babel_fish::MessageTypes::Int8:
            {
                encoder.encodeUnionIndex(0);
                auto val = msg.value<int8_t>();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 1);
            }
            break;
        case ros_babel_fish::MessageTypes::Int16:
            {
                encoder.encodeUnionIndex(0);
                auto val = msg.value<int16_t>();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 2);
            }
            break;
        case ros_babel_fish::MessageTypes::Int32:
            encoder.encodeUnionIndex(0);
            encoder.encodeInt(msg.value<int>());
            break;
        case ros_babel_fish::MessageTypes::Int64:
            encoder.encodeUnionIndex(0);
            encoder.encodeLong(msg.value<long>());
            break;
        case ros_babel_fish::MessageTypes::UInt8:
            {
                encoder.encodeUnionIndex(0);
                auto val = msg.value<uint8_t>();
                encoder.encodeFixed(&val, 1);
            }
            break;
        case ros_babel_fish::MessageTypes::UInt16:
            {
                encoder.encodeUnionIndex(0);
                auto val = msg.value<uint16_t>();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 2);
            }
            break;
        case ros_babel_fish::MessageTypes::UInt32:
            {
                encoder.encodeUnionIndex(0);
                auto val = msg.value<uint32_t>();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 4);
            }
            break;
        case ros_babel_fish::MessageTypes::UInt64:
            {
                encoder.encodeUnionIndex(0);
                auto val = msg.value<uint64_t>();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 8);
            }
            break;
        case ros_babel_fish::MessageTypes::None:
            encoder.encodeUnionIndex(1);
            encoder.encodeNull();
            break;
        case ros_babel_fish::MessageTypes::String:
            encoder.encodeUnionIndex(0);
            encoder.encodeString(msg.value<std::string>());
            break;
        case ros_babel_fish::MessageTypes::Time:
            {
                encoder.encodeUnionIndex(0);
                auto msg_parsed = msg.value<ros::Time>();
                auto val = msg_parsed.toNSec();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 8);
            }
            break;
        case ros_babel_fish::MessageTypes::Duration:
            {
                encoder.encodeUnionIndex(0);
                auto msg_parsed = msg.value<ros::Duration>();
                auto val = msg_parsed.toNSec();
                encoder.encodeFixed(reinterpret_cast<uint8_t*>(&val), 8);
            }
            break;
        case ros_babel_fish::MessageTypes::Array:
            {
                encoder.arrayStart();
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessageBase>();
                auto array_len = array_msg.length();
                if (array_len != 0) {
                    encoder.setItemCount(array_len);
                    parse_array_message(array_msg, encoder);
                }
                encoder.arrayEnd();
            }
            break;
        case ros_babel_fish::MessageTypes::Compound:
            {
                auto& compound_msg = msg.as<ros_babel_fish::CompoundMessage>();
                auto& keys = compound_msg.keys();
                for(auto& key: keys) {
                    parse_babel_fish_message(compound_msg[key], encoder);
                }
            }
            break;
    };
}


void babel_message_parser::parse_array_message(
    const ros_babel_fish::ArrayMessageBase& msg,
    avro::Encoder& encoder
) {
    switch (msg.elementType()) {
        case ros_babel_fish::MessageTypes::Bool:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<bool>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeBool(array_msg[i]);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Float32:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<float>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeFloat(array_msg[i]);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Float64:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<double>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeDouble(array_msg[i]);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Int8:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<int8_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i];
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 1);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Int16:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<int16_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i];
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 2);
                }
            };
            break;
        case ros_babel_fish::MessageTypes::Int32:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<int32_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeInt(array_msg[i]);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Int64:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<int64_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeLong(array_msg[i]);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::UInt8:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<uint8_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i];
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 1);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::UInt16:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<uint16_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i];
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 2);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::UInt32:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<uint32_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i];
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 4);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::UInt64:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<uint64_t>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i];
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 8);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::None:
            {
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeNull();
                }
            }
            break;
        case ros_babel_fish::MessageTypes::String:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<std::string>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    encoder.encodeString(array_msg[i]);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Time:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<ros::Time>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i].toNSec();
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 8);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Duration:
            {
                auto& array_msg = msg.as<ros_babel_fish::ArrayMessage<ros::Duration>>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    auto value = array_msg[i].toNSec();
                    encoder.encodeFixed(reinterpret_cast<uint8_t*>(&value), 8);
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Array:
            {
                ROS_ERROR("Sir, This message is illegal. (Arrays of arrays are not ROSy)");
            }
            break;
        case ros_babel_fish::MessageTypes::Compound:
            {
                auto& array_msg = msg.as<ros_babel_fish::CompoundArrayMessage>();
                for (size_t i = 0; i < msg.length(); i++) {
                    encoder.startItem();
                    parse_babel_fish_message(array_msg[i], encoder);
                }
            }
            break;
    };
}


std::string babel_message_parser::message_description_into_json(const ros_babel_fish::MessageDescription& msg_description) {
    auto& msg_template = msg_description.message_template;
    rapidjson::Document json_doc;
    json_doc.SetObject();
    // rapidjson::Value json_value;
    // json_value.SetObject();
    auto type_key = rapidjson::Value("type", json_doc.GetAllocator());
    babel_message_parser::parse_message_template(
        *msg_template,
        json_doc,
        json_doc,
        // json_value,
        type_key,
        true
    );

    // 3. Stringify the DOM
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json_doc.Accept(writer);

    return buffer.GetString();
}

void babel_message_parser::parse_message_template(
    const ros_babel_fish::MessageTemplate& msg_template,
    rapidjson::Document& json_doc,
    rapidjson::Value& json_value,
    rapidjson::Value& type_key,
    bool optional
) {
    switch (msg_template.type) {
        case ros_babel_fish::MessageTypes::Bool:
            {
                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack("boolean", json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                } else {
                    type_val.SetString("boolean");
                }
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
            }
            break;
        case ros_babel_fish::MessageTypes::Float32:
            {
                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack("float", json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                } else {
                    type_val.SetString("float");
                }
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                break;
            }
        case ros_babel_fish::MessageTypes::Float64:
            {
                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack("double", json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                } else {
                    type_val.SetString("double");
                }
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                break;
            }
        case ros_babel_fish::MessageTypes::Int8:
            {
                rapidjson::Value fixed_val;
                fixed_val.SetObject();
                fixed_val.AddMember("type", "fixed", json_doc.GetAllocator());
                fixed_val.AddMember("name", "int8", json_doc.GetAllocator());
                fixed_val.AddMember("size", 1, json_doc.GetAllocator());

                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack(fixed_val, json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                    json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                } else {
                    json_value.AddMember(type_key, fixed_val, json_doc.GetAllocator());
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Int16:
            {
                rapidjson::Value fixed_val;
                fixed_val.SetObject();
                fixed_val.AddMember("type", "fixed", json_doc.GetAllocator());
                fixed_val.AddMember("name", "int16", json_doc.GetAllocator());
                fixed_val.AddMember("size", 2, json_doc.GetAllocator());

                if (optional) {
                    rapidjson::Value type_val;
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack(fixed_val, json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                    json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                } else {
                    json_value.AddMember(type_key, fixed_val, json_doc.GetAllocator());
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Int32:
            {
                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack("int", json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                } else {
                    type_val.SetString("int");
                }
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
            }
            break;
        case ros_babel_fish::MessageTypes::Int64:
            {
                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack("long", json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                } else {
                    type_val.SetString("long");
                }
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
            }
            break;
        case ros_babel_fish::MessageTypes::UInt8:
            {
                rapidjson::Value fixed_val;
                fixed_val.SetObject();
                fixed_val.AddMember("type", "fixed", json_doc.GetAllocator());
                fixed_val.AddMember("name", "uint8", json_doc.GetAllocator());
                fixed_val.AddMember("size", 1, json_doc.GetAllocator());

                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack(fixed_val, json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                    json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                } else {
                    json_value.AddMember(type_key, fixed_val, json_doc.GetAllocator());
                }
            }
            break;
        case ros_babel_fish::MessageTypes::UInt16:
            {
                rapidjson::Value fixed_val;
                fixed_val.SetObject();
                fixed_val.AddMember("type", "fixed", json_doc.GetAllocator());
                fixed_val.AddMember("name", "uint16", json_doc.GetAllocator());
                fixed_val.AddMember("size", 2, json_doc.GetAllocator());

                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack(fixed_val, json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                    json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                } else {
                    json_value.AddMember(type_key, fixed_val, json_doc.GetAllocator());
                }
            }
            break;
        case ros_babel_fish::MessageTypes::UInt32:
            {
                rapidjson::Value fixed_val;
                fixed_val.SetObject();
                fixed_val.AddMember("type", "fixed", json_doc.GetAllocator());
                fixed_val.AddMember("name", "uint32", json_doc.GetAllocator());
                fixed_val.AddMember("size", 4, json_doc.GetAllocator());

                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack(fixed_val, json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                    json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                } else {
                    json_value.AddMember(type_key, fixed_val, json_doc.GetAllocator());
                }
            }
            break;
        case ros_babel_fish::MessageTypes::Duration:
        case ros_babel_fish::MessageTypes::Time:
        case ros_babel_fish::MessageTypes::UInt64:
            {
                rapidjson::Value fixed_val;
                fixed_val.SetObject();
                fixed_val.AddMember("type", "fixed", json_doc.GetAllocator());
                fixed_val.AddMember("name", "uint64", json_doc.GetAllocator());
                fixed_val.AddMember("size", 8, json_doc.GetAllocator());

                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack(fixed_val, json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                    json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                } else {
                    json_value.AddMember(type_key, fixed_val, json_doc.GetAllocator());
                }
            }
            break;
        case ros_babel_fish::MessageTypes::None:
            {
                rapidjson::Value type_val;
                type_val.SetString("null");
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
            }
            break;
        case ros_babel_fish::MessageTypes::String:
            {
                rapidjson::Value type_val;
                if (optional) {
                    type_val.SetArray();
                    type_val.Reserve(2, json_doc.GetAllocator());
                    type_val.PushBack("string", json_doc.GetAllocator());
                    type_val.PushBack("null", json_doc.GetAllocator());
                } else {
                    type_val.SetString("string");
                }
                json_value.AddMember(type_key, type_val, json_doc.GetAllocator());
                break;
            }
        case ros_babel_fish::MessageTypes::Array:
            {
                json_value.AddMember(type_key, "array", json_doc.GetAllocator());

                auto array_type_key = rapidjson::Value("items");
                parse_message_template(
                    *msg_template.array.element_template,
                    json_doc,
                    json_value,
                    array_type_key,
                    false
                );

                break;
            }
        case ros_babel_fish::MessageTypes::Compound:
            {
                json_value.AddMember(type_key, "record", json_doc.GetAllocator());
                rapidjson::Value name_value;
                auto name = msg_template.compound.datatype;
                boost::replace_all(name, "/", "_");
                name_value.SetString(name.data(), name.length(), json_doc.GetAllocator());
                json_value.AddMember("name", name_value, json_doc.GetAllocator());
                auto& field_names = msg_template.compound.names;
                auto& field_types = msg_template.compound.types;

                rapidjson::Value element_iter_value;
                rapidjson::Value element_iter_name_value;

                rapidjson::Value elements_value;
                elements_value.SetArray();
                elements_value.Reserve(field_names.size(), json_doc.GetAllocator());
                for (size_t idx = 0; idx < field_names.size(); idx++) {
                    auto compound_type_key = rapidjson::Value("type");
                    auto field_name = field_names[idx];
                    element_iter_name_value.SetString(field_name.data(), field_name.length(), json_doc.GetAllocator());

                    auto field_type = field_types[idx];

                    element_iter_value.SetObject();
                    element_iter_value.AddMember("name", element_iter_name_value, json_doc.GetAllocator());

                    // Schema is different on the outside here sadly
                    switch (field_type->type) {
                        case ros_babel_fish::MessageTypes::Compound:
                        case ros_babel_fish::MessageTypes::Array:
                        {
                            rapidjson::Value inner_record_type;
                            inner_record_type.SetObject();
                            parse_message_template(
                                *field_types[idx],
                                json_doc,
                                inner_record_type,
                                compound_type_key,
                                true
                            );
                            element_iter_value.AddMember("type", inner_record_type, json_doc.GetAllocator());
                        }
                        break;
                        default:
                        {
                            parse_message_template(
                                *field_types[idx],
                                json_doc,
                                element_iter_value,
                                compound_type_key,
                                true
                            );
                        }
                        break;
                    };

                    elements_value.PushBack(element_iter_value, json_doc.GetAllocator());
                }

                json_value.AddMember("fields", elements_value, json_doc.GetAllocator());
            }
            break;
    }
}