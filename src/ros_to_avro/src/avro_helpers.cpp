#include "../include/ros_to_avro/avro_helpers.h"


typedef std::map<std::string, std::vector<uint8_t>> Metadata;
typedef std::array<uint8_t, 4> Magic;
static Magic magic = {{'O', 'b', 'j', '\x01'}};

const std::string AVRO_SCHEMA_KEY("avro.schema");
const std::string AVRO_CODEC_KEY("avro.codec");
const std::string AVRO_NULL_CODEC("null");
const std::string AVRO_DEFLATE_CODEC("deflate");


void setMetadata(Metadata& metadata, const std::string& key, const std::string& value) {
    std::vector<uint8_t> v(value.size());
    copy(value.begin(), value.end(), v.begin());
    metadata[key] = v;
}


AvroWriterComponents::AvroWriterComponents(std::string& filename, avro::ValidSchema& schema) {
    // Create a values writer
    auto file_out_uniq = avro::fileOutputStream(filename.c_str());
    SharedOutStream out_stream = std::move(file_out_uniq);
    encoder = avro::binaryEncoder();
    encoder->init(*out_stream);
    sync_marker = makeSync();

    writeHeader();
    encoder->flush();
}

void AvroWriterComponents::writeHeader() {
    Metadata metadata;

    setMetadata(metadata, AVRO_CODEC_KEY, AVRO_NULL_CODEC);
    setMetadata(metadata, AVRO_CODEC_KEY, AVRO_NULL_CODEC);
    setMetadata(metadata, AVRO_SCHEMA_KEY, schema.toJson(false));

    avro::encode(*encoder, magic);
    avro::encode(*encoder, metadata);
    avro::encode(*encoder, sync_marker);
};
