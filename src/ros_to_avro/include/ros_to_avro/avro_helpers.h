#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Generic.hh"
#include "avro/ValidSchema.hh"

typedef std::array<uint8_t, 16> DataFileSync;
typedef std::shared_ptr<avro::OutputStream> SharedOutStream;

DataFileSync makeSync() {
    DataFileSync sync;
    std::generate(sync.begin(), sync.end(), random);
    return sync;
}


struct AvroWriterComponents {
    SharedOutStream out_stream;
    avro::EncoderPtr encoder;
    DataFileSync sync_marker;
    avro::ValidSchema schema;

    AvroWriterComponents(std::string& filename, avro::ValidSchema& schema);

    void writeHeader();
};
