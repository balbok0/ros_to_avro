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

    AvroWriterComponents(const AvroWriterComponents& w):
        out_stream(std::move(w.out_stream)),
        encoder(w.encoder),
        sync_marker(sync_marker),
        schema(schema)
    {}

    void writeHeader();

    void flush() {encoder->flush(); out_stream->flush();}
};


/**
 *  An Avro datafile that can store objects of type T.
 */
template<typename T>
class DataFileWriter {
    std::shared_ptr<avro::DataFileWriterBase> base_;

public:
    /**
     * Constructs a new data file.
     */
    DataFileWriter(
        const char *filename, const avro::ValidSchema &schema,
        size_t syncInterval = 16 * 1024, avro::Codec codec = avro::NULL_CODEC
    ) :
        base_(new avro::DataFileWriterBase(filename, schema, syncInterval, codec))
    {}

    DataFileWriter(
        std::unique_ptr<avro::OutputStream> outputStream, const avro::ValidSchema &schema,
        size_t syncInterval = 16 * 1024, avro::Codec codec = avro::NULL_CODEC
    ) :
        base_(new avro::DataFileWriterBase(std::move(outputStream), schema, syncInterval, codec))
    {}

    /**
     * Writes the given piece of data into the file.
     */
    void write(const T &datum) {
        base_->syncIfNeeded();
        avro::encode(base_->encoder(), datum);
        base_->incr();
    }

    /**
     *  Returns the byte offset (within the current file) of the start of the current block being written.
     */
    uint64_t getCurrentBlockStart() { return base_->getCurrentBlockStart(); }

    /**
     * Closes the current file. Once closed this datafile object cannot be
     * used for writing any more.
     */
    void close() { base_->close(); }

    /**
     * Returns the schema for this data file.
     */
    const avro::ValidSchema &schema() const { return base_->schema(); }

    /**
     * Flushes any unwritten data into the file.
     */
    void flush() { base_->flush(); }
};