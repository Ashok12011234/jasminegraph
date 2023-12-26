#include <string>
#include <chrono>
#include <nlohmann/json.hpp>
#include "StreamHandler.h"
#include "../logger/Logger.h"

using json = nlohmann::json;
using namespace std;
using namespace std::chrono;

StreamHandler::StreamHandler(KafkaConnector *kstream, Partitioner &graphPartitioner, vector<DataPublisher *> &workerClients)
        : kstream(kstream), graphPartitioner(graphPartitioner), workerClients(workerClients), stream_topic_name("stream_topic_name") {
}

// Polls kafka for a message.
cppkafka::Message StreamHandler::pollMessage() {
    return kstream->consumer.poll(std::chrono::milliseconds(1000));
}

// Checks if there's an error in Kafka's message.
bool StreamHandler::isErrorInMessage(const cppkafka::Message &msg) {
    if (!msg || msg.get_error()) {
        frontend_logger.log("Couldn't retrieve message from Kafka.", "info");
        return true;
    }
    return false;
}

// Ends the stream if the end message("-1") has been received.
bool StreamHandler::isEndOfStream(const cppkafka::Message &msg) {
    std::string data(msg.get_payload());
    if (data == "-1") {
        frontend_logger.info("Received the end of `" + stream_topic_name + "` input kafka stream");
        return true;
    }
    return false;
}

void StreamHandler::listen_to_kafka_topic() {
    while (true) {
        cppkafka::Message msg = this->pollMessage();

        if (this->isEndOfStream(msg)) {
            frontend_logger.info("Received the end of `" + stream_topic_name + "` input kafka stream");
            break;
        }

        if (this->isErrorInMessage(msg)) {
            frontend_logger.log("Couldn't retrieve message from Kafka.", "info");
            continue;
        }

        string data(msg.get_payload());
        auto edgeJson = json::parse(data);
        auto sourceJson = edgeJson["source"];
        auto destinationJson = edgeJson["destination"];
        string sId = std::string(sourceJson["id"]);
        string dId = std::string(destinationJson["id"]);
        partitionedEdge partitionedEdge = graphPartitioner.addEdge({sId, dId});
        sourceJson["pid"] = partitionedEdge[0].second;
        destinationJson["pid"] = partitionedEdge[1].second;
        string source = sourceJson.dump();
        string destination = destinationJson.dump();
        json obj;
        obj["source"] = sourceJson;
        obj["destination"] = destinationJson;
        obj["properties"] = edgeJson["properties"];
        long temp_s = partitionedEdge[0].second;
        long temp_d = partitionedEdge[1].second;

        // Storing Node block
        if (temp_s == temp_d) {
            obj["EdgeType"] = "Local";
            obj["PID"] = temp_s;
            workerClients.at(temp_s)->publish(obj.dump());
        }
        else {
            obj["EdgeType"] = "Central";
            obj["PID"] = temp_s;
            workerClients.at(temp_s)->publish(obj.dump());
            obj["PID"] = temp_d;
            workerClients.at(temp_d)->publish(obj.dump());
        }
    }

    graphPartitioner.printStats();
}
