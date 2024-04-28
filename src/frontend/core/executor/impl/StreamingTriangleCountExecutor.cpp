/**
Copyright 2020-2024 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**/

#include "StreamingTriangleCountExecutor.h"

#define DATA_BUFFER_SIZE (FRONTEND_DATA_LENGTH + 1)
using namespace std::chrono;

std::map<int, int> StreamingTriangleCountExecutor::local_socket_map; // port:socket
std::map<int, int> StreamingTriangleCountExecutor::central_socket_map; // port:socket

Logger streaming_triangleCount_logger;
std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>>
        StreamingTriangleCountExecutor::triangleTree;
long StreamingTriangleCountExecutor::triangleCount;
StreamingSnap StreamingTriangleCountExecutor::streamingSnap;

void saveLocalValues(StreamingSQLiteDBInterface stramingdb, std::string graphID, std::string partitionID,
                      NativeStoreTriangleResult result);
void saveCentralValues(StreamingSQLiteDBInterface stramingdb, std::string graphID, std::string trianglesValue);
NativeStoreTriangleResult retrieveLocalValues(StreamingSQLiteDBInterface stramingdb,
                                              std::string graphID, std::string partitionID);
std::string retrieveCentralValues(StreamingSQLiteDBInterface stramingdb, std::string graphID);
std::string getCentralRelationCount(StreamingSQLiteDBInterface stramingdb,
                                    std::string graphID, std::string partitionID);

void streaming_snaps(PerformanceSQLiteDBInterface* perf, StreamingSnap snap);

int getUid();

StreamingTriangleCountExecutor::StreamingTriangleCountExecutor() {}

StreamingTriangleCountExecutor::StreamingTriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb,
                                                               JobRequest jobRequest) {
    this->sqlite = db;
    this->perfDB = perfDb;
    this->request = jobRequest;
    streamingDB = *new StreamingSQLiteDBInterface();
}

void StreamingTriangleCountExecutor::execute() {
    workerResponded = false;
    int uniqueId = getUid();
    std::string masterIP = request.getMasterIP();
    std::string graphId = request.getParameter(Conts::PARAM_KEYS::GRAPH_ID);
    std::string canCalibrateString = request.getParameter(Conts::PARAM_KEYS::CAN_CALIBRATE);
    std::string queueTime = request.getParameter(Conts::PARAM_KEYS::QUEUE_TIME);
    std::string graphSLAString = request.getParameter(Conts::PARAM_KEYS::GRAPH_SLA);
    std::string mode = request.getParameter(Conts::PARAM_KEYS::MODE);
    std::string partitions = request.getParameter(Conts::PARAM_KEYS::PARTITION);

    streamingDB.init();
    bool canCalibrate = Utils::parseBoolean(canCalibrateString);
    int threadPriority = request.getPriority();
    std::string autoCalibrateString = request.getParameter(Conts::PARAM_KEYS::AUTO_CALIBRATION);
    bool autoCalibrate = false;//Utils::parseBoolean(autoCalibrateString);

    if (threadPriority == Conts::HIGH_PRIORITY_DEFAULT_VALUE) {
        highPriorityGraphList.push_back(graphId);
    }

    // Below code is used to update the process details
    processStatusMutex.lock();
    std::chrono::milliseconds startTime = duration_cast<milliseconds>(system_clock::now().time_since_epoch());

    struct ProcessInfo processInformation;
    processInformation.id = uniqueId;
    processInformation.graphId = graphId;
    processInformation.processName = STREAMING_TRIANGLES;
    processInformation.priority = threadPriority;
    processInformation.startTimestamp = startTime.count();

    if (!queueTime.empty()) {
        long sleepTime = atol(queueTime.c_str());
        processInformation.sleepTime = sleepTime;
        processData.insert(processInformation);
        processStatusMutex.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    } else {
        processData.insert(processInformation);
        processStatusMutex.unlock();
    }

    streaming_triangleCount_logger.info(
            "###STREAMING-TRIANGLE-COUNT-EXECUTOR### Started with graph ID : " + graphId +
            " Master IP : " + masterIP);

    vector<Utils::worker> workerList = Utils::getWorkerList(sqlite);
    int partitionCount = stoi(partitions);
    std::vector<std::future<long>> intermRes;
    std::vector<std::future<int>> statResponse;
    long result = 0;

    streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Completed central store counting");

    for (int i = 0; i < partitionCount; i++) {
        Utils::worker currentWorker = workerList.at(i);
        string host = currentWorker.hostname;

        int workerPort = atoi(string(currentWorker.port).c_str());
        int workerDataPort = atoi(string(currentWorker.dataPort).c_str());

        intermRes.push_back(std::async(
                std::launch::async, StreamingTriangleCountExecutor::getTriangleCount, atoi(graphId.c_str()),
                host, workerPort, workerDataPort, i, masterIP, mode, streamingDB));
    }

    PerformanceUtil::init();

    std::string query =
            "SELECT attempt from graph_sla INNER JOIN sla_category where graph_sla.id_sla_category=sla_category.id and "
            "graph_sla.graph_id='" +
            graphId + "' and graph_sla.partition_count='" + std::to_string(partitionCount) +
            "' and sla_category.category='" + Conts::SLA_CATEGORY::LATENCY +
            "' and sla_category.command='" + PAGE_RANK + "';";

    std::vector<vector<pair<string, string>>> queryResults = perfDB->runSelect(query);

    if ((queryResults.size() > 0) && false) {
        std::string attemptString = queryResults[0][0].second;
        int calibratedAttempts = atoi(attemptString.c_str());

        if (calibratedAttempts >= Conts::MAX_SLA_CALIBRATE_ATTEMPTS) {
            canCalibrate = false;
        }
    } else {
        streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Inserting initial record for SLA ");
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, 0,
                                    STREAMING_TRIANGLES, Conts::SLA_CATEGORY::LATENCY);
        statResponse.push_back(std::async(std::launch::async, collectPerformaceData, perfDB,
                                          graphId.c_str(), PAGE_RANK, Conts::SLA_CATEGORY::LATENCY, partitionCount,
                                          masterIP, autoCalibrate));
        isStatCollect = true;
    }

    if (partitionCount > 2) {
        long aggregatedTriangleCount = StreamingTriangleCountExecutor::aggregateCentralStoreTriangles(
                sqlite, streamingDB, graphId, masterIP, mode, partitionCount);
        if (mode == "0") {
            saveCentralValues(streamingDB, graphId, std::to_string(aggregatedTriangleCount));
            result += aggregatedTriangleCount;
        } else {
            long old_result = stol(retrieveCentralValues(streamingDB, graphId));
            saveCentralValues(streamingDB, graphId, std::to_string(aggregatedTriangleCount + old_result));
            result += (aggregatedTriangleCount + old_result);
        }
    }

    for (auto &&futureCall : intermRes) {
        result += futureCall.get();
    }

    streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Completed local counting");

    streaming_triangleCount_logger.info(
            "###STREAMING-TRIANGLE-COUNT-EXECUTOR### Getting Triangle Count : Completed: Triangles " +
            to_string(result));
    auto end = chrono::high_resolution_clock::now();

    workerResponded = true;
    JobResponse jobResponse;
    jobResponse.setJobId(request.getJobId());
    jobResponse.addParameter(Conts::PARAM_KEYS::STREAMING_TRIANGLE_COUNT, std::to_string(result));
    jobResponse.setEndTime(end);
    responseVector.push_back(jobResponse);

    responseMap[request.getJobId()] = jobResponse;
    auto dur = end - request.getBegin();
    auto msDuration = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    streamingSnap.duration = std::to_string(msDuration);
    streamingSnap.total_triangles = to_string(result);
    streamingSnap.graph_id = graphId;
    std::time_t end_time_t = std::chrono::system_clock::to_time_t(end);
    std::string s = std::ctime(&end_time_t);
    streamingSnap.time_stamp = "11";
    streaming_snaps(perfDB, streamingSnap);
    if (canCalibrate || autoCalibrate) {
        Utils::updateSLAInformation(perfDB, graphId, partitionCount, msDuration, PAGE_RANK,
                                    Conts::SLA_CATEGORY::LATENCY);
        isStatCollect = false;
    }

    processStatusMutex.lock();
    std::set<ProcessInfo>::iterator processCompleteIterator;
    for (processCompleteIterator = processData.begin(); processCompleteIterator != processData.end();
         ++processCompleteIterator) {
        ProcessInfo processInformation = *processCompleteIterator;

        if (processInformation.id == uniqueId) {
            processData.erase(processInformation);
            break;
        }
    }
    processStatusMutex.unlock();
}

long StreamingTriangleCountExecutor::getTriangleCount(int graphId, std::string host, int port,
                                                      int dataPort, int partitionId, std::string masterIP,
                                                      std::string runMode, StreamingSQLiteDBInterface streamingDB) {
    NativeStoreTriangleResult oldResult{1, 1, 0};

    if (runMode == "1") {
       oldResult = retrieveLocalValues(streamingDB, std::to_string(graphId),
                                       std::to_string(partitionId));
    }
    int sockfd;
    if (local_socket_map.find(port) == local_socket_map.end()) {
        struct sockaddr_in serv_addr;
        struct hostent *server;

        local_socket_map[port] = socket(AF_INET, SOCK_STREAM, 0);

        if (sockfd < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
            return 0;
        }

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host);

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cerr << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);

        if (Utils::connect_wrapper(local_socket_map[port], (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "ERROR connecting" << std::endl;
        }
    }
    sockfd = local_socket_map[port];
    char data[DATA_BUFFER_SIZE];

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) != 0) {
        streaming_triangleCount_logger.error("There was an error in the upload process and the response is : " +
                                             response);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::HANDSHAKE_OK);

    if (!Utils::send_str_wrapper(sockfd, masterIP)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : " + masterIP);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
        JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::INITIATE_STREAMING_TRIAN);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(graphId))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : Graph ID " + std::to_string(graphId));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(partitionId))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : Partition ID " + std::to_string(partitionId));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(oldResult.localRelationCount))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : local relation count " +
    std::to_string(oldResult.localRelationCount));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(oldResult.centralRelationCount))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent : Central relation count " +
                std::to_string(oldResult.centralRelationCount));

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return 0;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, runMode)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }
    streaming_triangleCount_logger.info("Sent :  mode " + runMode);

    string local_relation_count = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    streamingSnap.local_edges[partitionId] = local_relation_count;
    streaming_triangleCount_logger.info("Received Local relation count: " + local_relation_count);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }

    string central_relation_count = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    streamingSnap.central_edges[partitionId] = central_relation_count;
    streaming_triangleCount_logger.info("Received Central relation count: " + central_relation_count);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::OK)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return 0;
    }

    string triangles = Utils::read_str_trim_wrapper(sockfd, data, INSTANCE_DATA_LENGTH);
    streamingSnap.local_triangles[partitionId] = triangles;
    streaming_triangleCount_logger.info("Received result: " + triangles);

    NativeStoreTriangleResult newResult{ std::stol(local_relation_count),
                                         std::stol(central_relation_count),
                                         std::stol(triangles) + oldResult.result};
    saveLocalValues(streamingDB, std::to_string(graphId),
                    std::to_string(partitionId), newResult);
    return newResult.result;
}

long StreamingTriangleCountExecutor::aggregateCentralStoreTriangles(
        SQLiteDBInterface *sqlite, StreamingSQLiteDBInterface streamingdb, std::string graphId, std::string masterIP,
                                                                    std::string runMode, int partitionCount) {
    std::vector<std::vector<string>> workerCombinations = getWorkerCombination(sqlite, graphId, partitionCount);
    std::map<string, int> workerWeightMap;
    std::vector<std::vector<string>>::iterator workerCombinationsIterator;
    std::vector<std::future<string>> triangleCountResponse;
    std::string result = "";
    long aggregatedTriangleCount = 0;

    for (workerCombinationsIterator = workerCombinations.begin();
         workerCombinationsIterator != workerCombinations.end(); ++workerCombinationsIterator) {
        std::vector<string> workerCombination = *workerCombinationsIterator;
        std::map<string, int>::iterator workerWeightMapIterator;
        std::vector<std::future<string>> remoteGraphCopyResponse;
        int minimumWeight = 0;
        std::string minWeightWorker;
        string aggregatorHost = "";
        std::string partitionIdList = "";
        std::string centralCountList = "";

        std::vector<string>::iterator workerCombinationIterator;
        std::vector<string>::iterator aggregatorCopyCombinationIterator;

        for (workerCombinationIterator = workerCombination.begin();
             workerCombinationIterator != workerCombination.end(); ++workerCombinationIterator) {
            std::string workerId = *workerCombinationIterator;

            workerWeightMapIterator = workerWeightMap.find(workerId);

            if (workerWeightMapIterator != workerWeightMap.end()) {
                int weight = workerWeightMap.at(workerId);

                if (minimumWeight == 0 || minimumWeight > weight) {
                    minimumWeight = weight + 1;
                    minWeightWorker = workerId;
                }
            } else {
                minimumWeight = 1;
                minWeightWorker = workerId;
            }
        }

        string aggregatorSqlStatement =
                "SELECT ip,user,server_port,server_data_port "
                "FROM worker "
                "WHERE idworker=" + minWeightWorker + ";";

        std::vector<vector<pair<string, string>>> result = sqlite->runSelect(aggregatorSqlStatement);

        vector<pair<string, string>> aggregatorData = result.at(0);

        std::string aggregatorIp = aggregatorData.at(0).second;
        std::string aggregatorUser = aggregatorData.at(1).second;
        std::string aggregatorPort = aggregatorData.at(2).second;
        std::string aggregatorDataPort = aggregatorData.at(3).second;
        std::string aggregatorPartitionId = minWeightWorker;

        if ((aggregatorIp.find("localhost") != std::string::npos) || aggregatorIp == masterIP) {
            aggregatorHost = aggregatorIp;
        } else {
            aggregatorHost = aggregatorUser + "@" + aggregatorIp;
        }

        for (aggregatorCopyCombinationIterator = workerCombination.begin();
             aggregatorCopyCombinationIterator != workerCombination.end(); ++aggregatorCopyCombinationIterator) {
            std::string workerId = *aggregatorCopyCombinationIterator;
            if (workerId != minWeightWorker) {
                std::string partitionId = workerId;
                partitionIdList += partitionId + ",";
                centralCountList += getCentralRelationCount(streamingdb, graphId, partitionId) + ",";
            }
        }
        centralCountList += getCentralRelationCount(streamingdb, graphId, aggregatorPartitionId);
        std::string adjustedPartitionIdList = partitionIdList.substr(0, partitionIdList.size() - 1);

        workerWeightMap[minWeightWorker] = minimumWeight;

        triangleCountResponse.push_back(std::async(
                std::launch::async, StreamingTriangleCountExecutor::countCentralStoreTriangles, aggregatorHost,
                aggregatorPort, aggregatorHost, aggregatorPartitionId, adjustedPartitionIdList, centralCountList,
                graphId, masterIP, 5, runMode));
    }

    for (auto &&futureCall : triangleCountResponse) {
        result = result + ":" + futureCall.get();
    }

    std::vector<std::string> triangles = Utils::split(result, ':');
    std::vector<std::string>::iterator triangleIterator;
    std::set<std::string> uniqueTriangleSet;

    long currentSize = triangleCount;

    for (triangleIterator = triangles.begin(); triangleIterator != triangles.end(); ++triangleIterator) {
        std::string triangle = *triangleIterator;

        if (!triangle.empty() && triangle != "NILL") {
            if (runMode == "0") {
                uniqueTriangleSet.insert(triangle);
                continue;
            }

            std::vector<std::string> triangleList = Utils::split(triangle, ',');
            long varOne = std::stol(triangleList[0]);
            long varTwo = std::stol(triangleList[1]);
            long varThree = std::stol(triangleList[2]);

            auto &itemRes = triangleTree[varOne];
            auto itemResIterator = itemRes.find(varTwo);
            if (itemResIterator != itemRes.end()) {
                auto &set2 = itemRes[varTwo];
                auto set2Iter = set2.find(varThree);
                if (set2Iter == set2.end()) {
                    set2.insert(varThree);
                    triangleCount++;
                }
            } else {
                triangleTree[varOne][varTwo].insert(varThree);
                triangleCount++;
            }
        }
    }

    if (runMode == "0") {
        return uniqueTriangleSet.size();
    }
    return triangleCount - currentSize;
}

string StreamingTriangleCountExecutor::countCentralStoreTriangles(
        std::string aggregatorHostName, std::string aggregatorPort,
        std::string host, std::string partitionId,
        std::string partitionIdList, std::string centralCountList,
        std::string graphId, std::string masterIP,
        int threadPriority, std::string runMode) {
    int port = stoi(aggregatorPort);
    int sockfd;
    if (central_socket_map.find(port) == central_socket_map.end()) {
        struct sockaddr_in serv_addr;
        struct hostent *server;

        central_socket_map[port] = socket(AF_INET, SOCK_STREAM, 0);

        if (central_socket_map[port] < 0) {
            std::cerr << "Cannot accept connection" << std::endl;
            return 0;
        }

        if (host.find('@') != std::string::npos) {
            host = Utils::split(host, '@')[1];
        }

        streaming_triangleCount_logger.info("###STREAMING-TRIANGLE-COUNT-EXECUTOR### Get Host By Name : " + host);

        server = gethostbyname(host.c_str());
        if (server == NULL) {
            std::cerr << "ERROR, no host named " << server << std::endl;
        }

        bzero((char *)&serv_addr, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(port);

        if (Utils::connect_wrapper(central_socket_map[port], (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "ERROR connecting" << std::endl;
        }
    }
    sockfd = central_socket_map[port];

    char data[DATA_BUFFER_SIZE];
    std::string result = "";

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::HANDSHAKE)) {
        streaming_triangleCount_logger.error("Error writing to socket");
    }
    streaming_triangleCount_logger.info("Sent : " + JasmineGraphInstanceProtocol::HANDSHAKE);

    string response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HANDSHAKE_OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HANDSHAKE_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, masterIP)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : " + masterIP);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::HOST_OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : " +
        JasmineGraphInstanceProtocol::AGGREGATE_STREAMING_CENTRALSTORE_TRIANGLES);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, graphId)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Graph ID " + graphId);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, partitionId)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Partition ID " + partitionId);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, partitionIdList)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Partition ID List : " + partitionIdList);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, centralCountList)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Central count list : " + centralCountList);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, std::to_string(threadPriority))) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : Priority: " + threadPriority);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    if (response.compare(JasmineGraphInstanceProtocol::OK) != 0) {
        streaming_triangleCount_logger.error("Received : " + response + " instead of : " +
                                             JasmineGraphInstanceProtocol::HOST_OK);
        return result;
    }
    streaming_triangleCount_logger.info("Received : " + JasmineGraphInstanceProtocol::OK);

    if (!Utils::send_str_wrapper(sockfd, runMode)) {
        streaming_triangleCount_logger.error("Error writing to socket");
        return result;
    }
    streaming_triangleCount_logger.info("Sent : mode " + runMode);

    response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
    response = Utils::trim_copy(response, " \f\n\r\t\v");
    string status = response.substr(response.size() - 5);
    result = response.substr(0, response.size() - 5);

    while (status == "/SEND") {
        if (!Utils::send_str_wrapper(sockfd, status)) {
            streaming_triangleCount_logger.error("Error writing to socket");
        }
        response = Utils::read_str_trim_wrapper(sockfd, data, FRONTEND_DATA_LENGTH);
        response = Utils::trim_copy(response, " \f\n\r\t\v");
        status = response.substr(response.size() - 5);
        std::string triangleResponse = response.substr(0, response.size() - 5);
        result = result + triangleResponse;
    }
    response = result;

    return response;
}

std::vector<std::vector<string>> StreamingTriangleCountExecutor::getWorkerCombination(
        SQLiteDBInterface *sqlite, std::string graphId, int partitionCount) {
    std::set<string> workerIdSet;

    for (int i = 0; i < partitionCount; i++) {
        workerIdSet.insert(std::to_string(i));
    }

    std::vector<string> workerIdVector(workerIdSet.begin(), workerIdSet.end());

    std::vector<std::vector<string>> workerIdCombination = AbstractExecutor::getCombinations(workerIdVector);

    return workerIdCombination;
}

void saveLocalValues(StreamingSQLiteDBInterface sqlite, std::string graphID, std::string partitionId,
                      NativeStoreTriangleResult result) {
        // Row doesn't exist, insert a new row
        std::string insertQuery = "INSERT OR REPLACE into streaming_partition (partition_id, local_edges, "
                                  "triangles, central_edges, graph_id) VALUES ("
                                  + partitionId + ", "
                                  + std::to_string(result.localRelationCount) + ", "
                                  + std::to_string(result.result) + ", "
                                  + std::to_string(result.centralRelationCount) + ", "
                                  + graphID + ")";

        int newGraphID = sqlite.runInsert(insertQuery);

        if (newGraphID == -1) {
            streaming_triangleCount_logger.error("Streaming local values insertion failed.");
        }
}

void saveCentralValues(StreamingSQLiteDBInterface sqlite, std::string graphID, std::string trianglesValue) {
    // Row doesn't exist, insert a new row
    std::string insertQuery = "INSERT OR REPLACE INTO central_store (triangles, graph_id) VALUES ("
                              + trianglesValue + ", "
                              + graphID + ")";

    int newGraphID = sqlite.runInsert(insertQuery);

    if (newGraphID == -1) {
        streaming_triangleCount_logger.error("Streaming central values insertion failed.");
    }
}

NativeStoreTriangleResult retrieveLocalValues(StreamingSQLiteDBInterface sqlite, std::string graphID,
                                              std::string partitionId) {
    std::string sqlStatement = "SELECT local_edges, central_edges, triangles FROM streaming_partition "
                               "WHERE partition_id = " + partitionId + " AND graph_id = " + graphID;

    std::vector<std::vector<std::pair<std::string, std::string>>> result = sqlite.runSelect(sqlStatement);

    if (result.empty()) {
        streaming_triangleCount_logger.error("No matching row found for the given partitionID and graphID.");
    return {};  // Return an empty vector
    }

    std::vector<std::pair<std::string, std::string>> aggregatorData = result.at(0);

    NativeStoreTriangleResult new_result;
    new_result.localRelationCount = std::stol(aggregatorData.at(0).second);
    new_result.centralRelationCount = std::stol(aggregatorData.at(1).second);
    new_result.result = std::stol(aggregatorData.at(2).second);

    return  new_result;
}

std::string retrieveCentralValues(StreamingSQLiteDBInterface sqlite, std::string graphID) {
    std::string sqlStatement = "SELECT triangles FROM central_store "
                               "WHERE graph_id = " + graphID;

    std::vector<std::vector<std::pair<std::string, std::string>>> result = sqlite.runSelect(sqlStatement);

    if (result.empty()) {
        streaming_triangleCount_logger.error("No matching row found for the given partitionID and graphID.");
        return {};  // Return an empty vector
    }

    std::vector<std::pair<std::string, std::string>> aggregatorData = result.at(0);

    std::string count = aggregatorData.at(0).second;

    return count;
}

std::string getCentralRelationCount(StreamingSQLiteDBInterface sqlite, std::string graphId, std::string partitionId) {
    std::string sqlStatement = "SELECT central_edges FROM streaming_partition "
                               "WHERE graph_id = " + graphId + " and partition_id= " + partitionId;

    std::vector<std::vector<std::pair<std::string, std::string>>> result = sqlite.runSelect(sqlStatement);

    if (result.empty()) {
        streaming_triangleCount_logger.error("No matching row found for the given partitionID and graphID.");
        return {};  // Return an empty vector
    }

    std::vector<std::pair<std::string, std::string>> aggregatorData = result.at(0);

    std::string count = aggregatorData.at(0).second;

    return count;
}

int getUid() {
    static std::atomic<std::uint32_t> uid{0};
    return ++uid;
}

void streaming_snaps(PerformanceSQLiteDBInterface* perf, StreamingSnap snap) {
    // Row doesn't exist, insert a new row
    std::string insertQuery = "INSERT into streaming_performance (graph_id,local_edges0,central_edges0,local_triangles0,"
                              "local_edges1,central_edges1,local_triangles1,"
                              "local_edges2,central_edges2,local_triangles2,"
                              "local_edges3,central_edges3,local_triangles3,"
                              "total_triangles,duration,time_stamp) VALUES ("
                              + snap.graph_id + ", "
                              + snap.local_edges[0] + ", "
                              + snap.central_edges[0] + ", "
                              + snap.local_triangles[0] + ", "
                              + snap.local_edges[1] + ", "
                              + snap.central_edges[1] + ", "
                              + snap.local_triangles[1] + ", "
                              + snap.local_edges[2] + ", "
                              + snap.central_edges[2] + ", "
                              + snap.local_triangles[2] + ", "
                              + snap.local_edges[3] + ", "
                              + snap.central_edges[3] + ", "
                              + snap.local_triangles[3] + ", "
                              + snap.total_triangles + ", "
                              + snap.duration + ", "
                              + snap.time_stamp + ")";

    int newGraphID = perf->runInsert(insertQuery);

    if (newGraphID == -1) {
        streaming_triangleCount_logger.error("Streaming local values insertion failed.");
    }
}