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

#ifndef JASMINEGRAPH_STREAMINGTRIANGLECOUNTEXECUTOR_H
#define JASMINEGRAPH_STREAMINGTRIANGLECOUNTEXECUTOR_H

#include <chrono>

#include "../../../../metadb/SQLiteDBInterface.h"
#include "../../../../streamingdb/StreamingSQLiteDBInterface.h"
#include "../../../../performance/metrics/PerformanceUtil.h"
#include "../../../../performancedb/PerformanceSQLiteDBInterface.h"
#include "../../../../server/JasmineGraphInstanceProtocol.h"
#include "../../../../query/algorithms/triangles/StreamingTriangles.h"
#include "../../../../server/JasmineGraphServer.h"
#include "../../../JasmineGraphFrontEndProtocol.h"
#include "../../CoreConstants.h"
#include "../AbstractExecutor.h"

struct StreamingSnap{
    std::string graph_id;
    std::string local_edges[4];
    std::string central_edges[4];
    std::string local_triangles[4];
    std::string total_triangles;
    std::string duration;
    std::string time_stamp;
};

class StreamingTriangleCountExecutor : public AbstractExecutor{
 public:
    static std::map<int, int> local_socket_map; // port:socket
    static std::map<int, int> central_socket_map; // port:socket

    static StreamingSnap streamingSnap;
    static std::unordered_map<long, std::unordered_map<long, std::unordered_set<long>>> triangleTree;
    static long triangleCount;

    StreamingTriangleCountExecutor();

        StreamingTriangleCountExecutor(SQLiteDBInterface *db, PerformanceSQLiteDBInterface *perfDb, JobRequest jobRequest);

        void execute();

        static long getTriangleCount(int graphId, std::string host, int port, int dataPort, int partitionId,
                                     std::string masterIP, std::string runMode, StreamingSQLiteDBInterface streamingDB);

        static long aggregateCentralStoreTriangles(SQLiteDBInterface *sqlite, StreamingSQLiteDBInterface streamingdb,
                                                   std::string graphId, std::string masterIP,
                                                   std::string mode, int partitionCount);

        static string countCentralStoreTriangles(std::string aggregatorHostName, std::string aggregatorPort,
                                                std::string host, std::string partitionId, std::string partitionIdList,
                                                 std::string centralCountList, std::string graphId,
                                                 std::string masterIP, int threadPriority, std::string mode);

        static std::vector<std::vector<string>> getWorkerCombination(SQLiteDBInterface *sqlite,
                                                                     std::string graphId, int partitionCount);

 private:
        SQLiteDBInterface *sqlite;
        PerformanceSQLiteDBInterface *perfDB;
        StreamingSQLiteDBInterface streamingDB;
};

#endif  // JASMINEGRAPH_STREAMINGTRIANGLECOUNTEXECUTOR_H
