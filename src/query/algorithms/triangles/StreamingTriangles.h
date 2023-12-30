//
// Created by ashokkumar on 23/12/23.
//

#ifndef JASMINEGRAPH_STRAMINGTRIANGLES_H
#define JASMINEGRAPH_STRAMINGTRIANGLES_H

#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <string>
#include <thread>

#include "../../../util/Conts.h"
#include "../../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../../../nativestore/RelationBlock.h"

class StreamingTriangles {
public:

    static long run(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance);

    static std::string countCentralStoreStreamingTriangles(std::vector<JasmineGraphIncrementalLocalStore*> incrementalLocalStoreInstances, std::vector<std::string> partitionIds);

    static long countDynamicLocalTriangles(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance);
    static std::string countLocalStreamingTriangles(NodeManager* nodeManager, bool returnTriangles);
    static std::string countDynamicCentralTriangles(std::vector<std::string> partitionIds);


};

#endif //JASMINEGRAPH_STRAMINGTRIANGLES_H
