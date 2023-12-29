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
#include "../../nativestore/RelationBlock.h"

class StreamingTriangles {
public:

    static long run(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance);

    static std::string countCentralStoreStreamingTriangles(std::vector<JasmineGraphIncrementalLocalStore*> incrementalLocalStoreInstances);

    static long countDynamicLocalTriangles(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance);

};

#endif //JASMINEGRAPH_STRAMINGTRIANGLES_H
