//
// Created by ashokkumar on 29/12/23.
//

#include "NodeManager.h"
#include <vector>


#ifndef JASMINEGRAPH_COMPOSITECENTRALSTORE_H
#define JASMINEGRAPH_COMPOSITECENTRALSTORE_H


class CompositeCentralStore {
    public:
        GraphConfig gc;
        NodeManager* nm;
        std::list<NodeManager*> nodeManagers;
        std::string newPartitionID = "";

        CompositeCentralStore(std::string graphID, std::vector<std::string> partionIds, std::string openMode);

        bool mergeCentralStores();
        NodeManager* getNodeManager();
};


#endif //JASMINEGRAPH_COMPOSITECENTRALSTORE_H
