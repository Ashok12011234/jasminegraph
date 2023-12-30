//
// Created by ashokkumar on 29/12/23.
//

#include "CompositeCentralStore.h"
#include "../util/logger/Logger.h"


Logger composite_centralstore_logger;

CompositeCentralStore::CompositeCentralStore(std::string graphID, std::vector<std::string> partitionIds, std::string openMode){
    composite_centralstore_logger.log("Creating CompositeCentralStore for graphID: " + graphID + " and partitionIds: " + partitionIds.front() + " and openMode: " + openMode, "info");
    gc.graphID = atoi(graphID.c_str());
    gc.maxLabelSize = 43;   
    gc.openMode = openMode;

    for (std::string partitionID : partitionIds) {
        newPartitionID += partitionID;
 
        gc.partitionID = atoi(partitionID.c_str());
        nodeManagers.push_back(new NodeManager(gc));   
    }

    gc.partitionID = atoi(newPartitionID.c_str());
    nm = new NodeManager(gc);
}

bool CompositeCentralStore::mergeCentralStores() {
    composite_centralstore_logger.log("Merging CentralStores", "info");
    for (NodeManager* nodeManager : nodeManagers) {
        std::list<NodeBlock> allNodes = nodeManager->getGraph();

        for (NodeBlock node : allNodes) {
            std::list<NodeBlock> centralEdges = node.getCentralEdges();

            for (NodeBlock centralEdge : centralEdges) {
                nm->addCentralEdge({node.id, centralEdge.id});
            }
            // std::list<NodeBlock> localEdges = node.getLocalEdges();
            
            // for (NodeBlock localEdge : localEdges) {
            //     nm->addEdge({node.id, localEdge.id});
            // }
        }

    }
    return true;
}

NodeManager* CompositeCentralStore::getNodeManager() {
    return nm;
}

long deleteStore(){
    composite_centralstore_logger.log("Deleting CompositeCentralStore", "info");
    
    return 0;
}
//create method which return singleton instance of CompositeCentralStore


