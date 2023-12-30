//
// Created by ashokkumar on 23/12/23.
//

#include "StreamingTriangles.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <vector>

#include "../../../util/logger/Logger.h"
#include "../../../util/Utils.h"
#include "../../../nativestore/CompositeCentralStore.h"

Logger streaming_triangle_logger;

long StreamingTriangles::run(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance) {
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Streaming Triangle Counting: Started", "info");
    long triangleCount = std::stol(countLocalStreamingTriangles(incrementalLocalStoreInstance->getNodeManager(), false));

    streaming_triangle_logger.log("###STREAMING TRIANGLE### Streaming Triangle Counting: Completed: StreamingTriangles" + std::to_string(triangleCount),
                        "info");
    return triangleCount ;
}

std::string StreamingTriangles::countLocalStreamingTriangles(NodeManager* nodeManager, bool returnTriangles){
    streaming_triangle_logger.log("###STREAMING TRIANGLE### Streaming Triangle Counting: Started", "info");
    long triangleCount = 0;
    std::string triangle = "";
    long varOne = 0;
    long varTwo = 0;
    long varThree = 0;

    std::list<NodeBlock> allNodes = nodeManager->getGraph();

    for (NodeBlock node:allNodes) {
        std::list<NodeBlock> neighboursLevelOne = node.getAllEdges();
        streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering vertice : " + node.id, "info");

        for (NodeBlock neighbour_1:neighboursLevelOne) {
            std::list<NodeBlock> neighboursLevelTwo = neighbour_1.getAllEdges();
            streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering neighbour 1 : " + neighbour_1.id, "info");

            for (NodeBlock neighbour_2:neighboursLevelTwo) {
                streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering neighbour 2 : " + neighbour_2.id, "info");
                if (neighbour_2.searchRelation(node)){
                    triangleCount += 1;
                    std::vector<long> tempVector;
                    tempVector.push_back(std::stol(node.id));
                    tempVector.push_back(std::stol(neighbour_1.id));
                    tempVector.push_back(std::stol(neighbour_2.id));
                    std::sort(tempVector.begin(), tempVector.end());

                    varOne = tempVector[0];
                    varTwo = tempVector[1];
                    varThree = tempVector[2];
                    triangle += std::to_string(varOne) + "," + std::to_string(varTwo) + "," +
                               std::to_string(varThree) + ":";
                }
            }
        }

    }

    streaming_triangle_logger.log("###STREAMING TRIANGLE### Streaming Triangle Counting: Completed: StreamingTriangles" + std::to_string(triangleCount), "info");
    
    if (returnTriangles) {
        return triangle.substr(0, triangle.size() - 1);
    } else {
        return std::to_string(triangleCount / 6);
    }
                             
}

std::string StreamingTriangles::countCentralStoreStreamingTriangles(std::vector<JasmineGraphIncrementalLocalStore*> incrementalLocalStoreInstances, std::vector<std::string> partitionIds) {

    CompositeCentralStore compositeCentralStore("0", partitionIds, "app");
    compositeCentralStore.mergeCentralStores();
    NodeManager* nodeManager = compositeCentralStore.getNodeManager();
    
    std::string triangle = countLocalStreamingTriangles(nodeManager, true);
  
    if (triangle.empty()) {
        return "NILL";
    }

    return triangle;
}

long StreamingTriangles::countDynamicLocalTriangles(JasmineGraphIncrementalLocalStore *incrementalLocalStoreInstance){
    NodeManager* nodeManager = incrementalLocalStoreInstance->getNodeManager();
    long previous_local_relation_count = 5;
    long previous_central_relation_count = 5;
    long previous_triangle_count = 2;

    std::list<RelationBlock*> allNewRelations ;
    std::string dbPrefix = nodeManager->getDBPrefix();
    int relationBlockSize = RelationBlock::BLOCK_SIZE;

    long new_local_relation_count = nodeManager->dbSize(dbPrefix + "_relations.db") / relationBlockSize;
    long new_central_relation_count = nodeManager->dbSize(dbPrefix + "_central_relations.db") / relationBlockSize;

    //get new local and new relation blocks - E'
    for (int i = previous_local_relation_count; i < new_local_relation_count; ++i) {
        allNewRelations.push_back(RelationBlock::get(i*relationBlockSize));
    }
    for (int i = previous_central_relation_count; i < new_central_relation_count; ++i) {
        allNewRelations.push_back(RelationBlock::getCentral(i*relationBlockSize));
    }
    //calculate s1 G',G',E'

    std::string triangle = "";
    long varOne = 0;
    long varTwo = 0;
    long varThree = 0;
    std::set<std::string> uniqueTriangleSet;

    for (RelationBlock* relation: allNewRelations) {
        NodeBlock* source = relation->getSource();
        NodeBlock* destination = relation->getDestination();

        std::list<NodeBlock> intermediates = source->getAllEdges();
        streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering vertice : " + source->id, "info");

        for (NodeBlock intermediate:intermediates) {
              if (intermediate.searchRelation(*destination)){
                    std::vector<long> tempVector;
                    tempVector.push_back(std::stol(source->id));
                    tempVector.push_back(std::stol(intermediate.id));
                    tempVector.push_back(std::stol(destination->id));
                    std::sort(tempVector.begin(), tempVector.end());

                    varOne = tempVector[0];
                    varTwo = tempVector[1];
                    varThree = tempVector[2];
                    triangle = std::to_string(varOne) + "," + std::to_string(varTwo) + "," +
                                           std::to_string(varThree);
                    uniqueTriangleSet.insert(triangle);
              }
        }
    }
    //calculate s2 G',G,E'

    //calculate s3 g,g,E'

    // s1 - s2- s3 / 3
    return uniqueTriangleSet.size() / 3;


}

std::string StreamingTriangles::countDynamicCentralTriangles(std::vector<std::string> partitionIds){
    //read existing composite central store's relation db sizes
    long previous_central_relation_count = 5;

    //delete existing composte store
    CompositeCentralStore oldCompositeCentralStore("0", partitionIds, "app");
    NodeManager* nodeManager = oldCompositeCentralStore.getNodeManager();
    nodeManager->deleteDBFiles();

    //create new composite store
    CompositeCentralStore compositeCentralStore("0", partitionIds, "app");
    compositeCentralStore.mergeCentralStores();
    nodeManager = compositeCentralStore.getNodeManager();
    //find new relations
    std::list<RelationBlock*> allNewRelations ;
    std::string dbPrefix = nodeManager->getDBPrefix();
    int relationBlockSize = RelationBlock::BLOCK_SIZE;

    long new_central_relation_count = nodeManager->dbSize(dbPrefix + "_central_relations.db") / relationBlockSize;

    //get new local and new relation blocks - E'
    for (int i = previous_central_relation_count; i < new_central_relation_count; ++i) {
        allNewRelations.push_back(RelationBlock::getCentral(i*relationBlockSize));
    }

    //iterate through new relations and find new triangles
    //calculate s1 G',G',E'

    std::string triangle = "";
    long varOne = 0;
    long varTwo = 0;
    long varThree = 0;

    for (RelationBlock* relation: allNewRelations) {
        NodeBlock* source = relation->getSource();
        NodeBlock* destination = relation->getDestination();

        std::list<NodeBlock> intermediates = source->getAllEdges();
        streaming_triangle_logger.log("###STREAMING TRIANGLE### Entering vertice : " + source->id, "info");

        for (NodeBlock intermediate:intermediates) {
              if (intermediate.searchRelation(*destination)){
                    std::vector<long> tempVector;
                    tempVector.push_back(std::stol(source->id));
                    tempVector.push_back(std::stol(intermediate.id));
                    tempVector.push_back(std::stol(destination->id));
                    std::sort(tempVector.begin(), tempVector.end());

                    varOne = tempVector[0];
                    varTwo = tempVector[1];
                    varThree = tempVector[2];
                    triangle += std::to_string(varOne) + "," + std::to_string(varTwo) + "," +
                                           std::to_string(varThree) + ":";
              }
        }
    }
    //return new triangles
    return triangle.substr(0, triangle.size() - 1);

}