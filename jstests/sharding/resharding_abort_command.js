/**
 * Test to make sure that the abort command interrupts a resharding operation that has not yet
 * persisted a decision.
 *
 * @tags: [requires_fcv_49, uses_atclustertime]
 */
(function() {
"use strict";
load("jstests/libs/discover_topology.js");
load("jstests/libs/parallelTester.js");
load("jstests/sharding/libs/resharding_test_fixture.js");
load("jstests/sharding/libs/resharding_test_util.js");

// TODO SERVER-52838 Re-enable checking UUIDs.
TestData.skipCheckingUUIDsConsistentAcrossCluster = true;

const originalCollectionNs = "reshardingDb.coll";
const enterAbortFailpointName = "reshardingPauseCoordinatorBeforeStartingErrorFlow";

const nodeTypeEnum = {
    COORDINATOR: 1,
    DONOR: 2,
    RECIPIENT: 3,
    NO_EXTRA_FAILPOINTS_SENTINEL: 4
};

const abortLocationEnum = {
    BEFORE_STEADY_STATE: 1,
    BEFORE_DECISION_PERSISTED: 2,
    AFTER_DECISION_PERSISTED: 3
};

let getConnStringsFromNodeType = (nodeType, reshardingTest, topology) => {
    let connStrings = [];
    if (nodeType == nodeTypeEnum.COORDINATOR) {
        connStrings.push(topology.configsvr.nodes[0]);
    } else if (nodeType == nodeTypeEnum.DONOR) {
        for (let donor of reshardingTest.donorShardNames) {
            connStrings.push(topology.shards[donor].primary);
        }
    } else if (nodeType == nodeTypeEnum.RECIPIENT) {
        for (let recipient of reshardingTest.recipientShardNames) {
            connStrings.push(topology.shards[recipient].primary);
        }
    } else if (nodeType == nodeTypeEnum.NO_EXTRA_FAILPOINTS_SENTINEL) {
    } else {
        throw 'unsupported node type in resharding abort test';
    }

    return connStrings;
};

let getMongosFromConnStrings = (connStrings) => {
    let mongos = [];
    for (let conn of connStrings) {
        mongos.push(new Mongo(conn));
    }
    return mongos;
};

let generateFailpoints =
    (failpointName, failpointNodeType, reshardingTest, toplogy, failpointMode = "alwaysOn") => {
        const failpointTargetConnStrings =
            getConnStringsFromNodeType(failpointNodeType, reshardingTest, toplogy);
        const failpointHosts = getMongosFromConnStrings(failpointTargetConnStrings);

        let failpoints = [];
        for (let host of failpointHosts) {
            failpoints.push(configureFailPoint(host, failpointName));
        }

        return failpoints;
    };

let generateAbortThread = (mongosConnString, ns, expectedErrorCodes) => {
    return new Thread((mongosConnString, ns, expectedErrorCodes) => {
        const mongos = new Mongo(mongosConnString);
        if (expectedErrorCodes == ErrorCodes.OK) {
            assert.commandWorked(mongos.adminCommand({abortReshardCollection: ns}));
        } else {
            assert.commandFailedWithCode(mongos.adminCommand({abortReshardCollection: ns}),
                                         expectedErrorCodes);
        }
    }, mongosConnString, ns, expectedErrorCodes);
};

let triggerAbortAndCoordinateFailpoints = (failpointName,
                                           failpointNodeType,
                                           reshardingTest,
                                           topology,
                                           mongos,
                                           configsvr,
                                           abortThread,
                                           failpoints,
                                           executeBeforeWaitingOnFailpointsFn,
                                           executeAfterWaitingOnFailpointsFn,
                                           executeAfterAbortingFn) => {
    if (executeBeforeWaitingOnFailpointsFn) {
        jsTestLog(`Executing the before-waiting-on-failpoint function`);
        executeBeforeWaitingOnFailpointsFn(mongos, originalCollectionNs);
    }

    if (failpointNodeType != nodeTypeEnum.NO_EXTRA_FAILPOINTS_SENTINEL) {
        jsTestLog(`Wait for the failpoint ${failpointName} to be reached on all applicable nodes`);
        for (let failpoint of failpoints) {
            failpoint.wait();
        }
    }

    if (executeAfterWaitingOnFailpointsFn) {
        jsTestLog(`Executing the after-waiting-on-failpoint function`);
        executeAfterWaitingOnFailpointsFn(mongos, originalCollectionNs);
    }

    jsTestLog(`Wait for the coordinator to recognize that it's been aborted`);

    const enterAbortFailpoint = configureFailPoint(configsvr, enterAbortFailpointName);
    abortThread.start();
    enterAbortFailpoint.wait();

    if (executeAfterAbortingFn) {
        jsTestLog(`Executing the after-aborting function`);
        executeAfterAbortingFn(mongos, originalCollectionNs);
    }

    enterAbortFailpoint.off();

    if (failpointNodeType != nodeTypeEnum.NO_EXTRA_FAILPOINTS_SENTINEL) {
        jsTestLog(`Turn off the failpoint ${
            failpointName} to allow both the abort and the resharding operation to complete`);
        for (let failpoint of failpoints) {
            failpoint.off();
        }
    }
};

let triggerPostDecisionPersistedAbort = (mongos, abortThread) => {
    assert.soon(() => {
        // It's possible that after the decision has been persisted, the
        // coordinator document could be in either of the two specified states,
        // or will have been completely deleted. Await any of these conditions
        // in order to test the abort command's inability to abort after a
        // persisted decision.
        const coordinatorDoc =
            mongos.getCollection('config.reshardingOperations').findOne({ns: originalCollectionNs});
        return coordinatorDoc == null ||
            (coordinatorDoc.state === "decision-persisted" || coordinatorDoc.state === "done");
    });

    abortThread.start();
};

const runAbortWithFailpoint = (failpointName, failpointNodeType, abortLocation, {
    executeAtStartOfReshardingFn = null,
    executeBeforeWaitingOnFailpointsFn = null,
    executeAfterWaitingOnFailpointsFn = null,
    executeAfterAbortingFn = null,
} = {}) => {
    const reshardingTest =
        new ReshardingTest({numDonors: 2, numRecipients: 2, reshardInPlace: true});
    reshardingTest.setup();

    const donorShardNames = reshardingTest.donorShardNames;
    const recipientShardNames = reshardingTest.recipientShardNames;

    const sourceCollection = reshardingTest.createShardedCollection({
        ns: "reshardingDb.coll",
        shardKeyPattern: {oldKey: 1},
        chunks: [
            {min: {oldKey: MinKey}, max: {oldKey: 0}, shard: donorShardNames[0]},
            {min: {oldKey: 0}, max: {oldKey: MaxKey}, shard: donorShardNames[1]},
        ],
    });

    const mongos = sourceCollection.getMongo();
    const topology = DiscoverTopology.findConnectedNodes(mongos);
    const configsvr = new Mongo(topology.configsvr.nodes[0]);

    let expectedAbortErrorCodes = ErrorCodes.OK;
    let expectedReshardingErrorCode = ErrorCodes.ReshardCollectionAborted;

    // If the abort is going to happen after the decision is persisted, it's expected that the
    // resharding operation will have finished without error, and that the abort itself will
    // error.
    if (abortLocation == abortLocationEnum.AFTER_DECISION_PERSISTED) {
        expectedAbortErrorCodes =
            [ErrorCodes.ReshardCollectionCommitted, ErrorCodes.NoSuchReshardCollection];
        expectedReshardingErrorCode = ErrorCodes.OK;
    }

    const abortThread = generateAbortThread(
        topology.mongos.nodes[0], originalCollectionNs, expectedAbortErrorCodes);

    let failpoints = [];
    if (failpointNodeType != nodeTypeEnum.NO_EXTRA_FAILPOINTS_SENTINEL) {
        failpoints = generateFailpoints(failpointName, failpointNodeType, reshardingTest, topology);
    }

    reshardingTest.withReshardingInBackground(
        {
            newShardKeyPattern: {newKey: 1},
            newChunks: [
                {min: {newKey: MinKey}, max: {newKey: 0}, shard: recipientShardNames[0]},
                {min: {newKey: 0}, max: {newKey: MaxKey}, shard: recipientShardNames[1]},
            ],
        },
        () => {
            if (executeAtStartOfReshardingFn) {
                jsTestLog(`Executing the start-of-resharding fn`);
                executeAtStartOfReshardingFn(
                    reshardingTest, topology, mongos, originalCollectionNs);
            }

            if (abortLocation == abortLocationEnum.BEFORE_STEADY_STATE) {
                triggerAbortAndCoordinateFailpoints(failpointName,
                                                    failpointNodeType,
                                                    reshardingTest,
                                                    topology,
                                                    mongos,
                                                    configsvr,
                                                    abortThread,
                                                    failpoints,
                                                    executeBeforeWaitingOnFailpointsFn,
                                                    executeAfterWaitingOnFailpointsFn,
                                                    executeAfterAbortingFn);
            }
        },
        {
            expectedErrorCode: expectedReshardingErrorCode,
            postCheckConsistencyFn: () => {
                if (abortLocation == abortLocationEnum.BEFORE_DECISION_PERSISTED) {
                    triggerAbortAndCoordinateFailpoints(failpointName,
                                                        failpointNodeType,
                                                        reshardingTest,
                                                        topology,
                                                        mongos,
                                                        configsvr,
                                                        abortThread,
                                                        failpoints,
                                                        executeBeforeWaitingOnFailpointsFn,
                                                        executeAfterWaitingOnFailpointsFn,
                                                        executeAfterAbortingFn);
                }
            },
            postDecisionPersistedFn: () => {
                if (abortLocation == abortLocationEnum.AFTER_DECISION_PERSISTED) {
                    triggerPostDecisionPersistedAbort(mongos, abortThread);
                }
            }
        });
    reshardingTest.teardown();

    abortThread.join();
};

runAbortWithFailpoint("reshardingPauseRecipientBeforeCloning",
                      nodeTypeEnum.RECIPIENT,
                      abortLocationEnum.BEFORE_STEADY_STATE);
runAbortWithFailpoint("reshardingPauseRecipientDuringCloning",
                      nodeTypeEnum.RECIPIENT,
                      abortLocationEnum.BEFORE_STEADY_STATE);

runAbortWithFailpoint("reshardingPauseRecipientDuringOplogApplication",
                      nodeTypeEnum.RECIPIENT,
                      abortLocationEnum.BEFORE_STEADY_STATE,
                      {
                          executeAfterWaitingOnFailpointsFn: (mongos, ns) => {
                              assert.commandWorked(mongos.getCollection(ns).insert([
                                  {_id: 0, oldKey: -10, newKey: -10},
                                  {_id: 1, oldKey: 10, newKey: -10},
                                  {_id: 2, oldKey: -10, newKey: 10},
                                  {_id: 3, oldKey: 10, newKey: 10},
                              ]));
                          },
                      });

// Rely on the resharding_test_fixture's built-in failpoint that hangs before switching to
// the blocking writes state.
runAbortWithFailpoint(
    null, nodeTypeEnum.NO_EXTRA_FAILPOINTS_SENTINEL, abortLocationEnum.BEFORE_STEADY_STATE, {
        executeAtStartOfReshardingFn: (reshardingTest, topology, mongos, ns) => {
            assert.soon(() => {
                const coordinatorDoc =
                    mongos.getCollection('config.reshardingOperations').findOne({ns: ns});
                if (coordinatorDoc == null) {
                    return false;
                }

                jsTestLog(tojson(coordinatorDoc));

                for (const shardEntry of coordinatorDoc.recipientShards) {
                    if (shardEntry.mutableState.state !== "steady-state") {
                        return false;
                    }
                }

                return true;
            });
        },
    });

runAbortWithFailpoint(
    null, nodeTypeEnum.NO_EXTRA_FAILPOINTS_SENTINEL, abortLocationEnum.AFTER_DECISION_PERSISTED);

// TODO SERVER-55506 Uncomment and fix this case after the _flushReshardingStateChange command has
// been emplaced.
/*
runAbortWithFailpoint("reshardingDonorPausesAfterEmplacingCriticalSection", nodeTypeEnum.DONOR,
abortLocationEnum.BEFORE_DECISION_PERSISTED,
{
    executeAtStartOfReshardingFn: (reshardingTest, topology, mongos, ns) => {
        assert.soon(() => {
            const coordinatorDoc = mongos.getCollection('config.reshardingOperations').findOne({ns:
ns}); return coordinatorDoc != null && coordinatorDoc.state === "applying";
        });

        generateFailpoints("reshardingPauseRecipientDuringOplogApplication", nodeTypeEnum.RECIPIENT,
reshardingTest, topology);

        assert.commandWorked(mongos.getCollection(ns).insert([
            {_id: 0, oldKey: -10, newKey: -10},
            {_id: 1, oldKey: 10, newKey: -10},
            {_id: 2, oldKey: -10, newKey: 10},
            {_id: 3, oldKey: 10, newKey: 10},
        ]));
    },
    executeAfterAbortingFn: (reshardingTest, topology, mongos, ns) => {
        generateFailpoints("reshardingPauseRecipientDuringOplogApplication", nodeTypeEnum.RECIPIENT,
reshardingTest, topology, "off");
    }
});*/
})();
