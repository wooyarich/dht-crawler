/*
 * Copyright (C) 2014 Xianguang Zhou <xianguang.zhou@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.zxg.network.dhtcrawler;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class RouteTable {

    public List<Bucket> buckets;
    public byte[] selfNodeId;

    public RouteTable(byte[] selfNodeId) {
        this.selfNodeId = selfNodeId;
        buckets = new CopyOnWriteArrayList<>();
        buckets.add(new Bucket(BigInteger.ZERO, BigInteger.valueOf(2).pow(160)));
    }

    public boolean isEmpty() {
        return buckets.stream().noneMatch((bucket) -> (!bucket.nodes.isEmpty()));
    }

    public void add(Node node, BiConsumer<Node, Node> copyNewToOld) throws BucketFullException {
        if (!Arrays.equals(selfNodeId, node.id)) {
            int index = bucketIndex(node.id);
            Bucket bucket = buckets.get(index);
            try {
                bucket.add(node, copyNewToOld);
            } catch (BucketFullException ex) {
                if (bucket.isInRange(selfNodeId)) {
                    splitBucket(index);
                    add(node, copyNewToOld);
                }
            }
        }
    }

    public void add(Node node) throws BucketFullException {
        add(node, (newNode, oldNode) -> {
        });
    }

    private int bucketIndex(byte[] nodeId) {
        int i = 0;
        for (Bucket bucket : buckets) {
            if (bucket.isInRange(nodeId)) {
                return i;
            }
            i++;
        }
        return i - 1;
    }

    private void splitBucket(int bucketIndex) throws BucketFullException {
        Bucket oldBucket = buckets.get(bucketIndex);
        BigInteger point = oldBucket.max.subtract(oldBucket.max.subtract(oldBucket.min).divide(BigInteger.valueOf(2)));
        Bucket newBucket = new Bucket(point, oldBucket.max);
        oldBucket.max = point;
        buckets.add(bucketIndex + 1, newBucket);
        for (Node node : oldBucket.nodes) {
            if (newBucket.isInRange(node.id)) {
                newBucket.add(node);
                oldBucket.remove(node);
            }
        }
    }

    public List<Node> nearestNodes(byte[] nodeIdOrInfoHash) {
        List<Node> nodes = new LinkedList<>();
        if (buckets.isEmpty()) {
            return nodes;
        }
        int bucketIndex = bucketIndex(nodeIdOrInfoHash);
        nodes.addAll(buckets.get(bucketIndex).nodes);
        int minBucketIndex = bucketIndex - 1;
        int maxBucketIndex = bucketIndex + 1;
        while ((nodes.size() < Bucket.K) && (minBucketIndex >= 0 || maxBucketIndex < buckets.size())) {
            if (minBucketIndex >= 0) {
                nodes.addAll(buckets.get(minBucketIndex).nodes);
            }
            if (maxBucketIndex < buckets.size()) {
                nodes.addAll(buckets.get(maxBucketIndex).nodes);
            }
            minBucketIndex--;
            maxBucketIndex++;
        }
        BigInteger nodeIdOrInfoHashInt = Util.nodeIdToInt(nodeIdOrInfoHash);
        nodes.sort((Node node1, Node node2) -> nodeIdOrInfoHashInt.xor(Util.nodeIdToInt(node1.id)).subtract(nodeIdOrInfoHashInt.xor(Util.nodeIdToInt(node2.id))).signum());
        return nodes.subList(0, Bucket.K > nodes.size() ? nodes.size() : Bucket.K);
    }
}
