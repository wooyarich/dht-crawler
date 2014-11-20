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
package org.zxg.network.dhtclawer;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class Bucket {

    public static final int K = 8;

    public BigInteger min;
    public BigInteger max;
    public List<Node> nodes;
    public long lastChangeTime;

    public Bucket(BigInteger min, BigInteger max) {
        this.min = min;
        this.max = max;
        nodes = new CopyOnWriteArrayList<>();
        lastChangeTime = System.currentTimeMillis();
    }

    public boolean isInRange(byte[] nodeId) {
        BigInteger value = Util.nodeIdToInt(nodeId);
        return this.min.compareTo(value) <= 0 && this.max.compareTo(value) >= 0;
    }

    public void add(Node node, BiConsumer<Node, Node> copyNewToOld) throws BucketFullException {
        if (node.id.length == 20) {
            if (nodes.size() < K) {
                if (nodes.contains(node)) {
                    nodes.stream().filter(oldNode -> node.equals(oldNode)).forEach(oldNode -> {
                        copyNewToOld.accept(node, oldNode);
                    });
                } else {
                    nodes.add(node);
                }
                lastChangeTime = System.currentTimeMillis();
            } else {
                throw new BucketFullException();
            }
        }
    }

    public void add(Node node) throws BucketFullException {
        add(node, (newNode, oldNode) -> {
        });
    }

    public void remove(Node node) {
        nodes.remove(node);
        lastChangeTime = System.currentTimeMillis();
    }
}
