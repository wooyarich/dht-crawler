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

import java.util.Arrays;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class Node {

    public byte[] id;
    public String ip;
    public int port;
    public long lastReqTime;
    public long lastReplyTime;
    public boolean replied;

    public Node() {
        lastReqTime = System.currentTimeMillis();
        lastReplyTime = System.currentTimeMillis();
        replied = false;
    }

    public Node(byte[] id, String ip, int port) {
        this();
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Node) {
            Node other = (Node) obj;
            if (Arrays.equals(this.id, other.id)) {
                return true;
            }
        }
        return false;
    }

}
