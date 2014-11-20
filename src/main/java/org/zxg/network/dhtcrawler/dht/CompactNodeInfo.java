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
package org.zxg.network.dhtcrawler.dht;

import java.net.UnknownHostException;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class CompactNodeInfo {

    public byte[] nodeId;
    public CompactIpAddressPortInfo compactIpAddressPortInfo;

    public CompactNodeInfo() {
    }

    public CompactNodeInfo(byte[] bytes) throws UnknownHostException {
        this(bytes, 0, bytes.length);
    }

    public CompactNodeInfo(byte[] bytes, int offset, int length)
            throws UnknownHostException {
        this.nodeId = new byte[20];
        System.arraycopy(bytes, offset, this.nodeId, 0, 20);

        this.compactIpAddressPortInfo = new CompactIpAddressPortInfo(bytes,
                offset + 20, 6);
    }

    public byte[] toBytes() throws UnknownHostException {
        byte[] bytes = new byte[26];
        System.arraycopy(this.nodeId, 0, bytes, 0, 20);
        System.arraycopy(this.compactIpAddressPortInfo.toBytes(), 0, bytes, 20,
                6);
        return bytes;
    }
}
