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
package org.zxg.network.dhtclawer.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class CompactIpAddressPortInfo {

    public String ip;
    public int port;

    public CompactIpAddressPortInfo() {
    }

    public CompactIpAddressPortInfo(byte[] bytes) throws UnknownHostException {
        this(bytes, 0, bytes.length);
    }

    public CompactIpAddressPortInfo(byte[] bytes, int offset, int length)
            throws UnknownHostException {
        byte[] ipBytes = new byte[4];
        System.arraycopy(bytes, offset, ipBytes, 0, 4);
        this.ip = InetAddress.getByAddress(ipBytes).getHostAddress();

        byte[] portBytes = new byte[4];
        System.arraycopy(bytes, offset + 4, portBytes, 2, 2);
        this.port = ByteBuffer.wrap(portBytes).getInt();
    }

    public byte[] toBytes() throws UnknownHostException {
        byte[] ipBytes = InetAddress.getByName(this.ip).getAddress();
        byte[] portBytes = new byte[4];
        ByteBuffer.wrap(portBytes).putInt(this.port);

        byte[] bytes = new byte[6];
        System.arraycopy(ipBytes, 0, bytes, 0, 4);
        System.arraycopy(portBytes, 2, bytes, 4, 2);
        return bytes;
    }
}
