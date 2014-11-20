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
package org.zxg.network.dhtcrawler.krpc;

import com.turn.ttorrent.bcodec.BEValue;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import org.zxg.network.dhtcrawler.Addr;
import org.zxg.network.dhtcrawler.UdpMsg;
import org.zxg.network.dhtcrawler.Util;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public abstract class Krpc {

    protected abstract void sendUdp(UdpMsg msg) throws IOException;

    protected abstract UdpMsg recvUdp() throws IOException;

    private void sendKrpc(BEValue data, Addr addr) throws IOException {
        UdpMsg udpMsg = new UdpMsg();
        udpMsg.addr = addr;
        udpMsg.data = Util.bencode(data);
        sendUdp(udpMsg);
    }

    private byte[] generateTransactionId() {
        return Util.entropy(4);
    }

    protected final void sendKrpc(KrpcReq msg) throws IOException {
        BEValue data = new BEValue(new HashMap<>());

        data.getMap().put("q", new BEValue(msg.method));
        data.getMap().put("a", new BEValue(msg.arguments));

        byte[] transactionId = generateTransactionId();
        msg.tId = transactionId;
        data.getMap().put("t", new BEValue(transactionId));
        data.getMap().put("y", new BEValue("q"));

        sendKrpc(data, msg.addr);
    }

    protected final void sendKrpc(KrpcReply msg) throws IOException {
        BEValue data = new BEValue(new HashMap<>());

        data.getMap().put("r", new BEValue(msg.replies));

        data.getMap().put("t", new BEValue(msg.tId));
        data.getMap().put("y", new BEValue("r"));

        sendKrpc(data, msg.addr);
    }

    protected final void sendKrpc(KrpcError msg) throws IOException {
        BEValue data = new BEValue(new HashMap<>());

        data.getMap().put(
                "e",
                new BEValue(Arrays.asList(new BEValue(msg.num), new BEValue(
                                        msg.msg))));

        data.getMap().put("t", new BEValue(msg.tId));
        data.getMap().put("y", new BEValue("e"));

        sendKrpc(data, msg.addr);
    }

    protected final KrpcMsg recvKrpc() throws IOException, KrpcMsgTypeException {
        UdpMsg udpMsg = recvUdp();

        BEValue data = Util.bdecode(udpMsg.data, 0, udpMsg.data.length);
        KrpcMsg krpcMsg;
        String msgType = data.getMap().get("y").getString();
        switch (msgType) {
            case "r": {
                KrpcReply krpcReply = new KrpcReply();
                krpcReply.replies = data.getMap().get("r").getMap();
                krpcMsg = krpcReply;
            }
            break;
            case "q": {
                KrpcReq krpcReq = new KrpcReq();
                krpcReq.method = data.getMap().get("q").getString();
                krpcReq.arguments = data.getMap().get("a").getMap();
                krpcMsg = krpcReq;
            }
            break;
            case "e": {
                KrpcError krpcError = new KrpcError();
                krpcError.num = data.getMap().get("e").getList().get(0).getInt();
                krpcError.msg = data.getMap().get("e").getList().get(1).getString();
                krpcMsg = krpcError;
            }
            break;
            default:
                throw new KrpcMsgTypeException(msgType);
        }

        krpcMsg.addr = udpMsg.addr;
        krpcMsg.tId = data.getMap().get("t").getBytes();

        return krpcMsg;
    }
}
