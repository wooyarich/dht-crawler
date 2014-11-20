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

import com.turn.ttorrent.bcodec.BEValue;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zxg.network.dhtcrawler.Addr;
import org.zxg.network.dhtcrawler.krpc.Krpc;
import org.zxg.network.dhtcrawler.krpc.KrpcError;
import org.zxg.network.dhtcrawler.krpc.KrpcMsg;
import org.zxg.network.dhtcrawler.krpc.KrpcMsgTypeException;
import org.zxg.network.dhtcrawler.krpc.KrpcReply;
import org.zxg.network.dhtcrawler.krpc.KrpcReq;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public abstract class Dht extends Krpc {

    protected abstract void setDhtReqMethod(Addr addr, byte[] tid, String method);

    protected abstract String removeDhtReqMethod(Addr addr, byte[] tid);

    protected final void sendDht(DhtPingReq req) throws IOException {
        KrpcReq krpcReq = new KrpcReq();
        krpcReq.addr = req.addr;
        krpcReq.method = "ping";
        krpcReq.arguments = new HashMap<>();
        krpcReq.arguments.put("id", new BEValue(req.nodeId));
        sendKrpc(krpcReq);
        req.tId = krpcReq.tId;
        setDhtReqMethod(req.addr, req.tId, "ping");
    }

    protected final void sendDht(DhtFindNodeReq req) throws IOException {
        KrpcReq krpcReq = new KrpcReq();
        krpcReq.addr = req.addr;
        krpcReq.method = "find_node";
        krpcReq.arguments = new HashMap<>();
        krpcReq.arguments.put("id", new BEValue(req.nodeId));
        krpcReq.arguments.put("target", new BEValue(req.targetNodeId));
        sendKrpc(krpcReq);
        req.tId = krpcReq.tId;
        setDhtReqMethod(req.addr, req.tId, "find_node");
    }

    protected final void sendDht(DhtGetPeersReq req) throws IOException {
        KrpcReq krpcReq = new KrpcReq();
        krpcReq.addr = req.addr;
        krpcReq.method = "get_peers";
        krpcReq.arguments = new HashMap<>();
        krpcReq.arguments.put("id", new BEValue(req.nodeId));
        krpcReq.arguments.put("info_hash", new BEValue(req.infoHash));
        sendKrpc(krpcReq);
        req.tId = krpcReq.tId;
        setDhtReqMethod(req.addr, req.tId, "get_peers");
    }

    protected final void sendDht(DhtAnnouncePeerReq req) throws IOException {
        KrpcReq krpcReq = new KrpcReq();
        krpcReq.addr = req.addr;
        krpcReq.method = "announce_peer";
        krpcReq.arguments = new HashMap<>();
        krpcReq.arguments.put("id", new BEValue(req.nodeId));
        krpcReq.arguments.put("info_hash", new BEValue(req.infoHash));
        krpcReq.arguments.put("port", new BEValue(req.port));
        krpcReq.arguments.put("token", new BEValue(req.token));
        sendKrpc(krpcReq);
        req.tId = krpcReq.tId;
        setDhtReqMethod(req.addr, req.tId, "announce_peer");
    }

    protected final void sendDht(DhtPingReply reply) throws IOException {
        KrpcReply krpcReply = new KrpcReply();
        krpcReply.addr = reply.addr;
        krpcReply.tId = reply.tId;
        krpcReply.replies = new HashMap<>();
        krpcReply.replies.put("id", new BEValue(reply.nodeId));
        sendKrpc(krpcReply);
    }

    protected final void sendDht(DhtFindNodeReply reply) throws IOException {
        KrpcReply krpcReply = new KrpcReply();
        krpcReply.addr = reply.addr;
        krpcReply.tId = reply.tId;
        krpcReply.replies = new HashMap<>();
        krpcReply.replies.put("id", new BEValue(reply.nodeId));
        byte[] nodesBytes = new byte[reply.nodes.size() * 26];
        for (int i = 0; i < reply.nodes.size(); i++) {
            System.arraycopy(reply.nodes.get(i).toBytes(), 0, nodesBytes,
                    i * 26, 26);
        }
        krpcReply.replies.put("nodes", new BEValue(nodesBytes));
        sendKrpc(krpcReply);
    }

    protected final void sendDht(DhtGetPeersReply reply) throws IOException {
        KrpcReply krpcReply = new KrpcReply();
        krpcReply.addr = reply.addr;
        krpcReply.tId = reply.tId;
        krpcReply.replies = new HashMap<>();
        krpcReply.replies.put("id", new BEValue(reply.nodeId));
        krpcReply.replies.put("token", new BEValue(reply.token));
        if (reply.peers != null) {
            List<BEValue> values = new LinkedList<>();
            for (CompactIpAddressPortInfo info : reply.peers) {
                values.add(new BEValue(info.toBytes()));
            }
            krpcReply.replies.put("values", new BEValue(values));
        } else {
            byte[] nodesBytes = new byte[reply.nodes.size() * 26];
            for (int i = 0; i < reply.nodes.size(); i++) {
                System.arraycopy(reply.nodes.get(i).toBytes(), 0, nodesBytes,
                        i * 26, 26);
            }
            krpcReply.replies.put("nodes", new BEValue(nodesBytes));
        }
        sendKrpc(krpcReply);
    }

    protected final void sendDht(DhtAnnouncePeerReply reply) throws IOException {
        KrpcReply krpcReply = new KrpcReply();
        krpcReply.addr = reply.addr;
        krpcReply.tId = reply.tId;
        krpcReply.replies = new HashMap<>();
        krpcReply.replies.put("id", new BEValue(reply.nodeId));
        sendKrpc(krpcReply);
    }

    protected final void sendDht(DhtGeneralError err) throws IOException {
        KrpcError krpcError = new KrpcError();
        krpcError.addr = err.addr;
        krpcError.tId = err.tId;
        krpcError.msg = err.msg;
        krpcError.num = 201;
        sendKrpc(krpcError);
    }

    protected final void sendDht(DhtServiceError err) throws IOException {
        KrpcError krpcError = new KrpcError();
        krpcError.addr = err.addr;
        krpcError.tId = err.tId;
        krpcError.msg = err.msg;
        krpcError.num = 202;
        sendKrpc(krpcError);
    }

    protected final void sendDht(DhtProtocolError err) throws IOException {
        KrpcError krpcError = new KrpcError();
        krpcError.addr = err.addr;
        krpcError.tId = err.tId;
        krpcError.msg = err.msg;
        krpcError.num = 203;
        sendKrpc(krpcError);
    }

    protected final void sendDht(DhtUnknownMethodError err) throws IOException {
        KrpcError krpcError = new KrpcError();
        krpcError.addr = err.addr;
        krpcError.tId = err.tId;
        krpcError.msg = err.msg;
        krpcError.num = 204;
        sendKrpc(krpcError);
    }

    protected final DhtMsg recvDht() throws IOException, KrpcMsgTypeException,
            DhtReqMethodException, DhtReplyTypeException, DhtErrorNumException {
        KrpcMsg krpcMsg = recvKrpc();
        DhtMsg dhtMsg;
        if (krpcMsg instanceof KrpcReq) {
            KrpcReq krpcReq = (KrpcReq) krpcMsg;
            DhtReq dhtReq;
            switch (krpcReq.method) {
                case "ping": {
                    DhtPingReq dhtPingReq = new DhtPingReq();
                    dhtPingReq.nodeId = krpcReq.arguments.get("id").getBytes();
                    dhtReq = dhtPingReq;
                }
                break;
                case "find_node": {
                    DhtFindNodeReq dhtFindNodeReq = new DhtFindNodeReq();
                    dhtFindNodeReq.nodeId = krpcReq.arguments.get("id").getBytes();
                    dhtFindNodeReq.targetNodeId = krpcReq.arguments.get("target")
                            .getBytes();
                    dhtReq = dhtFindNodeReq;
                }
                break;
                case "get_peers": {
                    DhtGetPeersReq dhtGetPeersReq = new DhtGetPeersReq();
                    dhtGetPeersReq.nodeId = krpcReq.arguments.get("id").getBytes();
                    dhtGetPeersReq.infoHash = krpcReq.arguments.get("info_hash")
                            .getBytes();
                    dhtReq = dhtGetPeersReq;
                }
                break;
                case "announce_peer": {
                    DhtAnnouncePeerReq dhtAnnouncePeerReq = new DhtAnnouncePeerReq();
                    dhtAnnouncePeerReq.nodeId = krpcReq.arguments.get("id")
                            .getBytes();
                    dhtAnnouncePeerReq.infoHash = krpcReq.arguments
                            .get("info_hash").getBytes();
                    dhtAnnouncePeerReq.port = krpcReq.arguments.get("port")
                            .getInt();
                    dhtAnnouncePeerReq.token = krpcReq.arguments.get("token")
                            .getBytes();
                    dhtReq = dhtAnnouncePeerReq;
                }
                break;
                default:
                    throw new DhtReqMethodException();
            }
            dhtMsg = dhtReq;
        } else if (krpcMsg instanceof KrpcReply) {
            KrpcReply krpcReply = (KrpcReply) krpcMsg;
            DhtReply dhtReply;
            String dhtReqMethod = removeDhtReqMethod(krpcReply.addr,
                    krpcReply.tId);
            switch (dhtReqMethod) {
                case "ping": {
                    DhtPingReply dhtPingReply = new DhtPingReply();
                    dhtPingReply.nodeId = krpcReply.replies.get("id").getBytes();
                    dhtReply = dhtPingReply;
                }
                break;
                case "find_node": {
                    DhtFindNodeReply dhtFindNodeReply = new DhtFindNodeReply();
                    dhtFindNodeReply.nodeId = krpcReply.replies.get("id")
                            .getBytes();
                    dhtFindNodeReply.nodes = new LinkedList<>();
                    if (krpcReply.replies.containsKey("nodes")) {
                        byte[] nodesBytes = krpcReply.replies.get("nodes").getBytes();
                        for (int i = 0; i * 26 < nodesBytes.length; i++) {
                            dhtFindNodeReply.nodes.add(new CompactNodeInfo(nodesBytes,
                                    i * 26, 26));
                        }
                    } else {
                        Logger.getLogger(Dht.class.getName()).log(Level.FINEST, "find_node reply doesn't contain nodes");
                    }
                    dhtReply = dhtFindNodeReply;
                }
                break;
                case "get_peers": {
                    DhtGetPeersReply dhtGetPeersReply = new DhtGetPeersReply();
                    dhtGetPeersReply.nodeId = krpcReply.replies.get("id")
                            .getBytes();
                    dhtGetPeersReply.token = krpcReply.replies.get("token")
                            .getBytes();
                    if (krpcReply.replies.get("values") != null) {
                        dhtGetPeersReply.peers = new LinkedList<>();
                        List<BEValue> values = krpcReply.replies.get("values")
                                .getList();
                        for (BEValue value : values) {
                            dhtGetPeersReply.peers
                                    .add(new CompactIpAddressPortInfo(value
                                                    .getBytes()));
                        }
                    } else {
                        dhtGetPeersReply.nodes = new LinkedList<>();
                        byte[] nodesBytes = krpcReply.replies.get("nodes")
                                .getBytes();
                        for (int i = 0; i * 26 < nodesBytes.length; i++) {
                            dhtGetPeersReply.nodes.add(new CompactNodeInfo(
                                    nodesBytes, i * 26, 26));
                        }
                    }
                    dhtReply = dhtGetPeersReply;
                }
                break;
                case "announce_peer": {
                    DhtAnnouncePeerReply dhtAnnouncePeerReply = new DhtAnnouncePeerReply();
                    dhtAnnouncePeerReply.nodeId = krpcReply.replies.get("id")
                            .getBytes();
                    dhtReply = dhtAnnouncePeerReply;
                }
                break;
                default: {
                    throw new DhtReplyTypeException();
                }
            }
            dhtMsg = dhtReply;
        } else if (krpcMsg instanceof KrpcError) {
            KrpcError krpcError = (KrpcError) krpcMsg;
            DhtError dhtError;
            switch (krpcError.num) {
                case 201: {
                    DhtGeneralError dhtGeneralError = new DhtGeneralError();
                    dhtError = dhtGeneralError;
                }
                break;
                case 202: {
                    DhtServiceError dhtServiceError = new DhtServiceError();
                    dhtError = dhtServiceError;
                }
                break;
                case 203: {
                    DhtProtocolError dhtProtocolError = new DhtProtocolError();
                    dhtError = dhtProtocolError;
                }
                break;
                case 204: {
                    DhtUnknownMethodError dhtUnknownMethodError = new DhtUnknownMethodError();
                    dhtError = dhtUnknownMethodError;
                }
                break;
                default:
                    throw new DhtErrorNumException();
            }
            dhtError.msg = krpcError.msg;
            dhtMsg = dhtError;
        } else {
            throw new KrpcMsgTypeException();
        }
        dhtMsg.addr = krpcMsg.addr;
        dhtMsg.tId = krpcMsg.tId;
        return dhtMsg;
    }
}
