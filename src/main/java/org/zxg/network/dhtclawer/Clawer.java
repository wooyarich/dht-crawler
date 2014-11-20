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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Hex;
import org.zxg.network.dhtclawer.dht.CompactIpAddressPortInfo;
import org.zxg.network.dhtclawer.dht.CompactNodeInfo;
import org.zxg.network.dhtclawer.dht.Dht;
import org.zxg.network.dhtclawer.dht.DhtAnnouncePeerReply;
import org.zxg.network.dhtclawer.dht.DhtAnnouncePeerReq;
import org.zxg.network.dhtclawer.dht.DhtException;
import org.zxg.network.dhtclawer.dht.DhtFindNodeReply;
import org.zxg.network.dhtclawer.dht.DhtFindNodeReq;
import org.zxg.network.dhtclawer.dht.DhtGetPeersReply;
import org.zxg.network.dhtclawer.dht.DhtGetPeersReq;
import org.zxg.network.dhtclawer.dht.DhtMsg;
import org.zxg.network.dhtclawer.dht.DhtPingReply;
import org.zxg.network.dhtclawer.dht.DhtPingReq;
import org.zxg.network.dhtclawer.dht.DhtReply;
import org.zxg.network.dhtclawer.dht.DhtReq;
import org.zxg.network.dhtclawer.krpc.KrpcException;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class Clawer extends Dht {

    private final static List<Addr> BOOTSTRAP_NODES = new LinkedList<>();

    static {
        BOOTSTRAP_NODES.add(new Addr("router.bittorrent.com", 6881));
        BOOTSTRAP_NODES.add(new Addr("dht.transmissionbt.com", 6881));
        BOOTSTRAP_NODES.add(new Addr("router.utorrent.com", 6881));
    }

    private String host;
    private int port;

    private byte[] nodeId;
    private RouteTable routeTable;
    private Map<String, MethodAndTime> dhtReqMethodCache;

    private DatagramSocket socket;
    private ReceiveThread receiveThread;
    private Timer timer;

    private InfoHashListener infoHashListener;

    public Clawer(String host, int port, InfoHashListener infoHashListener) {
        this.host = host;
        this.port = port;
        this.infoHashListener = infoHashListener;
    }

    public void start() throws SocketException, NoSuchAlgorithmException {
        nodeId = Util.randomId();
        routeTable = new RouteTable(nodeId);
        dhtReqMethodCache = new ConcurrentHashMap<>();
        socket = new DatagramSocket(new InetSocketAddress(host, port));
        receiveThread = new ReceiveThread();
        receiveThread.start();
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                clearDhtReqMethodCache();
            }
        }, 900000, 900000);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    maintainRouteTable();
                } catch (Exception ex) {
                    Logger.getLogger(Clawer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }, 1000, 1000);
    }

    public void stop() {
        timer.cancel();
        try {
            receiveThread.cancel();
            receiveThread.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Clawer.class.getName()).log(Level.SEVERE, null, ex);
        }
        socket.close();
    }

    private void clearDhtReqMethodCache() {
        long currentTime = System.currentTimeMillis();
        this.dhtReqMethodCache
                .entrySet().stream().filter((entry) -> (currentTime - entry.getValue().time > 900000)).forEach((entry) -> {
                    this.dhtReqMethodCache.remove(entry.getKey());
                });
    }

    private void maintainRouteTable() throws IOException {
        long currentTime = System.currentTimeMillis();
        for (Bucket bucket : routeTable.buckets) {
            if (currentTime - bucket.lastChangeTime > 900000) {
                byte[] randomId = Util.randomId(bucket.min, bucket.max);
                findNodes(randomId);
            }
            for (Node node : bucket.nodes) {
                if (!((currentTime - node.lastReplyTime < 900000) || (node.replied && currentTime - node.lastReqTime < 900000))) {
                    pingNode(new Addr(node.ip, node.port), this.nodeId);
                    bucket.nodes.remove(node);
                }
            }
        }
        findNodes(this.nodeId);
    }

    private void receive() throws IOException, DhtException, KrpcException {
        DhtMsg dhtMsg = recvDht();
        addRouteTableNode(dhtMsg);
        if (dhtMsg instanceof DhtGetPeersReq) {
            DhtGetPeersReq dhtGetPeersReq = (DhtGetPeersReq) dhtMsg;
            receive(dhtGetPeersReq);
        } else if (dhtMsg instanceof DhtGetPeersReply) {
            DhtGetPeersReply dhtGetPeersReply = (DhtGetPeersReply) dhtMsg;
            receive(dhtGetPeersReply);
        } else if (dhtMsg instanceof DhtFindNodeReq) {
            DhtFindNodeReq dhtFindNodeReq = (DhtFindNodeReq) dhtMsg;
            receive(dhtFindNodeReq);
        } else if (dhtMsg instanceof DhtFindNodeReply) {
            DhtFindNodeReply dhtFindNodeReply = (DhtFindNodeReply) dhtMsg;
            receive(dhtFindNodeReply);
        } else if (dhtMsg instanceof DhtPingReq) {
            DhtPingReq dhtPingReq = (DhtPingReq) dhtMsg;
            receive(dhtPingReq);
        } else if (dhtMsg instanceof DhtPingReply) {
            DhtPingReply dhtPingReply = (DhtPingReply) dhtMsg;
            receive(dhtPingReply);
        } else if (dhtMsg instanceof DhtAnnouncePeerReq) {
            DhtAnnouncePeerReq dhtAnnouncePeerReq = (DhtAnnouncePeerReq) dhtMsg;
            receive(dhtAnnouncePeerReq);
        } else if (dhtMsg instanceof DhtAnnouncePeerReply) {
            DhtAnnouncePeerReply dhtAnnouncePeerReply = (DhtAnnouncePeerReply) dhtMsg;
            receive(dhtAnnouncePeerReply);
        }
    }

    private void receive(DhtGetPeersReq dhtGetPeersReq) throws IOException {
        DhtGetPeersReply dhtGetPeersReply = new DhtGetPeersReply();
        dhtGetPeersReply.addr = dhtGetPeersReq.addr;
        dhtGetPeersReply.tId = dhtGetPeersReq.tId;
        dhtGetPeersReply.nodeId = this.nodeId;
        dhtGetPeersReply.token = Util.entropy(4);
        dhtGetPeersReply.nodes = new LinkedList<>();
        routeTable.nearestNodes(dhtGetPeersReq.infoHash).stream().map(node -> {
            CompactNodeInfo compactNodeInfo = new CompactNodeInfo();
            compactNodeInfo.nodeId = node.id;
            compactNodeInfo.compactIpAddressPortInfo = new CompactIpAddressPortInfo();
            compactNodeInfo.compactIpAddressPortInfo.ip = node.ip;
            compactNodeInfo.compactIpAddressPortInfo.port = node.port;
            return compactNodeInfo;
        }).forEach((compactNodeInfo) -> {
            dhtGetPeersReply.nodes.add(compactNodeInfo);
        });
        sendDht(dhtGetPeersReply);
        infoHashListener.accept(dhtGetPeersReq.infoHash);
    }

    private void receive(DhtGetPeersReply dhtGetPeersReply) {
    }

    private void receive(DhtFindNodeReq dhtFindNodeReq) throws IOException {
        DhtFindNodeReply dhtFindNodeReply = new DhtFindNodeReply();
        dhtFindNodeReply.addr = dhtFindNodeReq.addr;
        dhtFindNodeReply.tId = dhtFindNodeReq.tId;
        dhtFindNodeReply.nodeId = this.nodeId;
        dhtFindNodeReply.nodes = new LinkedList<>();
        routeTable.nearestNodes(dhtFindNodeReq.targetNodeId).stream().map((node) -> {
            CompactNodeInfo compactNodeInfo = new CompactNodeInfo();
            compactNodeInfo.nodeId = node.id;
            compactNodeInfo.compactIpAddressPortInfo = new CompactIpAddressPortInfo();
            compactNodeInfo.compactIpAddressPortInfo.ip = node.ip;
            compactNodeInfo.compactIpAddressPortInfo.port = node.port;
            return compactNodeInfo;
        }).forEach((compactNodeInfo) -> {
            dhtFindNodeReply.nodes.add(compactNodeInfo);
        });
        sendDht(dhtFindNodeReply);
    }

    private void receive(DhtFindNodeReply dhtFindNodeReply) throws IOException {
        for (CompactNodeInfo compactNodeInfo : dhtFindNodeReply.nodes) {
            try {
                this.routeTable.add(new Node(compactNodeInfo.nodeId, compactNodeInfo.compactIpAddressPortInfo.ip, compactNodeInfo.compactIpAddressPortInfo.port));
            } catch (BucketFullException ex) {
                Logger.getLogger(Clawer.class.getName()).log(Level.SEVERE, null, ex);
            }
            findNode(new Addr(compactNodeInfo.compactIpAddressPortInfo.ip, compactNodeInfo.compactIpAddressPortInfo.port), this.nodeId);
        }
    }

    private void receive(DhtPingReq dhtPingReq) throws IOException {
        DhtPingReply dhtPingReply = new DhtPingReply();
        dhtPingReply.addr = dhtPingReq.addr;
        dhtPingReply.tId = dhtPingReq.tId;
        dhtPingReply.nodeId = this.nodeId;
        sendDht(dhtPingReply);
    }

    private void receive(DhtPingReply dhtPingReply) {
    }

    private void receive(DhtAnnouncePeerReq dhtAnnouncePeerReq) throws IOException {
        DhtAnnouncePeerReply dhtAnnouncePeerReply = new DhtAnnouncePeerReply();
        dhtAnnouncePeerReply.addr = dhtAnnouncePeerReq.addr;
        dhtAnnouncePeerReply.tId = dhtAnnouncePeerReq.tId;
        dhtAnnouncePeerReply.nodeId = this.nodeId;
        sendDht(dhtAnnouncePeerReply);
        infoHashListener.accept(dhtAnnouncePeerReq.infoHash);
    }

    private void receive(DhtAnnouncePeerReply dhtAnnouncePeerReply) {
    }

    private void addRouteTableNode(DhtMsg dhtMsg) {
        try {
            if (dhtMsg instanceof DhtReq) {
                DhtReq dhtReq = (DhtReq) dhtMsg;
                routeTable.add(new Node(dhtReq.nodeId, dhtReq.addr.ip, dhtReq.addr.port), (newNode, oldNode) -> {
                    oldNode.ip = newNode.ip;
                    oldNode.port = newNode.port;
                    oldNode.lastReqTime = newNode.lastReqTime;
                });
            } else if (dhtMsg instanceof DhtReply) {
                DhtReply dhtReply = (DhtReply) dhtMsg;
                routeTable.add(new Node(dhtReply.nodeId, dhtReply.addr.ip, dhtReply.addr.port), (newNode, oldNode) -> {
                    oldNode.ip = newNode.ip;
                    oldNode.port = newNode.port;
                    oldNode.lastReplyTime = newNode.lastReplyTime;
                    oldNode.replied = true;
                });
            }
        } catch (BucketFullException ex) {
            Logger.getLogger(Clawer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void pingNode(Addr addr, byte[] nodeId) throws IOException {
        DhtPingReq req = new DhtPingReq();
        req.addr = addr;
        req.nodeId = nodeId;
        sendDht(req);
    }

    private void findNodes(byte[] targetNodeId) throws IOException {
        List<Node> nearestNodes = routeTable.nearestNodes(targetNodeId);
        if (nearestNodes.isEmpty()) {
            for (Addr addr : BOOTSTRAP_NODES) {
                findNode(addr, targetNodeId);
            }
        } else {
            for (Node node : nearestNodes) {
                findNode(new Addr(node.ip, node.port), targetNodeId);
            }
        }
    }

    private void findNode(Addr addr, byte[] targetNodeId) throws IOException {
        DhtFindNodeReq req = new DhtFindNodeReq();
        req.addr = addr;
        req.nodeId = this.nodeId;
        req.targetNodeId = targetNodeId;
        sendDht(req);
    }

    private class ReceiveThread extends Thread {

        private boolean running;

        public ReceiveThread() {
            super();
            setDaemon(true);
            running = true;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    receive();
                } catch (DhtException | KrpcException ex) {
                    Logger.getLogger(ReceiveThread.class.getName()).log(Level.FINE, null, ex);
                } catch (Exception ex) {
                    Logger.getLogger(ReceiveThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        public void cancel() {
            running = false;
        }
    }

    public void join() throws InterruptedException {
        receiveThread.join();
    }

    public static void main(String[] args) throws Exception {
        Clawer clawer = new Clawer("0.0.0.0", 6881, infoHash -> System.out.println(Hex.encodeHexString(infoHash)));
        clawer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> clawer.stop()));
        clawer.join();
    }

    @Override
    protected void sendUdp(UdpMsg msg) throws IOException {
        DatagramPacket datagramPacket = new DatagramPacket(msg.data,
                msg.data.length);
        datagramPacket.setSocketAddress(new InetSocketAddress(msg.addr.ip,
                msg.addr.port));
        socket.send(datagramPacket);
    }

    @Override
    protected UdpMsg recvUdp() throws IOException {
        byte[] buffer = new byte[65536];
        DatagramPacket datagramPacket = new DatagramPacket(buffer,
                buffer.length);
        socket.receive(datagramPacket);
        UdpMsg msg = new UdpMsg();
        msg.data = new byte[datagramPacket.getLength()];
        System.arraycopy(datagramPacket.getData(), datagramPacket.getOffset(),
                msg.data, 0, datagramPacket.getLength());
        InetSocketAddress socketAddress = (InetSocketAddress) datagramPacket
                .getSocketAddress();
        msg.addr = new Addr(socketAddress.getHostString(), socketAddress.getPort());
        return msg;
    }

    @Override
    protected void setDhtReqMethod(Addr addr, byte[] tid, String method) {
//        dhtReqMethodCache.put(
//                addr.ip + ":" + addr.port + "/"
//                + Hex.encodeHexString(tid), new MethodAndTime(method,
//                        System.currentTimeMillis()));
        dhtReqMethodCache.put(Hex.encodeHexString(tid), new MethodAndTime(method,
                System.currentTimeMillis()));
    }

    @Override
    protected String removeDhtReqMethod(Addr addr, byte[] tid) {
//        MethodAndTime methodAndTime = dhtReqMethodCache.remove(addr.ip + ":" + addr.port
//                + "/" + Hex.encodeHexString(tid));
        MethodAndTime methodAndTime = dhtReqMethodCache.remove(Hex.encodeHexString(tid));
        if (methodAndTime != null) {
            return methodAndTime.method;
        } else {
            return "unknown";
        }
    }
}
